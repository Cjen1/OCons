open Base
open Lwt.Infix

module DEBUG = struct
  let ( >>= ) p f =
    let handler e =
      Fmt.failwith "Got exn while resolving promise: %a" Fmt.exn e
    in
    Lwt.try_bind (fun () -> p) f handler
end

(* Returns f wrapped if either an exception occurrs in the evaluation of f() or in the promise *)
let catch f = try Lwt_result.catch (f ()) with exn -> Lwt.return_error exn

module Lookup = struct
  open Base

  type ('a, 'b) t = ('a, 'b) Base.Hashtbl.t

  let pp ppf _v = Stdlib.Format.fprintf ppf "Hashtbl"

  let get t key =
    Hashtbl.find t key
    |> Result.of_option ~error:(Invalid_argument "No such key")

  let get_exn t key = Result.ok_exn (get t key)

  let remove t key = Hashtbl.remove t key ; t

  let set t ~key ~data = Hashtbl.set t ~key ~data ; t

  let removep t p =
    Hashtbl.iteri t ~f:(fun ~key ~data -> if p data then remove t key |> ignore) ;
    t

  let create key_module = Base.Hashtbl.create key_module

  let fold t ~f ~init =
    Base.Hashtbl.fold t ~f:(fun ~key:_ ~data acc -> f data acc) ~init

  let find_or_add = Base.Hashtbl.find_or_add
end

module Lwt_queue = struct
  type 'a t =
    { q: 'a Stdlib.Queue.t
    ; cond: unit Lwt_condition.t
    ; switch: Lwt_switch.t option }

  let add t v =
    Stdlib.Queue.add v t.q ;
    Lwt_condition.signal t.cond ()

  let take t =
    let rec loop () =
      match (Stdlib.Queue.take_opt t.q, t.switch) with
      | _, Some switch when not (Lwt_switch.is_on switch) ->
          Lwt.return_error `Closed
      | Some v, _ ->
          Lwt.return_ok v
      | None, _ ->
          Lwt_condition.wait t.cond >>= fun () -> loop ()
    in
    loop ()

  let create ?switch () =
    let cond = Lwt_condition.create () in
    Lwt_switch.add_hook switch (fun () ->
        Lwt_condition.broadcast cond () |> Lwt.return) ;
    {q= Stdlib.Queue.create (); cond; switch}
end

module type Persistable = sig
  type t

  val init : unit -> t

  type op

  val encode_blit : op -> int * (bytes -> offset:int -> unit)

  val decode : bytes -> offset:int -> op

  val apply : t -> op -> t
end

module Persistant (P : Persistable) : sig
  type t =
    { t: P.t
    ; mutable write_promise: unit Lwt.t
    ; fd: Lwt_unix.file_descr
    ; channel: Lwt_io.output_channel }

  type op = P.op

  val sync : t -> t Lwt.t

  val of_file : string -> t Lwt.t

  val change : t -> op -> t
end = struct
  include P

  type t =
    { t: P.t
    ; mutable write_promise: unit Lwt.t
    ; fd: Lwt_unix.file_descr
    ; channel: Lwt_io.output_channel }

  let write t v =
    let p_len, p_blit = P.encode_blit v in
    let buf = Bytes.create (p_len + 4) in
    p_blit buf ~offset:4 ;
    EndianBytes.LittleEndian.set_int32 buf 0 (Int32.of_int_exn p_len) ;
    t.write_promise <-
      ( t.write_promise
      >>= fun () -> Lwt_io.write_from_exactly t.channel buf 0 (Bytes.length buf)
      )

  let sync t =
    let write_promise = t.write_promise in
    write_promise
    >>= fun () ->
    (* All pending writes now complete *)
    Lwt_io.flush t.channel
    >>= fun () ->
    Lwt_unix.fsync t.fd
    >>= fun () ->
    Logs.debug (fun m -> m "Finished syncing") ;
    Lwt.return t

  let read_value channel =
    let rd_buf = Bytes.create 4 in
    Lwt_io.read_into_exactly channel rd_buf 0 4
    >>= fun () ->
    let size =
      EndianBytes.LittleEndian.get_int32 rd_buf 0 |> Int32.to_int_exn
    in
    let payload_buf = Bytes.create size in
    Lwt_io.read_into_exactly channel payload_buf 0 size
    >>= fun () -> payload_buf |> Lwt.return

  let of_file file =
    Logs.debug (fun m -> m "Trying to open file") ;
    Lwt_unix.openfile file Lwt_unix.[O_RDONLY; O_CREAT] 0o640
    >>= fun fd ->
    let input_channel = Lwt_io.of_fd ~mode:Lwt_io.input fd in
    let stream =
      Lwt_stream.from (fun () ->
          Lwt.catch
            (fun () ->
              read_value input_channel
              >>= fun v -> decode v ~offset:0 |> Lwt.return_some)
            (function _ -> Lwt.return_none))
    in
    Logs.debug (fun m -> m "Reading in") ;
    Lwt_stream.fold (fun v t -> P.apply t v) stream (P.init ())
    >>= fun t ->
    Lwt_io.close input_channel
    >>= fun () ->
    Logs.debug (fun m -> m "Creating fd for persistance") ;
    Lwt_unix.openfile file Lwt_unix.[O_WRONLY; O_APPEND] 0o640
    >>= fun fd ->
    let channel = Lwt_io.of_fd ~mode:Lwt_io.output fd in
    Lwt.return {t; write_promise= Lwt.return_unit; fd; channel}

  let change t op =
    write t op ;
    {t with t= P.apply t.t op}
end

module Quorum = struct
  type ('k, 'v) t = {add: 'k * 'v -> unit; mutable fulfilled: bool}

  let make_quorum ~threshold ~equal ~(f : ('a * 'b) list -> unit) : ('a, 'b) t =
    let xs = ref [] in
    let rec add ((k, _) as x) =
      if not (List.Assoc.mem !xs ~equal k) then (
        xs := x :: !xs ;
        if List.length !xs > threshold then (
          t.fulfilled <- true ;
          f !xs ) )
    and t = {add; fulfilled= false} in
    t
end

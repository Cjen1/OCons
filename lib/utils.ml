open Base
open Lwt.Infix

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

module type Persistable = sig
  type t

  val init : unit -> t

  type op

  val op_to_protobuf : op -> Protobuf.Encoder.t -> unit

  val op_from_protobuf : Protobuf.Decoder.t -> op

  val apply : t -> op -> t
end

module Persistant (P : Persistable) : sig
  type t =
    { t: P.t
    ; mutable unsyncd: P.op list
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
    ; mutable unsyncd: P.op list
    ; fd: Lwt_unix.file_descr
    ; channel: Lwt_io.output_channel }

  let sync t =
    match t.unsyncd with
    | [] ->
        Lwt.return t
    | _ ->
        Logs.debug (fun m ->
            m "There are %d ops to sync" (List.length t.unsyncd)) ;
        let vs = t.unsyncd |> List.rev in
        t.unsyncd <- [] ;
        let writev vs =
          Lwt_list.iter_s
            (fun v ->
              Logs.debug (fun m -> m "Syncing op") ;
              let payload = Protobuf.Encoder.encode_exn P.op_to_protobuf v in
              let p_len = Bytes.length payload in
              let buf = Bytes.create (p_len + 4) in
              Bytes.blit ~src:payload ~src_pos:0 ~dst:buf ~dst_pos:4 ~len:p_len ;
              EndianBytes.LittleEndian.set_int32 buf 0 (Int32.of_int_exn p_len) ;
              Lwt_io.write_from_exactly t.channel buf 0 (Bytes.length buf))
            vs
        in
        writev vs
        >>= fun () ->
        Logs.debug (fun m -> m "Ops written to channel") ;
        Lwt_io.flush t.channel
        >>= fun () ->
        Logs.debug (fun m -> m "Channel flushed") ;
        Lwt_unix.fsync t.fd
        >>= fun () ->
        Logs.debug (fun m -> m "Ops fsync'd") ;
        Lwt.return {t with unsyncd= []}

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
              >>= fun v ->
              Protobuf.Decoder.decode_exn P.op_from_protobuf v
              |> Lwt.return_some)
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
    Lwt.return {t; unsyncd= []; fd; channel}

  let change t op = {t with t= P.apply t.t op; unsyncd= op :: t.unsyncd}
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

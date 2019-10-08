let ( let* ) = Lwt.bind

let ( and* ) = Lwt.both

let critical_section mutex ~f =
  let* () = Lwt_mutex.lock mutex in
  let* res = f () in
  let () = Lwt_mutex.unlock mutex in
  Lwt.return @@ res

let write_to_wal (oc, fd) line =
  let* res = Lwt_io.write_line oc line in
  let () = Unix.fsync fd in
  Lwt.return res

module Queue : sig
  type 'a t

  val create : unit -> 'a t

  val add : 'a -> 'a t -> unit

  val take : 'a t -> 'a Lwt.t
end = struct
  type 'a t = {m: Lwt_mutex.t; c: unit Lwt_condition.t; q: 'a Queue.t}

  let create () =
    {m= Lwt_mutex.create (); c= Lwt_condition.create (); q= Queue.create ()}

  let add e t =
    Queue.add e t.q ;
    Lwt_condition.signal t.c ()

  let take t =
    let* () = Lwt_mutex.lock t.m in
    let* () =
      if Queue.is_empty t.q then Lwt_condition.wait ~mutex:t.m t.c
      else Lwt.return_unit
    in
    let e = Lwt.return (Queue.take t.q) in
    Lwt_mutex.unlock t.m ; e
end

module PQueue = struct
  open Core_kernel
  open Types

  type t =
    { mutable high_slot: slot_number
    ; q: slot_number Core_kernel.Heap.t
    ; m: Lwt_mutex.t
    ; c: unit Lwt_condition.t
    ; ongoing: int }

  let create () =
    { m= Lwt_mutex.create ()
    ; c= Lwt_condition.create ()
    ; q= Heap.create ~cmp:Int.compare ()
    ; high_slot= 0
    ; ongoing= 0 }

  let add e t =
    Heap.add t.q e ;
    Lwt_condition.signal t.c ()

  let take t =
    let* () = Lwt_mutex.lock t.m in
    let* () =
      if Heap.is_empty t.q then Lwt_condition.wait ~mutex:t.m t.c
      else Lwt.return_unit
    in
    let e = Lwt.return (Heap.pop_exn t.q) in
    Lwt_mutex.unlock t.m ; e
end

(* TODO move create_socket out of critical path? *)
let connect uri =
  (* TODO change out from TCP? *)
  let sock = Lwt_unix.socket Lwt_unix.PF_INET Lwt_unix.SOCK_STREAM 0 in
  let* () = Lwt_unix.connect sock uri in
  Lwt.return
    ( Lwt_io.of_fd ~mode:Lwt_io.Input sock
    , Lwt_io.of_fd ~mode:Lwt_io.Output sock )

let comm uri msg =
  let* ic, oc = connect uri in
  let* () = Bytes.to_string msg |> Lwt_io.write_line oc in
  let* bytes = Lwt_io.read_line ic in
  Lwt.return bytes

let send uri msg =
  let* _, oc = connect uri in
  Lwt_io.write_line oc msg

let uri_of_string str =
  match Base.String.split ~on:':' str with
  | [ip; port] ->
      let ip = Unix.inet_addr_of_string ip in
      let port = Base.Int.of_string port in
      Unix.ADDR_INET (ip, port)
  | _ ->
      assert false

let uri_of_string_and_port ip port =
  let ip = Unix.inet_addr_of_string ip in
  Unix.ADDR_INET (ip, port)

open Base

let critical_section mutex ~f =
  try%lwt
    let%lwt () = Logs_lwt.debug (fun m -> m "Entering cs") in
    let%lwt () = Lwt_mutex.lock mutex in
    let%lwt res = f () in
    let () = Lwt_mutex.unlock mutex in
    Lwt.return @@ res
  with e ->
    let () = Lwt_mutex.unlock mutex in
    let%lwt () = Logs_lwt.debug (fun m -> m "Entering cs") in
    raise e

let write_to_wal fd line =
  let written = Unix.write_substring fd line 0 (String.length line) in
  assert (written = String.length line) ;
  Unix.fsync fd

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
    Queue.enqueue t.q e ;
    Lwt_condition.signal t.c ()

  let take t =
    let%lwt () = Lwt_mutex.lock t.m in
    let%lwt () =
      if Queue.is_empty t.q then Lwt_condition.wait ~mutex:t.m t.c
      else Lwt.return_unit
    in
    let e = Lwt.return (Queue.dequeue_exn t.q) in
    Lwt_mutex.unlock t.m ; e
end

module PQueue = struct
  open Core_kernel
  open Types

  type t = {mutable high_slot: slot_number; q: slot_number Core_kernel.Heap.t}

  let create () = {q= Heap.create ~cmp:Int.compare (); high_slot= 0}

  let add e t = Heap.add t.q e

  let take t =
    match Heap.is_empty t.q with
    | false ->
        Heap.pop_exn t.q
    | true ->
        let slot = t.high_slot in
        t.high_slot <- t.high_slot + 1 ;
        slot
end

(* TODO move create_socket out of critical path? *)
let connect uri =
  (* TODO change out from TCP? *)
  let sock = Lwt_unix.socket Lwt_unix.PF_INET Lwt_unix.SOCK_STREAM 0 in
  let%lwt () = Lwt_unix.connect sock uri in
  Lwt.return
    ( Lwt_io.of_fd ~mode:Lwt_io.Input sock
    , Lwt_io.of_fd ~mode:Lwt_io.Output sock )

let unix_error_handler (e, f, p) tag =
  let%lwt () =
    Logs_lwt.debug (fun m ->
        m "%s: failed to communicate with %s calling %s with parameter %s" tag
          (Unix.error_message e) f p)
  in
  fst @@ Lwt.task ()

let comm uri msg =
  try%lwt
    Logs.debug (fun m -> m "comm: connect") ;
    let%lwt ic, oc = connect uri in
    Logs.debug (fun m -> m "comm: send") ;
    let%lwt () = Bytes.to_string msg |> Lwt_io.write_value oc in
    Logs.debug (fun m -> m "comm: waiting resp") ;
    let%lwt bytes = Lwt_io.read_value ic in
    Logs.debug (fun m -> m "comm: got resp") ;
    Lwt.return bytes
  with Unix.Unix_error (e, f, p) -> unix_error_handler (e, f, p) "comm"

let send uri msg =
  try%lwt
    let%lwt _, oc = connect uri in
    Lwt_io.write_value oc msg
  with Unix.Unix_error (e, f, p) -> unix_error_handler (e, f, p) "send"

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

let string_of_sockaddr s =
  match s with
  | Lwt_unix.ADDR_UNIX s ->
      s
  | Lwt_unix.ADDR_INET (inet, p) ->
      Unix.string_of_inet_addr inet ^ ":" ^ Int.to_string p

module Semaphore = struct
  type t = {m_count: Lwt_mutex.t; m_queue: Lwt_mutex.t; mutable n: int}

  let create n =
    Lwt_main.run
    @@
    let m_count = Lwt_mutex.create () in
    let m_queue = Lwt_mutex.create () in
    let%lwt () = Lwt_mutex.lock m_queue in
    Lwt.return {n; m_count; m_queue}

  let wait t =
    let%lwt () = Lwt_mutex.lock t.m_count in
    t.n <- t.n - 1 ;
    let%lwt () =
      if t.n < 0 then (Lwt_mutex.unlock t.m_count ; Lwt_mutex.lock t.m_queue)
      else Lwt.return_unit
    in
    Lwt.return @@ Lwt_mutex.unlock t.m_count

  let signal t =
    let%lwt () = Lwt_mutex.lock t.m_count in
    t.n <- t.n + 1 ;
    if t.n <= 0 then Lwt_mutex.unlock t.m_queue |> Lwt.return
    else Lwt_mutex.unlock t.m_count |> Lwt.return
end

module AIMDTimeout = struct
  type state = SlowStart | Normal

  type t =
    { mutable state: state
    ; mutable timeout: float
    ; slow_start: float
    ; ai: float
    ; md: float }

  let increase = function
    | {state= SlowStart; _} as t ->
        t.timeout <- t.timeout *. t.slow_start
    | {state= Normal; _} as t ->
        t.timeout <- t.timeout +. t.ai

  let decrease t =
    (match t.state with SlowStart -> t.state <- Normal | Normal -> ()) ;
    t.timeout <- t.timeout /. t.md

  let wait t = Lwt_unix.sleep (Random.float_range 0. t.timeout)

  let create ~timeout ~slow_start ~ai ~md =
    {state= SlowStart; timeout; slow_start; ai; md}
end

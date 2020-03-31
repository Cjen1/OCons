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

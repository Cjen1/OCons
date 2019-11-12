val start :
     Unix.inet_addr
  -> int
  -> 'a
  -> ((unit -> string Lwt.t) * (string -> unit Lwt.t) -> 'a -> unit Lwt.t)
  -> unit Lwt.t

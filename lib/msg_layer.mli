type t

val attach_watch :
  t -> filter:string -> callback:('a -> unit Lwt.t) -> typ:'a Messaging.msg_typ -> unit

val send_msg :
     t
  -> ?timeout:float
  -> ?finished:unit Lwt.t
  -> filter:string
  -> string
  -> unit Lwt.t
(** Sends a message onto the bus, finished allows for retry semantics if a condition is not met (i.e. for paxos a quorum hasn't responded) *)

val node_alive : t -> node:string -> (bool, [> `NodeNotFound]) Base.Result.t

val node_dead_watch :
     t
  -> node:string
  -> callback:(unit -> unit Lwt.t)
  -> (unit, [> `NodeNotFound]) Base.Result.t

val create :
     node_list:string list
  -> local:string
  -> alive_timeout:float
  -> t * unit Lwt.t

val client_socket :
     callback:(string -> (string -> unit Lwt.t) -> unit Lwt.t)
  -> address_req:string
  -> address_rep:string
  -> t
  -> 'a Lwt.t

val retry :
     ?tag:string
  -> finished:'a Lwt.t
  -> timeout:float
  -> (unit -> unit)
  -> 'a Lwt.t

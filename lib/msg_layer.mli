open Messaging

type t

val attach_watch :
  t -> msg_filter:'a msg_filter -> callback:('a -> unit Lwt.t) -> unit

val send_msg : t -> msg_filter:'a msg_filter -> 'a -> unit

val node_alive : t -> node:string -> (bool, [> `NodeNotFound]) Base.Result.t

val node_dead_watch :
     t
  -> node:string
  -> callback:(unit -> unit Lwt.t)
  -> (unit, [> `NodeNotFound]) Base.Result.t

val create :
     node_list:(string * string) list
  -> id:string
  -> alive_timeout:float
  -> (t * unit Lwt.t) Lwt.t

val client_socket :
     t
  -> callback:
       (   Messaging.client_request
        -> (Messaging.client_response -> unit Lwt.t)
        -> unit
        -> unit Lwt.t)
  -> port:string
  -> 'a Lwt.t

val retry :
     ?tag:string
  -> finished:'a Lwt.t
  -> timeout:float
  -> (unit -> unit)
  -> 'a Lwt.t

open Messaging

module Msg_Queue : sig
  type t
end 

type t =
  { endpoints: (string, string) Base.Hashtbl.t
  ; last_rec: (string, float) Base.Hashtbl.t
  ; id: string
  ; alive_timeout: float
  ; context: Zmq.Context.t
  ; incomming: [`Router] Zmq_lwt.Socket.t
  ; outgoing: [`Router] Zmq_lwt.Socket.t
  ; msg_queues: (string, Msg_Queue.t) Base.Hashtbl.t
  ; subs: (string, (string -> string -> unit Lwt.t) list) Base.Hashtbl.t
  ; mutable node_dead_watch: unit Lwt.t option }


val attach_watch_src :
  t -> msg_filter:'a msg_filter -> callback:(string -> 'a -> unit Lwt.t) -> unit

val attach_watch :
  t -> msg_filter:'a msg_filter -> callback:('a -> unit Lwt.t) -> unit

val send : t -> msg_filter:'a msg_filter -> dest:string -> 'a -> unit
val send_untyped : t -> filter:string -> dest:string -> string -> unit

val send_all : t -> msg_filter:'a msg_filter -> 'a -> unit
val send_all_untyped : t -> filter:string -> string -> unit

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

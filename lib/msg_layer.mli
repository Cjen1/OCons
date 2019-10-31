
type t
val attach_watch: 
  t ->
  filter:string ->
  callback:(string -> unit Lwt.t) ->
  unit 

val send_msg:
  t ->
  ?finished:unit Lwt.t ->
  ?timeout:float ->
  filter:string ->
  string -> unit
(** Sends a message onto the bus, finished allows for retry semantics if a condition is not met (i.e. for paxos a quorum hasn't responded) *)

val node_alive:
  t -> 
  node:string -> 
  bool

val create:
  node_list:string list ->
  local:string ->
  t
  

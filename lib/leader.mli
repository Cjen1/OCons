type t

val create_independent : string -> string list -> nodeid:float -> address_req:string -> address_rep:string -> float -> t * unit Lwt.t

val create : Msg_layer.t -> string -> string list -> nodeid:float -> address_req:string -> address_rep:string -> t * unit Lwt.t

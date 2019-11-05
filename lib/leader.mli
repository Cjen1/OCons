type t

val create_independent : string -> string list -> string -> float -> t * unit Lwt.t

val create : Msg_layer.t -> string -> 'a list -> string -> t * unit Lwt.t

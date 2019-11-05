type t

val create_independent : string -> string list -> cpub_address:string -> csub_address:string -> float -> t * unit Lwt.t

val create : Msg_layer.t -> string -> 'a list -> cpub_address:string -> csub_address:string -> t * unit Lwt.t

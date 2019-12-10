type t

val create_independent :
     id:string
  -> endpoints:(string * string) list
  -> client_port:string
  -> alive_timeout:float
  -> (t * unit Lwt.t) Lwt.t

val create :
     msg_layer:Msg_layer.t
  -> id:string
  -> endpoints:(string * string) list
  -> client_port:string
  -> t * unit Lwt.t

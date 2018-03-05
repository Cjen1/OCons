(* Client.mli *)

type t
val new_client : string -> int -> Uri.t list -> t Lwt.t
val send_request_message : t -> Types.operation -> unit Lwt.t

(* Currently for tracing...
   Should be separated out into its own module / sub-module *)
val send_timed_request  : t -> int -> unit Lwt.t

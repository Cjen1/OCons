(* Client.mli *)

type t;;
val new_client : string -> int -> Uri.t list -> t Lwt.t;;
val send_request_message : t -> Types.operation -> unit Lwt.t;;


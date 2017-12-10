(* Client.mli *)

type t;;
val new_client : Uri.t list -> t;;
val send_request_message : t -> Types.operation -> unit Lwt.t;;


(* Client.mli *)

type t;;
val new_client : Uri.t list -> t;;
val send_request_message : t -> Types.operation -> (Types.command_id * Types.result) Lwt.t list;;


open Types;;
open Message;;

open Core;;
open Core.Unix;;

module type CLIENT = sig
  type client_info;;
  val new_client : unit -> client_info;;
  val add_replica_uri : Uri.t -> client_info -> client_info;;  
  val getid : client_info -> client_id;;
  val getnextcommand : client_info -> command_id;;
  val geturis : client_info -> Uri.t list;;
  val send_request_message : client_info -> operation -> (command_id * Types.result) Lwt.t list;;
end;;

module Client : CLIENT;;







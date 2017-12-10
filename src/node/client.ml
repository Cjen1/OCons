(* client.ml *)

open Core
open Core.Unix
open Lwt.Infix
open Types
open Message

(* Type of clients *)
type t = {
  id : client_id;
  mutable next_command_id : command_id;
  mutable replica_uri_list : Uri.t list
};;
  
(* Create a new record of client information *)
let new_client uris = {
  id = Core.Uuid.create ();
  next_command_id = 1;
  replica_uri_list = uris
};;

(* Send a clientrequestmessage RPC of a given operation by a given client *)
let send_request_message client operation =
  (* Send the message to some underlying RPC subsystem. *)
  let client_id = client.id in
  let command_id = client.next_command_id in

  (* Increment the command number *)
  client.next_command_id <- command_id + 1;
  
  (* Map the list of clients to a list of response messages.
     For each uri in list, send the request message to that URI and
     bind the response, returning a (command_id, Types.result).
  *)
  List.map client.replica_uri_list (fun uri -> 
      Message.send_request (ClientRequestMessage(client_id,command_id,operation)) uri >>=
      function
      | Message.ClientRequestResponse (cid, result) -> Lwt.return (cid,result)
      | _ -> raise Message.Invalid_response);;

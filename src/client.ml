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
let initialize client_uri replica_uris = {
  id = (Core.Uuid.create (), client_uri);
  next_command_id = 1;
  replica_uri_list = replica_uris
};;


let result_callback (response : Types.command_id * Types.result) =
  let (cid, result) = response in
  Lwt_io.printl ("Received response for " ^ (string_of_int cid) ^ " as " ^ (Types.string_of_result result)) |> Lwt.ignore_result;;

let new_client host port replica_uris =
  Message.start_new_server ~response_callback:result_callback  host port >>= fun uri ->
  Lwt.return (initialize uri replica_uris);;

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
  Lwt_list.iter_p (fun uri -> 
    let message = (ClientRequestMessage(client_id,command_id,operation)) in
      Message.send_request message uri) client.replica_uri_list;;


(* replica.ml *)

(* THIS IS MOSTLY A STUB IMPLEMENTATION FOR NOW - DOES NOT CONTAIN
   ANY ACTUAL OPERATIONS PERFORMED BY A REPLICA IN THE PROTOCOL *)
open Types
open Lwt.Infix
       
module Replica = struct
  (* Type of replicas *)
  type t;;

  (* Simple function that maps from requests to results *)
  (* Actual replica implementations will have to propose these
     for sequence slots etc.

     But for now this is just to simulate message passing capability
  *)
  let test (cmd : command) : (command_id * result) =
    let (client_id, command_id, operation) = cmd in
    let result = match operation with
      | _ -> Failure
    in (command_id, result);;

  (* Leave these as placeholders for now *)
  let secret_key = `Ephemeral;;
  
  (* Starts a server that will run the service *)
  (* This is mostly Capnproto boilerplate *)
  let start_server (host : string) (port : int) : Uri.t Lwt.t =
    let listen_address = `TCP (host, port) in
    let config = Capnp_rpc_unix.Vat_config.create ~secret_key listen_address in
    let service_id = Capnp_rpc_unix.Vat_config.derived_id config "main" in
    let restore = Capnp_rpc_lwt.Restorer.single service_id (Message.local test) in
    Capnp_rpc_unix.serve config ~restore >|= fun vat ->
    Capnp_rpc_unix.Vat.sturdy_uri vat service_id;;

  let new_replica host port = 
    start_server host port;;
end;;

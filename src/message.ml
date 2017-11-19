(* message.ml *)
open Types;;
open Capnp_rpc_lwt;;
open Lwt.Infix;;

(* Expose the API service for the RPC system *)
module Api = Message_api.MakeRPC(Capnp_rpc_lwt);;

let local (f : command -> (command_id * result)) =
  let module Message = Api.Service.Message in
  Message.local @@ object
    inherit Message.service

    method client_request_impl params release_param_caps =
      let open Message.ClientRequest in
      let module Params = Message.ClientRequest.Params in
      (* Retrieve the fields from the command struct passed in request *)
      let cmd_reader = Params.command_get params in
        let client_id = Api.Reader.Message.Command.client_id_get cmd_reader in
        let command_id = Api.Reader.Message.Command.command_id_get cmd_reader in
        let operation = Api.Reader.Message.Command.operation_get cmd_reader in

        (* Get back response for request *)
        (* Note here there is a temporay Nop passed *)
        let (command_id', result) = f (client_id, command_id, Types.Nop) in
      
        (* Releases capabilities, doesn't matter for us *)
        release_param_caps ();
        
        (* Construct a response struct for the reply *)
        let open Api.Builder.Message in
        let response_rpc = Response.init_root () in
          Response.command_id_set_exn response_rpc command_id';
          Response.result_set response_rpc "FOOBAR";
          
          (* Reply with the response *)
          let response, results = Service.Response.create Results.init_pointer in
          (Results.response_set_builder results response_rpc |> ignore);
          Service.return response;
  end;;

let client_request_rpc t cmd =
  let open Api.Client.Message.ClientRequest in
  let request, params = Capability.Request.create Params.init_pointer in
  let open Api.Builder.Message in
    (* Create an empty command type as recognised by Capnp *)
    let cmd_rpc = (Command.init_root ()) in
    
    (* Construct a command struct for Capnp from the cmd argument given *)
    let (client_id, command_id, operation) = cmd in
      Command.client_id_set_exn cmd_rpc client_id;
      Command.command_id_set_exn cmd_rpc command_id;
      (* Note here the operation type in schema file is a test string,
         so we don't pass the actual operation type and just pass a
         string for testing *)
      Command.operation_set cmd_rpc "FOOBAR";

      (* Constructs the command struct and associates with params *)
      (Params.command_set_reader params (Command.to_reader cmd_rpc) |> ignore);

      (* Send the message and pull out the result *)
      Capability.call_for_value_exn t method_id request >|= Results.response_get;;

(*---------------------------------------------------------------------------*)

(* Types of message that can be passed between nodes:
      - This represents the application-level representation of a message.
      - These can be passed to the RPC api to be prepared for transport etc. *)
type message = ClientRequestMessage of command;;
          (* | ... further messages will be added *) 

(* Takes a Capnp URI for a service and returns the lwt capability of that
   service *)
let service_from_uri uri =
  let client_vat = Capnp_rpc_unix.client_only_vat () in
  let sr = Capnp_rpc_unix.Vat.import_exn client_vat uri in
  Sturdy_ref.connect_exn sr >>= fun proxy_to_service ->
  Lwt.return proxy_to_service;;

(* Accepts as input a message and prepares it for RPC transport,
   given the URI of the service to which it will be sent*)
let send_request message uri =
  (* Get the service for the given URI *)
  service_from_uri uri >>= fun service ->
  match message with
  | ClientRequestMessage(cmd) ->
    (* Perform the RPC with the given command *)
    client_request_rpc service cmd >>= fun response ->
      (* Pull the (command_id, result) from the Capnp struct response *)
      let command_id : command_id = Api.Reader.Message.Response.command_id_get response in
      let result = Api.Reader.Message.Response.result_get response in
      (* Return temporary result due to type mismatch here *)
      Lwt.return (command_id, Failure);;

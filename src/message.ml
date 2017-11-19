(* message.ml *)

open Types;;
open Capnp_rpc_lwt;;
open Lwt.Infix;;

exception Undefined_oper;;
exception Undefined_result;;

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
        let open Api.Reader.Message in
        
        (* Retrieve the client id and command id fields from the struct *)
        let client_id = Command.client_id_get cmd_reader in
        let command_id = Command.command_id_get cmd_reader in
        
        (* Operation is more difficult as it is a nested struct *)
        let op_reader = Command.operation_get cmd_reader in
        (* Operations are a union type in Capnp so match over the variant *)
        let operation = (match Command.Operation.get op_reader with
          | Command.Operation.Nop -> Types.Nop
          | Command.Operation.Create c_struct ->
              let k = Command.Operation.Create.key_get c_struct in
              let v = Command.Operation.Create.value_get c_struct in
              Types.Create(k,v)
          | Command.Operation.Read r_struct -> 
              let k = Command.Operation.Read.key_get r_struct in
              Types.Read(k)
          | Command.Operation.Update u_struct ->
              let k = Command.Operation.Update.key_get u_struct in
              let v = Command.Operation.Update.value_get u_struct in
              Types.Update(k,v)
          | Command.Operation.Remove r_struct ->
              let k = Command.Operation.Remove.key_get r_struct in
              Types.Remove(k)
          | Command.Operation.Undefined(_) -> raise Undefined_oper) in

        (* Get back response for request *)
        (* Note here there is a temporay Nop passed *)
        let (command_id', result) = f (client_id, command_id, operation) in
      
        (* Releases capabilities, doesn't matter for us *)
        release_param_caps ();
        
        (* Construct a response struct for the reply *)
        let open Api.Builder.Message in
        let response_rpc = Response.init_root () in
          Response.command_id_set_exn response_rpc command_id';
            
          let result_rpc = Response.Result.init_root () in
          
          (match result with
          | Success -> Response.Result.success_set result_rpc
          | Failure -> Response.Result.failure_set result_rpc
          | ReadSuccess v -> Response.Result.read_set result_rpc v);
          
          (* Need to somehow add the result to the response struct *)
          (* ... *)
          (* ... *)
          (Response.result_set_builder response_rpc result_rpc |> ignore);

          (* Reply with the response *)
          let response, results = Service.Response.create Results.init_pointer in
          (Results.response_set_builder results response_rpc |> ignore);
          Service.return response;
  end;;

(*---------------------------------------------------------------------------*)

let client_request_rpc t (cmd : Types.command) =
  let open Api.Client.Message.ClientRequest in
  let request, params = Capability.Request.create Params.init_pointer in
  let open Api.Builder.Message in
    (* Create an empty command type as recognised by Capnp *)
    let cmd_rpc = (Command.init_root ()) in
    
    (* Construct a command struct for Capnp from the cmd argument given *)
    let (client_id, command_id, operation) = cmd in
      Command.client_id_set_exn cmd_rpc client_id;
      Command.command_id_set_exn cmd_rpc command_id;
      
      (* Construct an operation struct here *)
      let oper_rpc = (Command.Operation.init_root ()) in
      
      (* Populate the operation struct with the correct values *)
      (match operation with
      | Nop         -> 
        Command.Operation.nop_set oper_rpc
      | Create(k,v) ->
        let create = (Command.Operation.create_init oper_rpc) in
        Command.Operation.Create.key_set_exn create k;
        Command.Operation.Create.value_set create v;
      | Read  (k)   -> 
        let read = Command.Operation.read_init oper_rpc in
        Command.Operation.Read.key_set_exn read k;
      | Update(k,v) ->
        let update = Command.Operation.update_init oper_rpc in
        Command.Operation.Update.key_set_exn update k;
        Command.Operation.Update.value_set update v;
      | Remove(k)   -> 
        let remove = Command.Operation.remove_init oper_rpc in
        Command.Operation.Remove.key_set_exn remove k);

      (Command.operation_set_builder cmd_rpc oper_rpc |> ignore);

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
(* This should probably be optimised - maybe store a reference to the
   instantiated service in the client once a connection has been
   established.

   This may even be necessary in order to preserve the ordering semantics
   we want with RPC delivery *)
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
      
      (* Convert the value stored in the Result struct into a Types.Result *)
      let open Api.Reader.Message in
      let result_reader = Response.result_get response in 
      let result = match Response.Result.get result_reader with
      | Response.Result.Failure -> Types.Failure
      | Response.Result.Success -> Types.Success
      | Response.Result.Read x -> Types.ReadSuccess x
      | Response.Result.Undefined _ -> raise Undefined_result in

      (* Return the response to the calling client *)
      Lwt.return (command_id, result);;

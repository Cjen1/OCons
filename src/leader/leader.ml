(* leader.ml *)

open Lwt.Infix;;

(* THIS IS A STUB IMPLEMENTATION FOR LEADERS FOR NOW, WITH ONLY THE ABILITY
   TO RECEIVE PROPOSALS AND SEND DECISIONS.

   THE FULL IMPLEMENTATION COMES WITH THE IMPLEMENTATION OF THE SYNOD 
   PROTOCOL
*)
module Leader = struct
  (* Types of leaders *)
  type t = {
    id : Types.leader_id;
    mutable replica_uris : Uri.t list
  };;

  (* Getter and setter functions for fields of leaders *)

  let get_id leader = match leader with
    | { id; replica_uris } -> id;;

  let get_uris leader = match leader with
    | { id; replica_uris } -> replica_uris;;

  let set_uris leader new_uris = 
    leader.replica_uris <- new_uris;;

  (* Create a new leader *)
  let initialize () = {
    id = Core.Uuid.create ();
    replica_uris = []
  };;

(*---------------------------------------------------------------------------*)
  
  (* This is utterly pointless code! *)
  (* Message.ml should be refactored so we don't have to implement
     this nonsense boilerplate *)
  let test (leader_ref : t ref) (cmd : Types.command) =
    (0,Types.Success);;

  (* Test to signify we have received a proposal message at leader *)
  let test2 (leader_ref : t ref) (p : Types.proposal) =
    Lwt.ignore_result (Lwt_io.printl ("Received a proposal " ^ Types.string_of_proposal p));
    
    (* Broadcast the decision to all participating replicas *)
    Core.List.iter (get_uris !leader_ref) ~f:(fun uri ->
        Lwt.ignore_result (
          (Message.send_request (Message.DecisionMessage p) uri) >>= 
          function Message.DecisionMessageResponse -> Lwt.return_unit
                 | _ -> raise Message.Invalid_response
        )
      );;

(*---------------------------------------------------------------------------*)
  
  (* Print line describing URI to terminal *)
  let print_uri uri =
    Lwt_io.printl ("Spinning up a leader with URI: " ^ (Uri.to_string uri));;  

  let start_server (leader_ref : t ref) (host : string) (port : int) =
    let listen_address = `TCP (host, port) in
    let config = Capnp_rpc_unix.Vat_config.create ~serve_tls:false ~secret_key:`Ephemeral listen_address in
    let service_id = Capnp_rpc_unix.Vat_config.derived_id config "main" in
    let restore = Capnp_rpc_lwt.Restorer.single service_id (Message.local None (Some (test2 leader_ref))) in
    Capnp_rpc_unix.serve config ~restore >|= fun vat ->
    Capnp_rpc_unix.Vat.sturdy_uri vat service_id;;

  let new_leader host port = 
    let leader_ref = ref ( initialize () ) in
    (leader_ref, Lwt.join [
    (start_server leader_ref host port >>= 
      fun uri -> print_uri uri >>=
      fun () -> fst @@ Lwt.wait ())
    ])
end;;

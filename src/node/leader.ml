(* leader.ml *)

open Lwt.Infix;;

module Leader = struct
  (* Types of leaders *)
  type t = {
    id : Types.leader_id;
    mutable replica_uris : Uri.t list
  };;
  
  (* Create a new leader *)
  let initialize () = {
    id = Core.Uuid.create ();
    replica_uris = []
  };;

(*---------------------------------------------------------------------------*)

  (* THIS IS A STUB IMPLEMENTATION FOR LEADERS FOR NOW, WITH ONLY THE ABILITY
   TO RECEIVE PROPOSALS AND SEND DECISIONS.

   THE FULL IMPLEMENTATION COMES WITH THE IMPLEMENTATION OF THE SYNOD 
   PROTOCOL *)

  (* Test to signify we have received a proposal message at leader *)
  let receive_proposal (leader : t) (p : Types.proposal) =
    Lwt.ignore_result (
      Lwt_io.printl ("Received a proposal " ^ Types.string_of_proposal p));
    
    (* Broadcast the decision to all participating replicas *)
    Core.List.iter (leader.replica_uris) ~f:(fun uri ->
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
  
  (* Initialize a server for a given leader on a given host and port *)
  let start_server (leader : t) (host : string) (port : int) =
    Message.start_new_server None (Some (receive_proposal leader)) host port;;
  
  let new_leader host port = 
    (* Initialize a new leader *)    
    let leader = initialize () in
    
    (* Return a pair consisting of a reference to the leader and 
       a Lwt thread that is the main execution context of the 
       leader *)
    (leader, Lwt.join [
    (start_server leader host port >>= 
      fun uri -> print_uri uri >>=
      fun () -> fst @@ Lwt.wait ())
    ])
end;;




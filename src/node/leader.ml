(* leader.ml *)

open Lwt.Infix;;
open Core;;

(* Types of leaders *)
type t = {
  id : Types.leader_id;
  mutable replica_uris : Uri.t list
};;
  
(* Create a new leader *)
let initialize uris = {
  id = Core.Uuid.create ();
  replica_uris = uris
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
  List.iter (leader.replica_uris) ~f:(fun uri ->
    Message.send_request (Message.DecisionMessage p) uri |>
    Lwt.ignore_result);;
(*---------------------------------------------------------------------------*)

(* Print line describing URI to terminal *)
let print_uri uri =
  Lwt_io.printl ("Spinning up a leader with URI: " ^ (Uri.to_string uri));;  

(* Initialize a server for a given leader on a given host and port *)
let start_server (leader : t) (host : string) (port : int) =
  Message.start_new_server None (Some (receive_proposal leader)) host port;;

let new_leader host port uris = 
  (* Initialize a new leader *)
  let leader = initialize uris in
  
  (* Return a Lwt thread that is the main execution context
     of the leader *)
  start_server leader host port >>= fun uri ->
  print_uri uri >>= fun () ->
  fst @@ Lwt.wait ();;

(* acceptor.ml *)

open Lwt.Infix
open Log.Logger

(* Types of acceptors *)
type t = {
  id : Types.unique_id;
  mutable ballot_num : Ballot.t;
  mutable accepted : Ballot.pvalue list
}

(* Useful helper functions *)
let (<) = Ballot.less_than
let (=) = Ballot.equal

(* Initialize a new acceptor *)
let initialize () = {
  id = Core.Uuid.create ();
  ballot_num = Ballot.Bottom;
  accepted = []
}

(* This callback occurs when an acceptor receives a phase1 message.

   The message will contain a ballot number that a leader is attempting
   to obtain a majority quorum over. The acceptor will adopt the ballot
   number if it is greater than the one it is currently storing.

   In either case, the acceptor replies with its current state (this
   may be the new ballot number it has adopted) *)
let phase1_callback (a : t) (b : Ballot.t) =
  Lwt.ignore_result (write_with_timestamp INFO ("Receive phase1 message with ballot " ^ (Ballot.to_string b)));
  if a.ballot_num < b then 
    Lwt.ignore_result (write_with_timestamp INFO ("Adopt ballot number " ^ (Ballot.to_string b)));
    a.ballot_num <- b;
  Lwt.ignore_result (write_with_timestamp INFO ("Reply to phase1 message with:" ^ (Ballot.to_string b) ^ " and "));
  Core.List.iter a.accepted ~f:(fun pval -> Lwt.ignore_result (write_to_log INFO ("\t\t\t\t" ^ Ballot.pvalue_to_string pval)));
  (a.id, a.ballot_num, a.accepted)

(* This callback occurs when an acceptor receives a phase2 message
   ... *)
let phase2_callback (a : t) (p : Ballot.pvalue) =
  let (b,_,_) = p in
  if b = a.ballot_num then
    a.accepted <- p :: a.accepted;
  (a.id, a.ballot_num)

(* Initialize a server for a given acceptor  on a given host and port *) 
let start_server (acceptor : t) (host : string) (port : int) =
  (* Todo: add callback mechanism... *)
  Message.start_new_server ~phase1_callback:(phase1_callback acceptor)  host port

(* Creating a new acceptor consists of initializing a record for it and
   starting a server for accepting incoming messages *)
let new_acceptor host port = 
  let acceptor = initialize () in
  start_server acceptor host port >>= fun uri ->
  write_to_log INFO ("Initializing new acceptor: \n\tURI " ^ (Uri.to_string uri)) >>= fun () ->
  Lwt.wait () |> fst

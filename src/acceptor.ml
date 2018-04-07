(* acceptor.ml *)

open Lwt.Infix
open Log.Logger

(* Types of acceptors *)
type t = {
  id : Types.unique_id;
  mutable ballot_num : Ballot.t;
  mutable accepted : Pval.t list
}

(* Useful helper functions *)
let (<) = Ballot.less_than
let (=) = Ballot.equal

(* Initialize a new acceptor *)
let initialize () = {
  id = Core.Uuid.create ();
  ballot_num = Ballot.bottom ();
  accepted = []
}

let callback1_mutex = Core.Mutex.create ()
let callback2_mutex = Core.Mutex.create ()
    
(* This callback occurs when an acceptor receives a phase1 message.

   The message will contain a ballot number that a leader is attempting
   to obtain a majority quorum over. The acceptor will adopt the ballot
   number if it is greater than the one it is currently storing.

   In either case, the acceptor replies with its current state (this
   may be the new ballot number it has adopted) *)
let phase1_callback (a : t) (b : Ballot.t) =
  Core.Mutex.critical_section callback2_mutex ~f:(fun () ->
  Lwt.ignore_result (write_to_log INFO "\n");
  Lwt.ignore_result (write_with_timestamp INFO ("Receive phase1 message with ballot " ^ (Ballot.to_string b)));

  if a.ballot_num < b then 
    (Lwt.ignore_result (write_with_timestamp INFO ("Adopt ballot number " ^ (Ballot.to_string b)));
     a.ballot_num <- b);
  
  Lwt.ignore_result (write_with_timestamp INFO ("Reply to phase1 message with: " ^ (Ballot.to_string a.ballot_num) ^ ", pvalues: "));
  Core.List.iter a.accepted ~f:(fun pval -> Lwt.ignore_result (write_to_log INFO ("\t\t\t\t" ^ Pval.to_string pval)));
  (a.id, a.ballot_num, a.accepted))

(* This callback occurs when an acceptor receives a phase2 message
   ... *)
let phase2_callback (a : t) (p : Pval.t) =
  Core.Mutex.critical_section callback2_mutex ~f:(fun () ->
  Lwt.ignore_result (write_to_log INFO "\n");  
  Lwt.ignore_result ( write_with_timestamp INFO ("Receive Phase2 callback with pvalue " ^ (Pval.to_string p)) );
  let (b,_,_) = p in
  if b = a.ballot_num then
    (Lwt.ignore_result ( write_with_timestamp INFO ("Accept pvalue " ^ (Pval.to_string p)) );
     if not (Core.List.mem a.accepted p ~equal:(Pval.equal)) then
        a.accepted <- p :: a.accepted);
  Lwt.ignore_result (write_with_timestamp INFO ("Reply to phase2 message with: (" ^ (Types.string_of_id a.id) ^ "," ^ (Ballot.to_string a.ballot_num) ^ ")"));
  (a.id, a.ballot_num))

(* Initialize a server for a given acceptor  on a given host and port *) 
let start_server (acceptor : t) (host : string) (port : int) =
  Message.start_new_server host port ~phase1_callback:(phase1_callback acceptor)
                                     ~phase2_callback:(phase2_callback acceptor) 


(* Creating a new acceptor consists of initializing a record for it and
   starting a server for accepting incoming messages *)
let new_acceptor host port = 
  let acceptor = initialize () in
  start_server acceptor host port >>= fun uri ->
  write_to_log INFO ("Initializing new acceptor: \n\tURI " ^ (Uri.to_string uri)) >>= fun () ->
  Lwt.wait () |> fst

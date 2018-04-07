(* leader.ml *)

open Types
open Lwt.Infix
open Core
open Log.Logger

(* Types of leaders *)
type t = {
  id : leader_id;
  mutable replica_uris : Uri.t list;
  mutable acceptor_uris : Uri.t list;
  mutable ballot_num : Ballot.t;
  mutable active : bool;
  mutable proposals : proposal list
}

(* Types of responses that can be returned by scout and commander sub-processes *)
type process_response = Adopted of Ballot.t * Pval.t list
                      | Preempted of Ballot.t

(* Create a new leader *)
let initialize replica_uris acceptor_uris =
  let id = create_id ()
  in {
    id;
    replica_uris;
    acceptor_uris;
    ballot_num = Ballot.init id;
    active = false;
    proposals = [] 
  }

(* Given a list of pvalues, return the pvalue with the greatest ballot number.
   Achieve this by first sorting the ballots in the list and then selecting the
   tail of the list *)
let max_ballot (pvals : Pval.t list) : Pval.t =
  let open Core.List in
  let cmp = fun (b,_,_) -> fun (b',_,_) -> - (Ballot.compare b b') in
    hd_exn ( stable_sort ~cmp:cmp pvals )

let pmax (pvals : Pval.t list) : proposal list =
  let open Core.List in
    (* Sort the list by slot number *)
    let sorted_pvals = stable_sort ~cmp:(fun (_,s,_) -> fun (_,s',_) -> Int.compare s s') pvals in
      (* Then group by slot number *)
      let grouped_pvals = group sorted_pvals ~break:(fun (_,s,_) -> fun (_,s',_) -> not (s=s')) in
        (* Finally map over the list of list of pvalues, selecting the proposal corresponding to
           the maximum ballot number in each sublist *)
        map grouped_pvals ~f:(fun ps -> let (_,s,c) = max_ballot ps in (s,c))

(* xs << ys returns the elements of y and the elements of x not in y *)
let (<<) xs ys =
  let open Core.List in
    append ys (filter xs ~f:(fun x -> not (mem ys x ~equal:proposals_equal)))






module Scout = struct
  type t' = {
    receive_lock : Lwt_mutex.t;  
    mutable pvalues : Pval.t list;
    mutable quorum : (Uri.t, Types.unique_id) Quorum.t
  }

  let receive_phase1b scout b (a,b',pvals) send =
    (* Attempt to lock mutex will either cause this scout to hold the lock
       or block until another scout holding the lock terminates *)
    Lwt_mutex.lock scout.receive_lock >>= fun () ->
    (* Check that, not including this acceptor's response, we already have a majority quorum *)
    (if not (Quorum.is_majority scout.quorum) then
      if Ballot.equal b b' then
        (scout.pvalues <- List.append pvals scout.pvalues; 
         scout.quorum <- Quorum.add scout.quorum a; 
        (* Check if we have a majority now we've added this acceptor *)
        if Quorum.is_majority scout.quorum then
          send @@ Adopted(b, scout.pvalues)
        else
          Lwt.return_unit)
      else
        send @@ Preempted(b')
     else
       Lwt.return_unit) >>= fun () ->
    Lwt_mutex.unlock scout.receive_lock |> Lwt.return
  
  (* Computation performed by the scout sub-process involves sending out phase1 requests
     and waiting for a majority of responses *)
  let run_scout (scout : t') (leader : t) (b : Ballot.t) send =
    (* Iterate through list of uris, sending a phase1 request to each in parallel 
       and call a function to operate over the response *)
    Lwt_list.iter_p (fun uri -> Message.send_phase1_message b uri >>= fun response ->
      receive_phase1b scout b response send) leader.acceptor_uris

  (* Spawn a new scout sub-process *)
  let spawn (leader : t) (ballot_num : Ballot.t) send =
    Lwt.ignore_result (write_with_timestamp INFO ("Spawning new scout for ballot " ^ (Ballot.to_string ballot_num)));
    (* Run this process in the background and don't wait for any sort of result *)
    Lwt.async (fun () -> 
        run_scout { pvalues = []; receive_lock = Lwt_mutex.create ();
                    quorum = Quorum.from_list leader.acceptor_uris }
                  leader ballot_num send)
end









(* Commander module ... *)
module Commander = struct
  type t' = {
    receive_lock : Lwt_mutex.t;
    mutable quorum : (Uri.t, Types.unique_id) Quorum.t
  }

  let receive_phase2b commander pval (a,b') replica_uris send = 
    Lwt_mutex.with_lock commander.receive_lock (fun () ->
      let (b,s,c) = pval in
      (if not (Quorum.is_majority commander.quorum) then
        if Ballot.equal b b' then
          (commander.quorum <- Quorum.add commander.quorum a; 
          (* Check if we have a majority now we've added this acceptor *)
           if Quorum.is_majority commander.quorum then
             (* Broadcast the decision to commit (s,c) to all replicas *)
             Lwt_list.iter_p (Message.send_request @@ Message.DecisionMessage(s,c)) replica_uris
          else Lwt.return_unit)
        else
          send @@ Preempted(b')
      else Lwt.return_unit))

  (* Computation performed by the commander sub-process involves sending out phase1 requests
   and waiting for a majority of responses *)
  let run_commander (commander : t') (leader : t) (pval : Pval.t) send =
    (* Iterate through list of uris, sending a phase1 request to each in parallel 
       and call a function to operate over the response *)
    Lwt_list.iter_p (fun uri -> Message.send_phase2_message pval uri >>= fun response ->
      receive_phase2b commander pval response leader.replica_uris send) leader.acceptor_uris    

  (* Spawn a new commander sub-process *)
  let spawn (leader : t) (pval : Pval.t) send =
    Lwt.ignore_result (write_with_timestamp INFO ("Spawning new commander for pvalue " ^ (Pval.to_string pval)));
    (* Run process in the background and don't wait for a result *)
    Lwt.async (fun () ->
        run_commander { receive_lock = Lwt_mutex.create ();
                        quorum = Quorum.from_list leader.acceptor_uris } 
                      leader pval send)
end















(* Mutex that guards the message queue *)
let queue_guard = Lwt_mutex.create ()

(* Queue that stores the messages returned by sub-processes *)
let message_queue : process_response Queue.t = Queue.of_list []

let send msg = 
  Lwt_mutex.lock queue_guard >|= (fun () ->
  Queue.enqueue message_queue msg) >>= fun () ->
  Lwt_mutex.unlock queue_guard |> Lwt.return


let message_mutex = Lwt_mutex.create ()

let adopt (leader : t) (ballot_num : Ballot.t) (pvals : Pval.t list) =
  Lwt_mutex.with_lock message_mutex (fun () ->
      write_with_timestamp INFO ("Adopted " ^ (Ballot.to_string ballot_num) ^ " with pvals " ^
                                 (Core.List.to_string ~f:(Pval.to_string) pvals)) >|= fun () ->

      (* Update set of proposals *)
      leader.proposals <- (leader.proposals << (pmax pvals));
      (* For each proposal, spawn a commander to attempt to commit it *)
      List.iter leader.proposals ~f:(fun (s,c) -> (
            Commander.spawn leader (leader.ballot_num,s,c) send));
      (* Set active for this leader to true *)
      leader.active <- true)

(*

OLD PREEMPT FUNCTION...

let preempt (leader : t) (b' : Ballot.t) =
  Lwt_mutex.with_lock message_mutex (fun () ->
  match b' with
  | Ballot.Bottom -> raise Invalid_ballot
  | Ballot.Number(r',l') ->
    if Ballot.less_than leader.ballot_num b' then
      write_with_timestamp INFO ("Preempted with ballot " ^ (Ballot.to_string b')) >|= fun () ->    
      (leader.active <- false;
       leader.ballot_num <- Number(r' + 1, leader.id);
       Scout.spawn leader leader.ballot_num send)
    else Lwt.return_unit)
*)

let preempt (leader : t) (b' : Ballot.t) =
  Lwt_mutex.with_lock message_mutex (fun () ->
    if Ballot.less_than leader.ballot_num b' then
      write_with_timestamp INFO ("Preempted with ballot " ^ (Ballot.to_string b')) >|= fun () ->    
      (leader.active <- false;
       leader.ballot_num <- Ballot.succ_exn b';
       Scout.spawn leader leader.ballot_num send)
    else Lwt.return_unit)

(* Process messages as they are added to the queue *)
let rec receive (leader : t) =
  Lwt_unix.sleep 0.00001 >>= fun () -> (* This is required for some reason... *)
  Lwt_mutex.lock queue_guard >>= fun () ->
  (match Queue.dequeue message_queue with
   | None -> Lwt.return_unit
   | Some msg -> match msg with
    | Adopted(ballot_num,pvals) -> adopt leader ballot_num pvals
    | Preempted(b') -> preempt leader b') >>= fun () ->
  Lwt_mutex.unlock queue_guard;
  receive leader

(* Test to signify we have received a proposal message at leader *)
let receive_proposal (leader : t) (p : Types.proposal) =
  (* Check if command is already in set of proposals for any slot number *)
  let (s,c) = p in
  if not (List.Assoc.mem leader.proposals ~equal:(=) s) then
    (Lwt.ignore_result ( write_with_timestamp INFO ("Receive proposal " ^ (Types.string_of_proposal p) ^ ", add to set of proposals"));    
    leader.proposals <- List.append leader.proposals [p];
    if leader.active then
      Commander.spawn leader (leader.ballot_num,s,c) send)
  else
    Lwt.ignore_result ( write_with_timestamp INFO ("Receive and ignore proposal " ^ Types.string_of_proposal p) )
















(* Print line describing URI to terminal *)
let print_uri uri =
  Lwt_io.printl ("Spinning up a leader with URI: " ^ (Uri.to_string uri));;  

(* Initialize a server for a given leader on a given host and port *)
let start_server (leader : t) (host : string) (port : int) =
  Message.start_new_server ~proposal_callback:(receive_proposal leader) host port;;

let new_leader host port replica_uris acceptor_uris = 
  (* Initialize a new leader *)
  let leader = initialize replica_uris acceptor_uris in
  
  (* Return a Lwt thread that is the main execution context
     of the leader *)
  Lwt.join [
    (start_server leader host port >>= fun uri ->
     print_uri uri >>= fun () ->
     Scout.spawn leader (Ballot.init leader.id) send;
     Lwt.wait () |> fst);
     receive leader]

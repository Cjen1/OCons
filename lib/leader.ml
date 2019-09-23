(* leader.ml *)

open Types
open Lwt.Infix
open Core
open Log.Logger

(* Types of leaders *)
type t =
  { id: leader_id
  ; mutable replica_uris: Uri.t list
  ; mutable acceptor_uris: Uri.t list
  ; mutable ballot_num: Ballot.t
  ; mutable active: bool
  ; mutable proposals: proposal list
  ; mutable replica_slot_outs: (replica_id, slot_number, UUID.comparator_witness) Base.Map.t
  ; f: int }

(* Types of responses that can be returned by scout and commander sub-processes *)
type process_response =
  | Adopted of Ballot.t * Pval.t list
  | Preempted of Ballot.t

(* Create a new leader *)
let initialize replica_uris leader_uris acceptor_uris =
  let id = create_id () in
  { id
  ; replica_uris
  ; acceptor_uris
  ; ballot_num= Ballot.init id
  ; active= false
  ; proposals= []
  ; (* Initially create empty map. This will eventually get populated with <id,
     * slot> pairs and no garbage collection will be carried out until sufficiently
     * many replica_ids have been collected.
     *)
    replica_slot_outs= Base.Map.empty (module Uuid)
  ; (* Potentially overestimating nr of failures is admissible here because we 
     * only use the value of f to determine when to garbage collect. This means
     * that we can safely ignore the number of leaders here. 
     *)
    f=
      min
        ((List.length acceptor_uris - 1) / 2)
        (min (List.length replica_uris - 1) (List.length leader_uris)) }

(* Given a list of pvalues, return the pvalue with the greatest ballot number.
   Achieve this by first sorting the ballots in the list and then selecting the
   tail of the list *)
let max_ballot (pvals : Pval.t list) : Pval.t =
  let open Core.List in
  let comp (b, _, _) (b', _, _) = -Ballot.compare b b' in
  hd_exn (stable_sort ~compare:comp pvals)

let pmax (pvals : Pval.t list) : proposal list =
  let open Core.List in
  (* Sort the list by slot number *)
  let sorted_pvals =
    stable_sort ~compare:(fun (_, s, _) (_, s', _) -> Int.compare s s') pvals
  in
  (* Then group by slot number *)
  let grouped_pvals =
    group sorted_pvals ~break:(fun (_, s, _) (_, s', _) -> not (s = s'))
  in
  (* Finally map over the list of list of pvalues, selecting the proposal corresponding to
           the maximum ballot number in each sublist *)
  map grouped_pvals ~f:(fun ps ->
      let _, s, c = max_ballot ps in
      (s, c))

(* xs << ys returns the elements of y and the elements of x not in y *)
let ( << ) xs ys =
  let open Core.List in
  append ys (filter xs ~f:(fun x -> not (mem ys x ~equal:proposals_equal)))

module Scout = struct
  (* Accumulates 1b responses from acceptors in quorum until
     a majority is achieved.
   * *)
  type t' =
    { receive_lock: Lwt_mutex.t
    ; mutable pvalues: Pval.t list
    ; mutable quorum: (Uri.t, Types.unique_id) Quorum.t }

  let receive_phase1b scout b (a, b', pvals, sl_gc) send =
    (* Attempt to lock mutex will either cause this scout to hold the lock
       or block until another scout holding the lock terminates *)
    let%lwt () = Lwt_mutex.lock scout.receive_lock in
    (* Check that, not including this acceptor's response, we already have a majority quorum *)
    let%lwt () =
      if not (Quorum.is_majority scout.quorum) then
        match Ballot.equal b b' with
        | true ->
            scout.pvalues <-
              List.append pvals
                (List.filter ~f:(fun (_, s, _) -> s >= sl_gc) scout.pvalues) ;
            scout.quorum <- Quorum.add scout.quorum a ;
            (* Check if we have a majority now we've added this acceptor *)
            if Quorum.is_majority scout.quorum then
              send (Adopted (b, scout.pvalues))
            else Lwt.return_unit
        | false ->
            send @@ Preempted b'
      else Lwt.return_unit
    in
    Lwt_mutex.unlock scout.receive_lock |> Lwt.return

  (* Computation performed by the scout sub-process involves sending out phase1 requests
     and waiting for a majority of responses *)
  let run_scout (scout : t') (leader : t) (b : Ballot.t) send =
    (* Iterate through list of uris, sending a phase1 request to each in parallel 
       and call a function to operate over the response *)
    Lwt_list.iter_p
      (fun uri ->
        let%lwt response = Message.send_phase1_message b uri in
        receive_phase1b scout b response send)
      leader.acceptor_uris

  (* Spawn a new scout sub-process *)
  let spawn (leader : t) (ballot_num : Ballot.t) send =
    Lwt.ignore_result
      (write_with_timestamp INFO
         ("Spawning new scout for ballot " ^ Ballot.to_string ballot_num)) ;
    (* Run this process in the background and don't wait for any sort of result *)
    Lwt.async (fun () ->
        run_scout
          { pvalues= []
          ; receive_lock= Lwt_mutex.create ()
          ; quorum= Quorum.from_list leader.acceptor_uris }
          leader ballot_num send)
end

(* Commander module ... *)
module Commander = struct
  type t' =
    { receive_lock: Lwt_mutex.t
    ; mutable quorum: (Uri.t, Types.unique_id) Quorum.t }

  let receive_phase2b commander pval (a, b') replica_uris send =
    Lwt_mutex.with_lock commander.receive_lock (fun () ->
        let b, s, c = pval in
        if not (Quorum.is_majority commander.quorum) then
          if Ballot.equal b b' then (
            commander.quorum <- Quorum.add commander.quorum a ;
            (* Check if we have a majority now we've added this acceptor *)
            if Quorum.is_majority commander.quorum then
              (* Broadcast the decision to commit (s,c) to all replicas *)
              Lwt_list.iter_p
                (Message.send_request @@ Message.DecisionMessage (s, c))
                replica_uris
            else Lwt.return_unit )
          else send @@ Preempted b'
        else Lwt.return_unit)

  (* Computation performed by the commander sub-process involves sending out phase1 requests
   and waiting for a majority of responses *)
  let run_commander (commander : t') (leader : t) (pval : Pval.t) send =
    (* Iterate through list of uris, sending a phase1 request to each in parallel 
       and call a function to operate over the response *)
    Lwt_list.iter_p
      (fun uri ->
        Message.send_phase2_message pval uri
        >>= fun response ->
        receive_phase2b commander pval response leader.replica_uris send)
      leader.acceptor_uris

  (* Spawn a new commander sub-process *)
  let spawn (leader : t) (pval : Pval.t) send =
    Lwt.ignore_result
      (write_with_timestamp INFO
         ("Spawning new commander for pvalue " ^ Pval.to_string pval)) ;
    (* Run process in the background and don't wait for a result *)
    Lwt.async (fun () ->
        run_commander
          { receive_lock= Lwt_mutex.create ()
          ; quorum= Quorum.from_list leader.acceptor_uris }
          leader pval send)
end

(* Mutex that guards the message queue *)
let queue_guard = Lwt_mutex.create ()

(* Queue that stores the messages returned by sub-processes *)
let message_queue : process_response Queue.t = Queue.of_list []

let send msg =
  Lwt_mutex.lock queue_guard
  >|= (fun () -> Queue.enqueue message_queue msg)
  >>= fun () -> Lwt_mutex.unlock queue_guard |> Lwt.return

let message_mutex = Lwt_mutex.create ()

let adopt (leader : t) (ballot_num : Ballot.t) (pvals : Pval.t list) =
  Lwt_mutex.with_lock message_mutex (fun () ->
      write_with_timestamp INFO
        ( "Adopted "
        ^ Ballot.to_string ballot_num
        ^ " with pvals "
        ^ Core.List.to_string ~f:Pval.to_string pvals )
      >|= fun () ->
      (* Update set of proposals *)
      leader.proposals <- leader.proposals << pmax pvals ;
      (* For each proposal, spawn a commander to attempt to commit it *)
      List.iter leader.proposals ~f:(fun (s, c) ->
          Commander.spawn leader (leader.ballot_num, s, c) send) ;
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
      if Ballot.less_than leader.ballot_num b' then (
        write_with_timestamp INFO
          ("Preempted with ballot " ^ Ballot.to_string b')
        >|= fun () ->
        leader.active <- false ;
        leader.ballot_num <- Ballot.succ_exn b' ;
        Scout.spawn leader leader.ballot_num send )
      else Lwt.return_unit)

(* Process messages as they are added to the queue *)
let rec receive (leader : t) =
  Lwt_unix.sleep 0.00001
  >>= fun () ->
  (* This is required for some reason... *)
  Lwt_mutex.lock queue_guard
  >>= fun () ->
  ( match Queue.dequeue message_queue with
  | None ->
      Lwt.return_unit
  | Some msg -> (
    match msg with
    | Adopted (ballot_num, pvals) ->
        adopt leader ballot_num pvals
    | Preempted b' ->
        preempt leader b' ) )
  >>= fun () ->
  Lwt_mutex.unlock queue_guard ;
  receive leader

(* Test to signify we have received a proposal message at leader *)
let receive_proposal (leader : t) (p : Types.proposal) =
  (* Check if command is already in set of proposals for any slot number *)
  let s, c = p in
  if not (List.Assoc.mem leader.proposals ~equal:( = ) s) then (
    Lwt.ignore_result
      (write_with_timestamp INFO
         ( "Receive proposal " ^ Types.string_of_proposal p
         ^ ", add to set of proposals" )) ;
    leader.proposals <- List.append leader.proposals [p] ;
    if leader.active then Commander.spawn leader (leader.ballot_num, s, c) send
    )
  else
    Lwt.ignore_result
      (write_with_timestamp INFO
         ("Receive and ignore proposal " ^ Types.string_of_proposal p))

let recv_so_update (leader : t) ((slot, rep) : slot_number * replica_id) =
  leader.replica_slot_outs <- Base.Map.set leader.replica_slot_outs rep slot ;
  let sl_os = Base.Map.data leader.replica_slot_outs in
  let unique_cons xs x = if List.mem ~equal:( = ) xs x then xs else x :: xs in
  let vals =
    List.stable_sort
      ~compare:(fun x y -> y - x)
      (List.fold_left ~f:unique_cons ~init:[] sl_os)
  in
  let max_slot_out =
    List.hd
      (List.filter
         ~f:(fun x -> List.count ~f:(fun y -> y >= x) sl_os > leader.f)
         vals)
  in
  let safe_to_gc = match max_slot_out with None -> 1 | Some v -> v in
  leader.proposals <-
    List.filter ~f:(fun (s, _) -> s >= safe_to_gc) leader.proposals

(* Print line describing URI to terminal *)
let print_uri uri =
  Lwt_io.printl ("Spinning up a leader with URI: " ^ Uri.to_string uri)

(* Initialize a server for a given leader on a given host and port *)
let start_server (leader : t) (host : string) (port : int) =
  Message.start_new_server ~proposal_callback:(receive_proposal leader)
    ~slot_out_update_callback:(recv_so_update leader) host port

let new_leader host port replica_uris leader_uris acceptor_uris =
  (* Initialize a new leader *)
  let leader = initialize replica_uris leader_uris acceptor_uris in
  (* Return a Lwt thread that is the main execution context
     of the leader and which cannot ever exit *)
  Lwt.join
    [ (let%lwt uri = start_server leader host port in
       let%lwt () = print_uri uri in
       Scout.spawn leader (Ballot.init leader.id) send)
      (* An unresolvable promise *)
    ; receive leader ]

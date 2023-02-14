open Types
open Utils
open C.Types

let dtraceln = C.Utils.dtraceln

module Make (Act : ActionSig) = struct
  type nonrec config = config

  let config_pp = config_pp

  type nonrec message = message

  let message_pp = message_pp

  type nonrec event = event

  let event_pp = event_pp

  type nonrec action = action

  let action_pp = action_pp

  type nonrec t = t

  let t_pp = t_pp

  open Act

  let get_log_term t idx = if idx < 0 then 0 else (Log.get t.log idx).term

  let send_append_entries ?(force = false) () =
    let ct = A.get t () in
    match ct.node_state with
    | Leader s ->
        let highest = Log.highest ct.log in
        (* Assume we are going to send up to highest to each *)
        let send_f id highest_sent =
          let lo = highest_sent + 1 in
          let hi = highest in
          (* so we want to send the segment [lo -> hi] inclusive *)
          if lo <= hi || force then
            let prev_log_index = lo - 1 in
            let entries = Log.iter_len ct.log ~lo ~hi () in
            send id
            @@ AppendEntries
                 { term= ct.current_term
                 ; leader_commit= A.get (t @> commit_index) ()
                 ; prev_log_index
                 ; prev_log_term= get_log_term ct prev_log_index
                 ; entries }
        in
        IntMap.iter send_f s.rep_sent ;
        A.set
          (t @> node_state @> Leader.rep_sent)
          ~to_:(IntMap.map (fun _ -> highest) s.rep_sent)
          ()
    | _ ->
        assert false

  let transit_follower term =
    Eio.traceln "Follower for term %d" term ;
    A.set (t @> node_state) () ~to_:(Follower {timeout= 0}) ;
    A.set (t @> current_term) () ~to_:term

  let transit_candidate () =
    let cterm = A.get (t @> current_term) () in
    let num_nodes = (A.get (t @> config) ()).num_nodes in
    let new_term =
      let quot, _ = (Int.div cterm num_nodes, cterm mod num_nodes) in
      let curr_epoch_term =
        (quot * num_nodes) + A.get (t @> config @> node_id) ()
      in
      if cterm < curr_epoch_term then curr_epoch_term
      else curr_epoch_term + num_nodes
    in
    Eio.traceln "Candidate for term %d" new_term ;
    A.set (t @> node_state) ()
      ~to_:
        (Candidate {quorum= Quorum.empty ((num_nodes / 2) + 1 - 1); timeout= 0}) ;
    A.set (t @> current_term) () ~to_:new_term ;
    broadcast
    @@ RequestVote {term= new_term; leader_commit= A.get (t @> commit_index) ()}

  let transit_leader () =
    let ct = A.get t () in
    match ct.node_state with
    | Candidate {quorum; _} ->
        Eio.traceln "Leader for term %d" ct.current_term ;
        let per_seq (_, seq) =
          seq
          |> Iter.iter (fun (idx, le) ->
                 if
                   (not (Log.mem ct.log idx))
                   || (Log.get ct.log idx).term < le.term
                 then Log.set ct.log idx le )
        in
        quorum.Quorum.elts |> IntMap.to_seq |> Seq.iter per_seq ;
        (* replace term with current term since we are re-proposing it *)
        for idx = ct.commit_index + 1 to Log.highest ct.log do
          let le = Log.get ct.log idx in
          Log.set ct.log idx {le with term= ct.current_term}
        done ;
        let rep_ackd =
          ct.config.other_nodes |> List.to_seq
          |> Seq.map (fun i -> (i, -1))
          |> IntMap.of_seq
        in
        let rep_sent =
          ct.config.other_nodes |> List.to_seq
          |> Seq.map (fun i -> (i, ct.commit_index))
          |> IntMap.of_seq
        in
        A.set (t @> node_state) ()
          ~to_:(Leader {rep_ackd; rep_sent; heartbeat= 0}) ;
        send_append_entries ~force:true ()
    | _ ->
        assert false

  let if_recv_advance_term e =
    match e with
    | Recv
        ( ( AppendEntries {term; _}
          | AppendEntriesResponse {term; _}
          | RequestVote {term; _}
          | RequestVoteResponse {term; _} )
        , _ )
      when term > A.get A.(t @> current_term) () ->
        transit_follower term
    | _ ->
        ()

  let incr h = h + 1

  let resolve_event e =
    if_recv_advance_term e ;
    match (e, A.get A.(t @> node_state) ()) with
    (* Increment ticks *)
    | Tick, _ ->
        A.map (t @> node_state @> timeout_a) ~f:incr ()
    (* Recv commands *)
    | Commands cs, Leader _ ->
        cs (fun c ->
            Log.add
              (A.get (t @> log) ())
              {command= c; term= A.get (t @> current_term) ()} )
    | Commands _cs, _ ->
        assert false (* only occurs if space_available > 0 and not leader *)
    (* Ignore msgs from lower terms *)
    | ( Recv
          ( ( RequestVote {term; _}
            | RequestVoteResponse {term; _}
            | AppendEntries {term; _}
            | AppendEntriesResponse {term; _} )
          , _ )
      , _ )
      when term < A.get A.(t @> current_term) () ->
        ()
    (* Recv msgs from this term*)
    (* Candidate*)
    | Recv (RequestVoteResponse m, src), Candidate _ ->
        assert (m.term = A.get (t @> current_term) ()) ;
        let entries, _ = m.entries in
        let q_entries =
          entries |> Iter.zip_i
          |> Iter.map (fun (i, e) -> (i + m.start_index, e))
        in
        A.map
          A.(t @> node_state @> Candidate.quorum)
          ~f:(Quorum.add src q_entries) ()
    (* Leader *)
    | Recv (AppendEntriesResponse ({success= Ok idx; _} as m), src), Leader _ ->
        assert (m.term = A.get (t @> current_term) ()) ;
        A.map A.(t @> node_state @> Leader.rep_ackd) () ~f:(IntMap.add src idx)
    | Recv (AppendEntriesResponse ({success= Error idx; _} as m), src), Leader _
      ->
        (* This case happens if a message is lost *)
        assert (m.term = A.get (t @> current_term) ()) ;
        A.map A.(t @> node_state @> Leader.rep_sent) () ~f:(IntMap.add src idx) ;
        dtraceln "Failed to match\n%a" t_pp (A.get t ())
    (* Follower *)
    | Recv (RequestVote m, cid), Follower _ ->
        let t = A.get t () in
        let start = m.leader_commit + 1 in
        let entries = Log.iter_len t.log ~lo:start () in
        send cid
        @@ RequestVoteResponse
             {term= t.current_term; start_index= start; entries}
    | ( Recv
          ( AppendEntries
              {prev_log_term; prev_log_index; entries; leader_commit; _}
          , lid )
      , Follower _ ) ->
        (* Reset leader alive timeout *)
        A.set (t @> node_state @> Follower.timeout) ~to_:0 () ;
        (* reply to append entries request *)
        let ct = A.get t () in
        let rooted_at_start = prev_log_index = -1 && prev_log_term = 0 in
        let matching_index_and_term () =
          Log.mem ct.log prev_log_index
          && (Log.get ct.log prev_log_index).term = prev_log_term
        in
        if not (rooted_at_start || matching_index_and_term ()) then (
          (* Reply with the highest index known not to be replicated *)
          (* This will be the prev_log_index of the next msg *)
          dtraceln
            "Failed to match\n\
             rooted_at_start(%b), matching_index_and_term(%b):\n\
             %a"
            rooted_at_start
            (matching_index_and_term ())
            t_pp ct ;
          send lid
          @@ AppendEntriesResponse
               { term= ct.current_term
               ; success= Error (min (prev_log_index - 1) (Log.highest ct.log))
               } )
        else
          ct.append_entries_length (snd entries);
          let index_iter =
            fst entries |> Iter.zip_i
            |> Iter.map (fun (i, v) -> (i + prev_log_index + 1, v))
          in
          index_iter
          |> Iter.iter (fun (idx, le) ->
                 if
                   (not (Log.mem ct.log idx))
                   || (Log.get ct.log idx).term < le.term
                 then Log.set ct.log idx le ) ;
          let max_entry =
            index_iter |> Iter.map fst |> Iter.fold max prev_log_index
          in
          Log.cut_after ct.log max_entry ;
          A.map (t @> commit_index) ~f:(max leader_commit) () ;
          send lid
          @@ AppendEntriesResponse {term= ct.current_term; success= Ok max_entry}
    (*Invalid or already handled *)
    | _ ->
        ()

  let check_conditions () =
    let ct = A.get t () in
    match ct.node_state with
    (* check if can become leader *)
    | Candidate {quorum; _} when Quorum.satisified quorum ->
        transit_leader ()
    (* send msg if exists entries to send *)
    | Leader _ ->
        send_append_entries ()
    | _ ->
        ()

  let resolve_timeouts () =
    let ct = A.get t () in
    match ct.node_state with
    (* When should ticking result in an action? *)
    | Follower {timeout} when timeout >= ct.config.election_timeout ->
        transit_candidate ()
    | Candidate {timeout; _} when timeout >= ct.config.election_timeout ->
        transit_candidate ()
    | Leader {heartbeat; _} when heartbeat > 0 ->
        send_append_entries ~force:true () ;
        A.set (t @> node_state @> Leader.heartbeat) ~to_:0 ()
    | _ ->
        ()

  let check_commit () =
    let ct = A.get t () in
    match ct.node_state with
    | Leader {rep_ackd; _} ->
        let acks =
          rep_ackd |> IntMap.to_seq
          |> Seq.map (fun (_, v) -> v)
          |> List.of_seq
          |> List.cons (A.get (t @> log) () |> Log.highest)
          |> List.sort (fun a b -> Int.neg @@ Int.compare a b)
        in
        let majority_rep = List.nth acks (ct.config.phase2quorum - 1) in
        A.set (t @> commit_index) ~to_:majority_rep ()
    | _ ->
        ()

  let advance_raw e =
    resolve_event e ;
    resolve_timeouts () ;
    check_conditions () ;
    check_commit ()

  let advance t e = run_side_effects (fun () -> advance_raw e) t
end

module Impl = Make (ImperativeActions)

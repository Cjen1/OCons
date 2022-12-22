open Types
open Utils
open C.Types

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
        s.rep_sent <-
          s.rep_sent
          |> IntMap.mapi (fun id highest_sent ->
                 let start = highest_sent + 1 in
                 let upper = highest in
                 ( if start <= upper || force then
                   let entries = Log.iter_len ct.log ~lo:start ~hi:upper () in
                   (* May want to limit max msg size *)
                   send id
                   @@ AppendEntries
                        { term= ct.current_term
                        ; leader_commit= A.get (t @> commit_index) ()
                        ; prev_log_index= start - 1
                        ; prev_log_term= get_log_term ct (start - 1)
                        ; entries } ) ;
                 upper )
    | _ ->
        assert false

  let transit_follower term =
    A.set (t @> node_state) () ~to_:(Follower {timeout= 0}) ;
    A.set (t @> current_term) () ~to_:term

  let transit_candidate () =
    let cterm = A.get (t @> current_term) () in
    let num_nodes = (A.get (t @> config) ()).num_nodes in
    let quot, _ = (Int.div cterm num_nodes, cterm mod num_nodes) in
    let curr_epoch_term =
      (quot * num_nodes) + A.get (t @> config @> node_id) ()
    in
    let new_term =
      if cterm < curr_epoch_term then curr_epoch_term
      else curr_epoch_term + num_nodes
    in
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
        let per_seq (_, seq) =
          seq
          |> Iter.iter (fun (idx, le) ->
                 if (Log.get ct.log idx).term < le.term then
                   Log.set ct.log idx le )
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

  let resolve_event e =
    if_recv_advance_term e ;
    match (e, A.get A.(t @> node_state) ()) with
    (* Increment ticks *)
    | Tick, Follower {timeout} ->
        A.set (t @> node_state @> Follower.timeout) ~to_:(timeout + 1) ()
    | Tick, Candidate {timeout; _} ->
        A.set (t @> node_state @> Candidate.timeout) ~to_:(timeout + 1) ()
    | Tick, Leader {heartbeat; _} ->
        A.set (t @> node_state @> Leader.heartbeat) ~to_:(heartbeat + 1) ()
    | Commands cs, Leader _ ->
        cs (fun c ->
            Log.add
              (A.get (t @> log) ())
              {command= c; term= A.get (t @> current_term) ()} )
    | Commands _cs, _ ->
        assert false (*TODO*)
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
        A.map A.(t @> node_state @> Leader.rep_sent) () ~f:(IntMap.add src idx)
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
        let ct = A.get t () in
        let rooted_at_start = prev_log_index = -1 && prev_log_term = 0 in
        let matching_index_and_term () =
          Log.mem ct.log prev_log_index
          && (Log.get ct.log prev_log_index).term = prev_log_term
        in
        if not (rooted_at_start || matching_index_and_term ()) then
          (* Reply with the highest index known not to be replicated *)
          (* This will be the prev_log_index of the next msg *)
          send lid
          @@ AppendEntriesResponse
               { term= ct.current_term
               ; success= Error (min (prev_log_index - 1) (Log.highest ct.log))
               }
        else
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
          let max_entry = index_iter |> Iter.map fst |> Iter.fold max (-1) in
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
        send_append_entries () ;
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
          |> List.sort (fun a b -> Int.neg @@ Int.compare a b)
        in
        let majority_rep = List.nth acks (Int.div ct.config.num_nodes 2 - 1) in
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

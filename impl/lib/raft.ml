open Types
open Utils
open C.Types
open Actions_f
open A.O

let dtraceln = Utils.dtraceln

module Make
    (Act : ActionSig
             with type t = RaftTypes.t
              and type message = RaftTypes.message
              and type action = RaftTypes.action) =
struct
  include RaftTypes
  open Act

  let ex = ()

  let get_log_term log idx = if idx < 0 then 0 else (Log.get log idx).term

  let send_append_entries ?(force = false) () =
    let ct = ex.@(t) in
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
                 ; leader_commit= ex.@(t @> commit_index)
                 ; prev_log_index
                 ; prev_log_term= get_log_term ct.log prev_log_index
                 ; entries }
        in
        IntMap.iter send_f s.rep_sent ;
        A.set
          (t @> node_state @> Leader.rep_sent)
          ~to_:(IntMap.map (fun _ -> highest) s.rep_sent)
          ()
    | _ ->
        assert false

  let transit_follower ?voted_for term =
    Eio.traceln "Follower for term %d" term ;
    ex.@(t @> node_state) <- Follower {timeout= 0; voted_for} ;
    ex.@(t @> current_term) <- term

  let transit_candidate () =
    let new_term = ex.@(t @> current_term) + 1 in
    let num_nodes = ex.@(t @> config @> num_nodes) in
    Eio.traceln "Candidate for term %d" new_term ;
    (* Vote for self *)
    ex.@(t @> node_state) <-
      Candidate {quorum= Quorum.empty ((num_nodes / 2) + 1 - 1); timeout= 0} ;
    ex.@(t @> current_term) <- new_term ;
    let lastIndex = Log.highest ex.@(t @> log) in
    let lastTerm = get_log_term ex.@(t @> log) lastIndex in
    broadcast @@ RequestVote {term= new_term; lastIndex; lastTerm}

  let transit_leader () =
    let ct = ex.@(t) in
    match ct.node_state with
    | Candidate _ ->
        Eio.traceln "Leader for term %d" ct.current_term ;
        let rep_ackd =
          ct.config.other_nodes |> List.to_seq
          |> Seq.map (fun i -> (i, -1))
          |> IntMap.of_seq
        in
        let rep_sent =
          ct.config.other_nodes |> List.to_seq
          |> Seq.map (fun i -> (i, Log.highest ct.log))
          |> IntMap.of_seq
        in
        ex.@(t @> node_state) <- Leader {rep_ackd; rep_sent; heartbeat= 0} ;
        (* Add a no-op to ensure that we always have a command to commit in the newest leader's term *)
        Log.add
          ex.@(t @> log)
          {command= empty_command; term= ex.@(t @> current_term)} ;
        send_append_entries ~force:true ()
    | _ ->
        assert false

  let if_recv_advance_term e =
    match (e, ex.@(t @> node_state)) with
    | ( Recv
          ( ( AppendEntries {term; _}
            | AppendEntriesResponse {term; _}
            | RequestVote {term; _}
            | RequestVoteResponse {term; _} )
          , _ )
      , _ )
      when term > ex.@(t @> current_term) ->
        transit_follower term
    | Recv (AppendEntries {term; _}, lid), Candidate _
      when term = ex.@(t @> current_term) ->
        transit_follower ~voted_for:lid term
    | _ ->
        ()

  let incr h = h + 1

  let resolve_event e =
    if_recv_advance_term e ;
    match (e, ex.@(A.(t @> node_state))) with
    (* Increment ticks *)
    | Tick, _ ->
        A.map (t @> node_state @> timeout_a) ~f:incr ()
    (* Recv commands *)
    | Commands cs, Leader _ ->
        cs (fun c ->
            Log.add ex.@(t @> log) {command= c; term= ex.@(t @> current_term)} )
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
      when term < ex.@(A.(t @> current_term)) ->
        ()
    (* Recv msgs from this term*)
    (* Candidate*)
    | Recv (RequestVoteResponse {term; success}, src), Candidate _ ->
        assert (term = ex.@(t @> current_term)) ;
        if success then
          A.map
            A.(t @> node_state @> Candidate.quorum)
            ~f:(Quorum.add src ()) ()
    (* Leader *)
    | Recv (AppendEntriesResponse ({success= Ok idx; _} as m), src), Leader _ ->
        assert (m.term = ex.@(t @> current_term)) ;
        A.map A.(t @> node_state @> Leader.rep_ackd) () ~f:(IntMap.add src idx)
    | Recv (AppendEntriesResponse ({success= Error idx; _} as m), src), Leader _
      ->
        (* This case happens if a message is lost *)
        assert (m.term = ex.@(t @> current_term)) ;
        A.map A.(t @> node_state @> Leader.rep_sent) () ~f:(IntMap.add src idx) ;
        dtraceln "Failed to match\n%a" t_pp ex.@(t)
    (* Follower *)
    | Recv (RequestVote m, cid), Follower {voted_for; _} ->
        let highest_index = Log.highest ex.@(t @> log) in
        let highest_term = get_log_term ex.@(t @> log) highest_index in
        let newer_log =
          m.lastTerm > highest_term
          || (m.lastTerm = highest_term && m.lastIndex >= highest_index)
        in
        let success = voted_for = None && newer_log in
        if success then (
          ex.@(t @> node_state @> Follower.voted_for) <- Some cid ;
          send cid
          @@ RequestVoteResponse {term= ex.@(t @> current_term); success} )
    | ( Recv
          ( AppendEntries
              {prev_log_term; prev_log_index; entries; leader_commit; _}
          , lid )
      , Follower _ ) -> (
        (* Reset leader alive timeout *)
        ex.@(t @> node_state @> Follower.timeout) <- 0 ;
        ex.@(t @> node_state @> Follower.voted_for) <- Some lid ;
        (* reply to append entries request *)
        let ct = ex.@(t) in
        let rooted_at_start = prev_log_index = -1 && prev_log_term = 0 in
        let matching_index_and_term () =
          Log.mem ct.log prev_log_index
          && (Log.get ct.log prev_log_index).term = prev_log_term
        in
        match rooted_at_start || matching_index_and_term () with
        | false ->
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
                 ; success=
                     Error (min (prev_log_index - 1) (Log.highest ct.log)) }
        | true ->
            ct.append_entries_length (snd entries) ;
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
            @@ AppendEntriesResponse
                 {term= ct.current_term; success= Ok max_entry} )
    (*Invalid or already handled *)
    | _ ->
        ()

  let check_conditions () =
    let ct = ex.@(t) in
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
    let ct = ex.@(t) in
    match ct.node_state with
    (* When should ticking result in an action? *)
    | Follower {timeout; _} when timeout >= ct.config.election_timeout ->
        transit_candidate ()
    | Candidate {timeout; _} when timeout >= ct.config.election_timeout ->
        transit_candidate ()
    | Leader {heartbeat; _} when heartbeat > 0 ->
        send_append_entries ~force:true () ;
        ex.@(t @> node_state @> Leader.heartbeat) <- 0
    | _ ->
        ()

  let check_commit () =
    let ct = ex.@(t) in
    match ct.node_state with
    | Leader {rep_ackd; _} ->
        let acks =
          rep_ackd |> IntMap.to_seq
          |> Seq.map (fun (_, v) -> v)
          |> List.of_seq
          |> List.cons (ex.@(t @> log) |> Log.highest)
          |> List.sort (fun a b -> Int.neg @@ Int.compare a b)
        in
        let majority_rep = List.nth acks (ct.config.phase2quorum - 1) in
        (* only commit if the commit index is from this term *)
        if get_log_term ex.@(t @> log) majority_rep = ex.@(t @> current_term)
        then ex.@(t @> commit_index) <- majority_rep
    | _ ->
        ()

  let advance_raw e =
    resolve_event e ;
    resolve_timeouts () ;
    check_conditions () ;
    check_commit ()

  let advance t e = run_side_effects (fun () -> advance_raw e) t

  let create config =
    let log = SegmentLog.create {term= -1; command= empty_command} in
    { log
    ; commit_index= -1
    ; config
    ; node_state= Follower {timeout= config.election_timeout; voted_for= None}
    ; current_term= 0
    ; append_entries_length=
        Ocons_core.Utils.InternalReporter.avg_reporter "ae_length" }
end

module Impl = Make (ImperativeActions (RaftTypes))

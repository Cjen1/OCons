open Types
open Utils
open C.Types
open Actions_f
open A.O

let dtraceln = Utils.dtraceln

module type PartialSig = sig
  type t

  type message

  type action
end

module Make
    (Strategy : Strategy)
    (IT : ImplTypes
            with type request_vote = Strategy.StrategyTypes.request_vote
             and type request_vote_response =
              Strategy.StrategyTypes.request_vote_response
             and type quorum_type = Strategy.StrategyTypes.quorum_type)
    (Act : ActionSig
             with type t = IT.t
              and type message = IT.message
              and type action = IT.action) =
struct
  open Types
  open Act
  open IT

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
        ex.@(t @> node_state @> Leader.rep_sent) <-
          IntMap.map (fun _ -> highest) s.rep_sent
    | _ ->
        assert false

  let transit_follower ?voted_for:_ term =
    Eio.traceln "Follower for term %d" term ;
    ex.@(t @> node_state) <- Follower {timeout= 0; voted_for= None} ;
    ex.@(t @> current_term) <- term

  let transit_candidate () =
    let num_nodes = ex.@(t @> config @> num_nodes) in
    (* Vote for self *)
    ex.@(t @> node_state) <-
      Candidate {quorum= Quorum.empty ((num_nodes / 2) + 1 - 1); timeout= 0} ;
    let new_term = Strategy.increment_term () in
    ex.@(t @> current_term) <- new_term ;
    Eio.traceln "Candidate for term %d" new_term ;
    Strategy.send_request_vote ()

  let transit_leader () =
    let ct = ex.@(t) in
    match ct.node_state with
    | Candidate _ ->
        Eio.traceln "Leader for term %d" ct.current_term ;
        Strategy.leader_transit_log_update () ;
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
        ex.@(t @> node_state) <- Leader {rep_ackd; rep_sent; heartbeat= 0} ;
        send_append_entries ~force:true ()
    | _ ->
        assert false

  let if_recv_advance_term e =
    match (e, ex.@(t @> node_state)) with
    | Recv (m, _), _ when m.@(term_a) > ex.@(t @> current_term) ->
        transit_follower m.@(term_a)
    | _ ->
        ()

  let incr h = h + 1

  let resolve_event e =
    if_recv_advance_term e ;
    match (e, ex.@(t @> node_state)) with
    (* Increment ticks *)
    | Tick, (Follower _ | Leader _) ->
        A.map (t @> node_state @> timeout_a) ~f:incr ()
    | Tick, Candidate _ ->
        Strategy.candidate_tick ()
    (* Recv commands *)
    | Commands cs, Leader _ ->
        cs (fun c ->
            Log.add ex.@(t @> log) {command= c; term= ex.@(t @> current_term)} )
    | Commands _cs, _ ->
        assert false (* only occurs if space_available > 0 and not leader *)
    (* Ignore msgs from lower terms *)
    | Recv (m, _), _ when m.@(term_a) < ex.@(t @> current_term) ->
        ()
    (* Recv msgs from this term*)
    (* Candidate*)
    | Recv (RequestVoteResponse m, src), Candidate _ ->
        Strategy.recv_request_vote_response (m, src)
    (* Leader *)
    | Recv (AppendEntriesResponse ({success= Ok idx; _} as m), src), Leader _ ->
        assert (m.term = ex.@(t @> current_term)) ;
        A.map (t @> node_state @> Leader.rep_ackd) () ~f:(IntMap.add src idx)
    | Recv (AppendEntriesResponse ({success= Error idx; _} as m), src), Leader _
      ->
        (* This case happens if a message is lost *)
        assert (m.term = ex.@(t @> current_term)) ;
        A.map (t @> node_state @> Leader.rep_sent) () ~f:(IntMap.add src idx) ;
        dtraceln "Failed to match\n%a" t_pp ex.@(t)
    (* Follower *)
    | Recv (RequestVote m, cid), Follower _ ->
        Strategy.recv_request_vote (m, cid)
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
    | Follower s when s.timeout >= ct.config.election_timeout ->
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

module PaxosStratTypes = struct
  type request_vote = {term: term; leader_commit: log_index}
  [@@deriving accessors ~submodule:RV]

  type request_vote_response =
    {term: term; start_index: log_index; entries: log_entry Iter.t * int}
  [@@deriving accessors ~submodule:RVR]

  type quorum_type = (log_index * log_entry) Iter.t

  type candidate = {mutable quorum: quorum_type Quorum.t; mutable timeout: int}
  [@@deriving accessors]

  module Utils = struct
    open Fmt

    let request_vote_pp ppf (s : request_vote) =
      Fmt.pf ppf "{term:%d; leader_commit:%d}" s.term s.leader_commit

    let request_vote_response_pp ppf (s : request_vote_response) =
      Fmt.pf ppf "{term:%d; start_index:%d; entries_length:%d; entries: %a}"
        s.term s.start_index (snd s.entries)
        (brackets @@ list ~sep:(const char ',') log_entry_pp)
        (fst s.entries |> Iter.to_list)

    let candidate_pp ppf (s : candidate) =
      pf ppf "{quorum:%a, timeout:%d}" Quorum.pp s.quorum s.timeout

    let term_rv = RV.term

    let term_rvr = RVR.term
  end
end

module MakePaxosStrat
    (IT : ImplTypes
            with type request_vote = PaxosStratTypes.request_vote
             and type request_vote_response =
              PaxosStratTypes.request_vote_response
             and type quorum_type = PaxosStratTypes.quorum_type)
    (AT : ActionSig
            with type t = IT.t
             and type message = IT.message
             and type action = IT.action) :
  Strategy with module StrategyTypes = PaxosStratTypes = struct
  open IT
  open AT
  module StrategyTypes = PaxosStratTypes
  open StrategyTypes

  let ex = ()

  let increment_term () =
    let cterm = ex.@(t @> current_term) in
    let num_nodes = ex.@(t @> config @> num_nodes) in
    let quot, _ = (Int.div cterm num_nodes, cterm mod num_nodes) in
    let curr_epoch_term = (quot * num_nodes) + ex.@(t @> config @> node_id) in
    if cterm < curr_epoch_term then curr_epoch_term
    else curr_epoch_term + num_nodes

  let send_request_vote () =
    broadcast
    @@ RequestVote
         {term= ex.@(t @> current_term); leader_commit= ex.@(t @> commit_index)}

  let leader_transit_log_update () =
    let per_seq (_, seq) =
      seq
      |> Iter.iter (fun (idx, (le : log_entry)) ->
             let log = ex.@(t @> log) in
             if (not (Log.mem log idx)) || (Log.get log idx).term < le.term then
               Log.set log idx le )
    in
    ex.@?(t @> node_state @> Candidate.quorum)
    |> Option.get
    |> (fun q -> q.Quorum.elts)
    |> IntMap.to_seq |> Seq.iter per_seq ;
    (* replace term with current term since we are re-proposing it *)
    let ct = ex.@(t) in
    for idx = ct.commit_index + 1 to Log.highest ct.log do
      let le = Log.get ct.log idx in
      Log.set ct.log idx {le with term= ct.current_term}
    done

  let candidate_tick () = send_request_vote ()

  let recv_request_vote (m, cid) =
    let t = ex.@(t) in
    let start = m.leader_commit + 1 in
    let entries = Log.iter_len t.log ~lo:start () in
    send cid
    @@ RequestVoteResponse {term= t.current_term; start_index= start; entries}

  let recv_request_vote_response ((m : request_vote_response), src) =
    assert (m.term = ex.@(t @> current_term)) ;
    let entries, _ = m.entries in
    let q_entries =
      entries |> Iter.zip_i |> Iter.map (fun (i, e) -> (i + m.start_index, e))
    in
    A.map (t @> node_state @> Candidate.quorum) ~f:(Quorum.add src q_entries) ()
end

module MakePaxos (AT : ActionFunc) = struct
  module PaxosTypes = VarPaxosTypes (PaxosStratTypes)
  module AT = AT (PaxosTypes)
  module PaxosStrat = MakePaxosStrat (PaxosTypes) (AT)
  include Make (PaxosStrat) (PaxosTypes) (AT)
end

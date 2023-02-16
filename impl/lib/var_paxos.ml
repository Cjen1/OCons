open Types
open Utils
open C.Types
open Actions_f
open A.O
open Var_impl

module PaxosStratTypes = struct
  type request_vote = {term: term; leader_commit: log_index}
  [@@deriving accessors ~submodule:RV]

  type request_vote_response =
    {term: term; start_index: log_index; entries: log_entry Iter.t * int}
  [@@deriving accessors ~submodule:RVR]

  type quorum_type = (log_index * log_entry) Iter.t

  module Utils = struct
    open Fmt

    let request_vote_pp ppf (s : request_vote) =
      Fmt.pf ppf "{term:%d; leader_commit:%d}" s.term s.leader_commit

    let request_vote_response_pp ppf (s : request_vote_response) =
      Fmt.pf ppf "{term:%d; start_index:%d; entries_length:%d; entries: %a}"
        s.term s.start_index (snd s.entries)
        (brackets @@ list ~sep:(const char ',') log_entry_pp)
        (fst s.entries |> Iter.to_list)

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

  let rep_sent_default () = ex.@(t @> commit_index)
end

module MakePaxos (AT : ActionFunc) = struct
  module PaxosTypes = VarImplTypes (PaxosStratTypes)
  module AT = AT (PaxosTypes)
  module PaxosStrat = MakePaxosStrat (PaxosTypes) (AT)
  include Make (PaxosStrat) (PaxosTypes) (AT)
end

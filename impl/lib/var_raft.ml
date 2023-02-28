open Types
open Utils
open C.Types
open Actions_f
open A.O
open Var_impl

module RaftStratTypes = struct
  type request_vote = {term: term; lastIndex: log_index; lastTerm: term}
  [@@deriving accessors ~submodule:RV]

  type request_vote_response = {term: term; success: bool}
  [@@deriving accessors ~submodule:RVR]

  type quorum_type = unit

  module Utils = struct
    let request_vote_pp ppf (s : request_vote) =
      Fmt.pf ppf "{term:%d; lastIndex:%d; lastTerm:%d}" s.term s.lastIndex
        s.lastTerm

    let request_vote_response_pp ppf (s : request_vote_response) =
      Fmt.pf ppf "{term:%d; success:%b}" s.term s.success

    let term_rv = RV.term

    let term_rvr = RVR.term

    open Eio.Buf_write

    let serialise_request_vote (s : request_vote) w =
      BE.uint64 w Int64.(of_int s.term) ;
      BE.uint64 w Int64.(of_int s.lastIndex) ;
      BE.uint64 w Int64.(of_int s.lastTerm)

    let serialise_request_vote_response (s : request_vote_response) w =
      BE.uint64 w Int64.(of_int s.term) ;
      Line_prot.SerPrim.bool s.success w

    open Eio.Buf_read
    open Eio.Buf_read.Syntax

    let uint64 = map Int64.to_int BE.uint64

    let parse_request_vote =
      let* term = uint64 and* lastIndex = uint64 and* lastTerm = uint64 in
      return @@ {term; lastIndex; lastTerm}

    let parse_request_vote_response =
      let* term = uint64 and* success = Line_prot.DeserPrim.bool in
      return @@ {term; success}
  end
end

module MakeRaftStrat
    (IT : ImplTypes
            with type request_vote = RaftStratTypes.request_vote
             and type request_vote_response =
              RaftStratTypes.request_vote_response
             and type quorum_type = RaftStratTypes.quorum_type)
    (AT : ActionSig
            with type t = IT.t
             and type message = IT.message
             and type action = IT.action) :
  Strategy with module StrategyTypes = RaftStratTypes = struct
  open IT
  open AT
  module StrategyTypes = RaftStratTypes
  open StrategyTypes

  let ex = ()

  let increment_term () = ex.@(t @> current_term) + 1

  let send_request_vote () =
    let lastIndex = Log.highest ex.@(t @> log) in
    let lastTerm = get_log_term ex.@(t @> log) lastIndex in
    broadcast @@ RequestVote {term= ex.@(t @> current_term); lastIndex; lastTerm}

  let leader_transit_log_update () =
    Log.add
      ex.@(t @> log)
      {command= empty_command; term= ex.@(t @> current_term)}

  let candidate_tick () =
    A.map (t @> node_state @> timeout_a) ~f:(fun i -> i + 1) ()

  let recv_request_vote (m, cid) =
    let highest_index = Log.highest ex.@(t @> log) in
    let highest_term = get_log_term ex.@(t @> log) highest_index in
    let newer_log =
      m.lastTerm > highest_term
      || (m.lastTerm = highest_term && m.lastIndex >= highest_index)
    in
    let success =
      A.get_option (t @> node_state @> Follower.voted_for) ()
      |> Option.get = None
      && newer_log
    in
    if success then (
      ex.@(t @> node_state @> Follower.voted_for) <- Some cid ;
      send cid @@ RequestVoteResponse {term= ex.@(t @> current_term); success} )

  let recv_request_vote_response ((m : request_vote_response), src) =
    assert (m.term = ex.@(t @> current_term)) ;
    if m.success then
      A.map A.(t @> node_state @> Candidate.quorum) ~f:(Quorum.add src ()) ()

  let rep_sent_default () = Log.highest ex.@(t @> log) - 1
end

module MakeRaft (AT : ActionFunc) = struct
  module RaftTypes = VarImplTypes (RaftStratTypes)
  module AT = AT (RaftTypes)
  module RaftStrat = MakeRaftStrat (RaftTypes) (AT)
  include RaftTypes
  include Make (RaftStrat) (RaftTypes) (AT)
  include Line_prot.VarLineProt (RaftStratTypes) (RaftTypes)
end

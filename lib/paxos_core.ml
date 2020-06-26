open Types
open Utils
open Base

(* To do this there are several changes required.
 * 
 * 1. Only the leader can respond to client requests
 * 2. In the failure case prospective leaders need to also get any missing client requests
        Thus request_vote_response messages must contain that info
        Thus request_vote messages must contain the highest known client_request
 *)

type index = int64

type term = int64

type command_id = int64

module LOG = struct
  type t =
    { last_index: index
    ; last_term: term
    ; entries: (Int64.t, command_id, Int64.comparator_witness) Map.t }

  let entries_after t index = Map.split
end

type node_id = int64

type request_vote = {term: int64; leader_commit: int64}

type request_vote_response =
  {term: int64; voteGranted: bool; entries: log_entry list; startIndex: int64}

type request = {term: int64}

(* R before means receiving *)
type event =
  [ `Tick
  | `RRequestVote of request_vote
  | `RRequestVoteResponse of request_vote_response
  | `RAppendEntries of request
  | `RAppendEntiresResponse of request ]

type action =
  [ `SendRequestVote
  | `SendRequestVoteResponse
  | `SendAppendEntries
  | `SendAppendEntriesResponse ]

type config

type node_state = Follower | Candidate | Leader

type t = {config: config; leader_exists: bool; currentTerm: Term.t}

let transition_to_candidate t = assert false

let transition_to_follower t = assert false

let update_current_term term t = assert false

let preempted t term =
  if Int64.(t.term < term) then
    t |> update_current_term term |> transition_to_follower
  else t

let handle t : event -> t * action list = function
  | `Tick when not t.leader_exists ->
      (transition_to_candidate (), [])
  | `Tick ->
      ({t with leader_exists= false}, [])
  | `RRequestVote {term; _}
  | `RRequestVoteResponse {term; _}
  | `RAppendEntries {term; _}
  | `RAppendEntiresResponse {term; _}
    when Int64.(t.currentTerm.t < term) ->
      (t |> update_current_term term |> transition_to_follower, [])
  | `RRequestVote s ->
      assert false
  | `RRequestVoteResponse s ->
      assert false
  | `RAppendEntries s ->
      assert false
  | `RAppendEntiresResponse s ->
      assert false

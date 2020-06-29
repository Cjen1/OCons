open Types

type t

(* R before means receiving *)
type event =
  [ `Tick
  | `RRequestVote of node_id * request_vote
  | `RRequestVoteResponse of node_id * request_vote_response
  | `RAppendEntries of node_id * append_entries
  | `RAppendEntiresResponse of node_id * append_entries_response
  | `LogAddition ]

type action =
  [ `SendRequestVote of node_id * request_vote
  | `SendRequestVoteResponse of node_id * request_vote_response
  | `SendAppendEntries of node_id * append_entries
  | `SendAppendEntriesResponse of node_id * append_entries_response
  | `CommitIndexUpdate of log_index ]

type config =
  {
    phase1majority: int
  ; phase2majority: int
  ; other_nodes: node_id list
  ; num_nodes : int
  ; node_id : node_id
  }

val handle : t -> event -> t * action list

val create_node : config -> Log.t -> Term.t -> t

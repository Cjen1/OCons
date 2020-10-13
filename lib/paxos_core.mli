val src : Logs.src

module L = Types.Log
module T = Types.Term

type event =
  [ `Tick
  | `RRequestVote of Types.node_id * Types.MessageTypes.request_vote
  | `RRequestVoteResponse of
    Types.node_id * Types.MessageTypes.request_vote_response
  | `RAppendEntries of Types.node_id * Types.MessageTypes.append_entries
  | `RAppendEntiresResponse of
    Types.node_id * Types.MessageTypes.append_entries_response
  | `Commands of Types.command list ]

type persistant_change = [`Log of L.op | `Term of T.op]

type action =
  [ `SendRequestVote of Types.node_id * Types.MessageTypes.request_vote
  | `SendRequestVoteResponse of
    Types.node_id * Types.MessageTypes.request_vote_response
  | `SendAppendEntries of Types.node_id * Types.MessageTypes.append_entries
  | `SendAppendEntriesResponse of
    Types.node_id * Types.MessageTypes.append_entries_response
  | `CommitIndexUpdate of Types.log_index
  | `PersistantChange of persistant_change
  | `Sync
  | `Unapplied of Types.command list ]

val compare_apply_order :
     [> `CommitIndexUpdate of 'a | `PersistantChange of 'b | `Sync]
  -> [> `CommitIndexUpdate of 'c | `PersistantChange of 'd | `Sync]
  -> int

val pp_action : Format.formatter -> action -> unit

type config =
  { phase1majority: int
  ; phase2majority: int
  ; other_nodes: Types.node_id list
  ; num_nodes: int
  ; node_id: Types.node_id
  ; election_timeout: int }
[@@deriving sexp]

type node_state [@@deriving sexp_of]

val pp_node_state : Format.formatter -> node_state -> unit

type t [@@deriving sexp_of]

val is_leader : t -> bool

val create_node : config -> L.t -> Types.Term.t -> t

val advance : t -> event -> (t * action  list, [>`Msg of string]) result

val get_log : t -> Types.log
val get_term : t -> Types.term

module Test : sig
  module Comp : sig
    type 'a t = 'a * action list
  end

  module CompRes : sig
    type ('a, 'b) t = ('a Comp.t, 'b) Result.t
  end

  val transition_to_leader : t -> (t, [> `Msg of string]) CompRes.t

  val transition_to_candidate : t -> (t, [> `Msg of string]) CompRes.t

  val transition_to_follower : t -> (t, [> `Msg of string]) CompRes.t

  val advance : t -> event -> (t, [> `Msg of string]) CompRes.t

  val get_node_state : t -> node_state

  val get_commit_index : t -> Types.log_index
end

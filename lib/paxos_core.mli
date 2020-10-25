val src : Logs.src

open Types
open Types.MessageTypes
module L = Log
module T = Term

type event =
  [ `Tick
  | `RRequestVote of Types.MessageTypes.request_vote
  | `RRequestVoteResponse of Types.MessageTypes.request_vote_response
  | `RAppendEntries of Types.MessageTypes.append_entries
  | `RAppendEntiresResponse of Types.MessageTypes.append_entries_response
  | `Commands of Types.command list ]

type persistant_change = [`Log of L.op | `Term of T.op]

type pre_sync_action =
  [ `PersistantChange of persistant_change
  | `SendRequestVote of node_id * request_vote
  | `SendAppendEntries of node_id * append_entries
  | `Unapplied of command list ]

type post_sync_action =
  [ `SendRequestVoteResponse of node_id * request_vote_response
  | `SendAppendEntriesResponse of node_id * append_entries_response
  | `CommitIndexUpdate of log_index ]

type do_sync = bool

type action_sequence = pre_sync_action list * do_sync * post_sync_action list

val pp_action :
  Format.formatter -> [< pre_sync_action | post_sync_action] -> unit

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

val advance : t -> event -> (t * action_sequence, [> `Msg of string]) result

val get_log : t -> Types.log

val get_term : t -> Types.term

module Test : sig
  module Comp : sig
    type 'a t = 'a * action_sequence
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

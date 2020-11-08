open Types
open Types.MessageTypes

val logger : Async.Log.t

(** All the events incomming into the advance function *)
type event =
  [ `Tick
  | `RRequestVote of Types.MessageTypes.request_vote  (** Request Vote msg *)
  | `RRequestVoteResponse of Types.MessageTypes.request_vote_response
    (** Request Vote response msg *)
  | `RAppendEntries of Types.MessageTypes.append_entries
    (** Append Entries msg *)
  | `RAppendEntiresResponse of Types.MessageTypes.append_entries_response
    (** Append Entries Response msg*)
  | `Commands of Types.command list  (** Commands received from clients *) ]

(** Actions which can be emitted by the state machine *)

type persistant_change = [`Log of Wal.Log.op | `Term of Wal.Term.op]

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

(** Return type of advance, post_sync actions must be done after the persistant state is stored to disk *)
type action_sequence = pre_sync_action list * do_sync * post_sync_action list

val pp_action :
  Format.formatter -> [< pre_sync_action | post_sync_action] -> unit

val pp_event : Format.formatter -> event -> unit

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

val is_leader : t -> term option
(** Returns the term that the node thinks it is the leader of *)

val create_node : config -> Wal.Log.t -> term -> t
(** [create_node config log term] returns the initialised state machine. It is initially a follower one tick away from calling an election*)

val advance : t -> event -> (t * action_sequence, [> `Msg of string]) result
(** [advance t event] applies the event to the state machine and returns the updated state machine and any actions to take. If this fails it returns an error message *)

val get_log : t -> Types.log

val get_term : t -> Types.term

(** Module for testing internal state *)
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

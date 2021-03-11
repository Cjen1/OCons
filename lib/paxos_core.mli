open Types
open Types.MessageTypes
module S = IStorage

val logger : Async.Log.t

(* More verbose logging for entry and exit of module *)
val io_logger : Async.Log.t

(** All the events incomming into the advance function *)
type event =
  [ `Tick
  | `Syncd of log_index
  | `RRequestVote of Types.MessageTypes.request_vote  (** Request Vote msg *)
  | `RRequestVoteResponse of Types.MessageTypes.request_vote_response
    (** Request Vote response msg *)
  | `RAppendEntries of Types.MessageTypes.append_entries
    (** Append Entries msg *)
  | `RAppendEntiresResponse of Types.MessageTypes.append_entries_response
    (** Append Entries Response msg*)
  | `Commands of Types.command list  (** Commands received from clients *) ]

(** Actions which can be emitted by the state machine *)

type action =
  [ `Unapplied of command list
  | `SendRequestVote of node_id * request_vote
  | `SendAppendEntries of node_id * append_entries
  | `SendRequestVoteResponse of node_id * request_vote_response
  | `SendAppendEntriesResponse of node_id * append_entries_response
  | `CommitIndexUpdate of log_index ]
[@@deriving sexp]

type actions = {acts: action list; nonblock_sync: bool}
[@@deriving sexp, accessors]

val pp_action : Format.formatter -> action -> unit

val pp_event : Format.formatter -> event -> unit

type config =
  { phase1quorum: int
  ; phase2quorum: int
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

val create_node : config -> S.t -> t
(** [create_node config log term] returns the initialised state machine. It is initially a follower one tick away from calling an election*)

val get_max_index : t -> log_index

val get_term : t -> Types.term

val advance : t -> event -> (t * actions, [> `Msg of string]) result
(** [advance t event] applies the event to the state machine and returns the updated state machine and any actions to take. If this fails it returns an error message *)

val pop_store : t -> t * S.t
(** [pop_store t] pops the store and removes any pending operations from the internal one *)

(** Module for testing internal state *)
module Test : sig

  module State : sig
    type state = {t : t; a : actions}
    val empty : t -> state
  end 

  module StateR : sig
    type ('a, 'b) t
    val eval : ('a, 'b) t -> State.state -> ('a * State.state, 'b) result
    module Let_syntax : sig
      module Let_syntax : sig
        val bind : ('a, 'b) t -> f:('a -> ('c, 'b) t) -> ('c, 'b) t
      end 
    end 
  end

  val transition_to_leader : unit -> (unit, [> `Msg of string]) StateR.t

  val transition_to_candidate : unit -> (unit, [> `Msg of string]) StateR.t

  val transition_to_follower : unit -> (unit, [> `Msg of string]) StateR.t

  val get_node_state : t -> node_state

  val get_commit_index : t -> Types.log_index

  val get_store : t -> S.t
end

open Ocons_core
open Types

type config =
  { phase1quorum: int
  ; phase2quorum: int
  ; other_nodes: node_id list
  ; num_nodes: int
  ; node_id: node_id
  ; election_timeout: int }
[@@deriving sexp]

val make_config : node_id:node_id -> node_list: node_id list -> election_timeout:int -> config

module Make (S : Immutable_store_intf.S) : sig
  include Consensus_intf.S with type config = config [@@deriving sexp_of]

  (** Module for testing internal state *)
  module Test : sig
    type node_state [@@deriving sexp_of]

    module State : sig
      type state = {t: t; a: actions}

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
end

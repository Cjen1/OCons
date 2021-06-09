open Ocons_core
open Types

val logger : Async.Log.t

val io_logger : Async.Log.t

type config =
  { phase1quorum: int
  ; phase2quorum: int
  ; other_nodes: node_id list
  ; num_nodes: int
  ; node_id: node_id
  ; election_timeout: int }
[@@deriving sexp]

val make_config :
  node_id:node_id -> node_list:node_id list -> election_timeout:int -> config

module Make (S : Immutable_store_intf.S) : sig
  include
    Consensus_intf.S with type config = config and type store = S.t
  [@@deriving sexp_of]
end

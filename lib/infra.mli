open! Core
open! Async
open! Types

val logger : Log.t

(** [infra_config] is the configuration settings for the infrastructure
    [node_list] is a list of pairs of node_ids and addresses (eg 127.0.0.1:5001)
    [datadir] is the location of the persistant data
    [tick_speed] is the frequency at which the background thread ticks the state machine
*)

type infra_config =
  { node_id: node_id
  ; node_list: (node_id * string) list
  ; datadir: string
  ; external_port: int
  ; internal_port: int
  ; tick_speed: Time.Span.t
  ; batch_size: int
  ; batch_timeout: Time.Span.t }
[@@deriving sexp_of]

module Make (C : Consensus_intf.S) : sig
  type t

  val create : infra_config -> C.config -> t Deferred.t
  (** [create] returns a new node after it has loaded its state from file.*)

  val close : t -> unit Deferred.t
  (** [close t] closes any outgoing connections, the incomming server and the write-ahead log *)
end

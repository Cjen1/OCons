open Types

module type S = sig
  val logger : Async.Log.t

  (* More verbose logging for entry and exit of module *)
  val io_logger : Async.Log.t

  (* Incomming and outgoing messages should be symmetrical *)
  type message [@@deriving sexp_of, bin_io]

  (** All the events incomming into the advance function *)
  type event = [`Tick | `Syncd of log_index | `Recv of message | `Commands of command list]
  [@@deriving sexp_of]

  (** Actions which can be emitted by the implementation *)
  type action =
    [`Unapplied of command list | `Send of node_id * message | `CommitIndexUpdate of log_index]
  [@@deriving sexp_of]

  type actions = {acts: action list; nonblock_sync: bool}
  [@@deriving sexp_of, accessors]

  type config [@@deriving sexp_of]

  type t

  module Store : Immutable_store_intf.S

  type store = Store.t

  val create_node : config -> store -> t
  (** [create_node config log term] returns the initialised state machine. *)

  val advance : t -> event -> (t * actions, [> `Msg of string]) result
  (** [advance t event] applies the event to the state machine and returns the updated state machine and any actions to take. If this fails it returns an error message *)

  val pop_store : t -> t * store
  (** [pop_store t] pops the store and removes any pending operations from the internal one *)
end

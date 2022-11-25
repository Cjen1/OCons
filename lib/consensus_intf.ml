open Types

type 'a iter = ('a -> unit) -> unit


module type S = sig
  type command

  (* Incomming and outgoing messages should be symmetrical *)
  type message [@@deriving sexp_of, bin_io]

  (** All the events incomming into the advance function *)
  type event = [`Tick | `Recv of message | `Commands of command list]
  [@@deriving sexp_of]


  (** Actions which can be emitted by the implementation *)
  type action =
    | Send of int * message
    | Broadcast of message
    | CommitCommands of command iter

  type config [@@deriving sexp_of]

  type t

  type store

  val create_node : config -> store -> t
  (** [create_node config log term] returns the initialised state machine. *)

  val advance : t -> event -> (t * action list, [> `Msg of string]) result
  (** [advance t event] applies the event to the state machine and returns the updated state machine and any actions to take. If this fails it returns an error message *)
  (* If the event is new commands to add to the log, *)

  val remaining_inflight_entries : t -> int
  (* The number of entries which can be added *)
end

open! Types

exception FailedAdvance of string

module type S = sig
  (** Incomming and outgoing messages should be symmetrical *)
  type message [@@deriving sexp_of]

  (** All the events incomming into the advance function *)
  type event = Tick | Recv of (message * node_id) | Commands of command iter

  (** Actions which can be emitted by the implementation *)
  type action =
    | Send of int * message
    | Broadcast of message
    | CommitCommands of command iter

  val parse : Eio.Buf_read.t -> message
  (** Reads the message from the buf_read*)

  val serialise : message -> Eio.Buf_write.t -> unit
  (** Allows for copy-less serialisation of the message to the buf_write *)

  type config [@@deriving sexp_of]

  type t

  val create_node : config -> t
  (** [create_node config] returns the initialised state machine. *)

  val advance : t -> event -> t * action list
  (** [advance t event] applies the event to the state machine and returns the updated state machine and any actions to take. If this fails it returns an error message *)
  (* If the event is new commands to add to the log, *)

  val available_space_for_commands : t -> int
  (* The number of entries which can be added *)
end

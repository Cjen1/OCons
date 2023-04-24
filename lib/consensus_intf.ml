open! Types

exception FailedAdvance of string

(** All the events incomming into the advance function *)
type 'm event = Tick | Recv of ('m * node_id) | Commands of command Iter.t

let event_pp ?pp_msg ppf v =
  let open Fmt in
  match (v, pp_msg) with
  | Tick, _ ->
      pf ppf "Tick"
  | Recv (m, src), Some message_pp ->
      pf ppf "Recv(%a, %d)" message_pp m src
  | Recv (_, src), None ->
      pf ppf "Recv(_, %d)" src
  | Commands _, _ ->
      pf ppf "Commands"

(** Actions which can be emitted by the implementation *)
type 'm action =
  | Send of int * 'm
  | Broadcast of 'm
  | CommitCommands of command Iter.t

let action_pp ?pp_msg ppf v =
  let open Fmt in
  match (v, pp_msg) with
  | Send (d, m), Some message_pp ->
      pf ppf "Send(%d, %a)" d message_pp m
  | Broadcast m, Some message_pp ->
      pf ppf "Broadcast(%a)" message_pp m
  | Send (d, _), None ->
      pf ppf "Send(%d, _)" d
  | Broadcast _, None ->
      pf ppf "Broadcast(_)"
  | CommitCommands cs, _ ->
      pf ppf "CommitCommands(%a)" (Iter.pp_seq ~sep:"," Types.Command.pp) cs

module type S = sig
  (** Incomming and outgoing messages should be symmetrical *)
  type message

  val message_pp : message Fmt.t

  val parse : Eio.Buf_read.t -> message
  (** Reads the message from the buf_read*)

  val serialise : message -> Eio.Buf_write.t -> unit
  (** Allows for copy-less serialisation of the message to the buf_write *)

  type config

  val config_pp : config Fmt.t

  type t

  val t_pp : t Fmt.t

  val create_node : node_id -> config -> t
  (** [create_node config] returns the initialised state machine. *)

  val advance : t -> message event -> t * message action list
  (** [advance t event] applies the event to the state machine and returns the updated state machine and any actions to take. If this fails it returns an error message *)
  (* If the event is new commands to add to the log, *)

  val available_space_for_commands : t -> int
  (* The number of entries which can be added *)

  val should_ack_clients : t -> bool
end

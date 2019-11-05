open Types
open State_machine

type t = Ballot.t * slot_number * StateMachine.command [@@deriving protobuf]

val equal : t -> t -> bool

val to_string : t -> string

val get_ballot : t -> Ballot.t

val get_slot : t -> slot_number

val get_cmd : t -> StateMachine.command

open Types
open State_machine

type client_request = {command: StateMachine.command [@key 1]}
[@@deriving protobuf]

type replica_request =
  {slot_num: Types.slot_number [@key 1]; command: StateMachine.command [@key 2]}
[@@deriving protobuf]

type p1a = {ballot: Ballot.t [@key 1] (*; high_slot : slotber [@key 2]*)}
[@@deriving protobuf]

type p1b =
  { ballot: Ballot.t [@key 1]
  ; accepted: Pval.t list [@key 2]
  ; id: string [@key 3] }
[@@deriving protobuf]

type p2a = {pval: Pval.t [@key 1]} [@@deriving protobuf]

type p2b =
  {id: unique_id [@key 1]; ballot: Ballot.t [@key 2]; pval: Pval.t [@key 3]}
[@@deriving protobuf]

type nack_p1 = {ballot: Ballot.t [@key 1]} [@@deriving protobuf]

type nack_p2 = {ballot: Ballot.t [@key 1]} [@@deriving protobuf]

type decision_response =
  {slot: Types.slot_number [@key 1]; command: StateMachine.command [@key 2]}
[@@deriving protobuf]

type client_response = {result: StateMachine.op_result [@key 1]}
[@@deriving protobuf]

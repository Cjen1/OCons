open Types

type client_request = {command: Types.command [@key 1]} [@@deriving protobuf]

type replica_request =
  {slot_num: Types.slot_number [@key 1]; command: Types.command [@key 2]}
[@@deriving protobuf]

type p1a = {ballot: Ballot.t [@key 1] (*; high_slot : slotber [@key 2]*)}
[@@deriving protobuf]

type p1b = {ballot: Ballot.t [@key 1]; accepted: Pval.t list [@key 2]}
[@@deriving protobuf]

type p2a = {pval: Pval.t [@key 1]} [@@deriving protobuf]

type p2b = {acceptor_id: unique_id [@key 1]; ballot: Ballot.t [@key 2]}
[@@deriving protobuf]

type nack = {ballot: Ballot.t [@key 1]} [@@deriving protobuf]

type decision_response =
  {slot_num: Types.slot_number [@key 1]; command: Types.command [@key 2]}
[@@deriving protobuf]

type client_response = {result: Types.result [@key 1]} [@@deriving protobuf]

open Types

type t = Ballot.t * slot_number * command [@@deriving protobuf]

val equal : t -> t -> bool

val to_string : t -> string

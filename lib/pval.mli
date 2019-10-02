open Types
open Yojson

type t = Ballot.t * slot_number * command

val equal : t -> t -> bool

val to_string : t -> string

val serialize : t -> Basic.t

val deserialize : Basic.t -> t

val serialize_list : t list -> Basic.t

val deserialize_list : Basic.t -> t list

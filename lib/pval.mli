open Types
open Yojson
    
type t = Ballot.t * slot_number * command
val equal : t -> t -> bool
  
val to_string : t -> string
val serialize : t -> Basic.json
val deserialize : Basic.json -> t
val serialize_list : t list -> Basic.json
val deserialize_list : Basic.json -> t list

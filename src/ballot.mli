(* ballot.ml *)

open Types
open Yojson
    
type t
  
val bottom : unit -> t
val init : leader_id -> t
val succ_exn : t -> t

val equal : t -> t -> bool
val less_than : t -> t -> bool
val compare : t -> t -> int

val serialize : t -> Basic.json
val deserialize : Basic.json -> t
val to_string : t -> string

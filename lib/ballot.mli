(* ballot.ml *)

open Types
open Yojson

(* Types of ballots *)
type t = Bottom | Number of int * leader_id

val bottom : unit -> t

val init : leader_id -> t

val succ_exn : t -> t

val equal : t -> t -> bool

val less_than : t -> t -> bool

val compare : t -> t -> int

val serialize : t -> Basic.t

val deserialize : Basic.t -> t

val to_string : t -> string

module Infix : sig
  val ( < ) : t -> t -> bool

  val ( = ) : t -> t -> bool
end

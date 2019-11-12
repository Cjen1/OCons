(* ballot.ml *)

open Types
open Base

(* Types of ballots *)
type t = Bottom [@key 1] | Number of int * leader_id [@key 2]
[@@deriving protobuf, sexp]

val bottom : unit -> t

val init : leader_id -> t

val succ_exn : t -> leader_id -> t

val phys_equal : t -> t -> bool

val equal : t -> t -> bool
val less_than : t -> t -> bool
val greater_than : t -> t -> bool

val compare : t -> t -> int

val get_leader_id_exn : t -> leader_id

(*
val serialize : t -> Basic.t

val deserialize : Basic.t -> t
   *)

val to_string : t -> string

module Infix : sig
  val ( < ) : t -> t -> bool
  val ( <= ) : t -> t -> bool

  val ( > ) : t -> t -> bool
  val ( >= ) : t -> t -> bool

  val ( = ) : t -> t -> bool
end

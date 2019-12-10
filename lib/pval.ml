open Types
open State_machine
open Base

(* Pvalues are triples consisting of a ballot number, a slot number and a command.

   Each pvalue represents a <slot_number,cmd> proposal that has been assigned
   a ballot number. The ballot number is necessary to ensure that a leader
   has secured a majority quorum on committing this command to this slot *)
type t = Ballot.t * int * StateMachine.command [@@deriving protobuf, sexp]

(* Function to check two pvals are equal *)
let equal (b, s, c) (b', s', c') =
  Ballot.equal b b' && s = s' && Poly.equal c c'

(* Function returns string representation of pval *)
let to_string pval = pval |> sexp_of_t |> Sexp.to_string_hum

let get_ballot (a, _, _) = a

let get_slot (_, s, _) = s

let get_cmd (_, _, c) = c

open Types

(* Pvalues are triples consisting of a ballot number, a slot number and a command.

   Each pvalue represents a <slot_number,cmd> proposal that has been assigned
   a ballot number. The ballot number is necessary to ensure that a leader
   has secured a majority quorum on committing this command to this slot *)
type t = Ballot.t * slot_number * command [@@deriving protobuf]

(* Function to check two pvals are equal *)
let equal pval pval' =
  let b, s, c = pval and b', s', c' = pval' in
  Ballot.equal b b' && s = s' && commands_equal c c'

(* Function returns string representation of pval *)
let to_string pval =
  let b, s, c = pval in
  "<" ^ Ballot.to_string b ^ ", " ^ string_of_int s ^ ", "
  ^ string_of_command c ^ ">"

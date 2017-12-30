(* Ballots are identified uniquely by their ballot number

   This consists of a pair of an integer and the id of the leader that
   initiated the ballot. There is also the Bottom variant of ballot.

   Ballots are totally ordered, based on the lexicographical ordering of the
   integer component and the leader id.

   The Bottom ballot denotes the lowest possible ballot number (used for
   initialisation in accceptors for example). *) 
type t = Bottom
       | Number of int * Types.leader_id

(* To ensure the total ordering on ballot, two functions are provided to test
   equality of ballots and the partial ordering of ballots.

   Since integers and ids are totally ordered the pairing here is totally
   ordered *)
let equal b1 b2 =
  match b1, b2 with
  | Bottom, Bottom -> true  
  | Number(n,l), Number(n',l') -> (n = n') && (Types.leader_ids_equal l l')
  | _, _ -> false

let less_than b1 b2 =
  match b1, b2 with
  | Bottom, Bottom -> false
  | Bottom, _ -> true
  | _, Bottom -> false
  | Number(n,lid), Number(n',lid') -> if n = n' then lid < lid' else n < n'

let to_string = function
  | Bottom -> "Bottom"
  | Number(n,lid) -> "Number(" ^ (string_of_int n) ^ "," ^ (Types.string_of_id lid) ^ ")"

(* We also include pvalues here, these are triples consisting of a ballot
   number, a slot number and a command.

   Each pvalue represents a <slot_number,cmd> proposal that has been assigned
   a ballot number. The ballot number is necessary to ensure that a leader
   has secured a majority quorum on committing this command to this slot *)
type pvalue = t * Types.slot_number * Types.command

let pvalue_to_string pval =
  let (b,s,c) = pval in
  "<" ^ (to_string b) ^ ", " ^ (string_of_int s) ^ 
  ", " ^ (Types.string_of_command c) ^ ">"

(* TODO: *)
(* ...A quorum is a collection of acceptors... *)
module Quorum = struct
  (* Let quorums be a list of the ids of the acceptors *)
  type t = Types.unique_id list
  
  (* Placeholder *)
  let is_majority (q : t) : bool = false
end

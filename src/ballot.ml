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

(* For reference, from Real World OCaml...
   
   compare x y < 0     (* x < y *)
   compare x y = 0     (* x = y *)
   compare x y > 0     (* x > y *)
*)
let compare b1 b2 =
  match b1, b2 with
  | Bottom, Bottom -> 0
  | Bottom, _ -> - 1
  | _, Bottom -> 1
  | Number(n,lid), Number(n',lid') -> 
    let int_comp = Core.Int.compare n n' in
    if int_comp = 0 then Core.Uuid.compare lid lid'
    else int_comp

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

let pvalue_equal p p' =
  let (b,s,c) = p and (b',s',c') = p' in
  (equal b b') && (s=s') && (Types.commands_equal c c')

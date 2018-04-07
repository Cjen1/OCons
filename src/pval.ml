open Types
open Core
open Yojson

(* Pvalues are triples consisting of a ballot number, a slot number and a command.

   Each pvalue represents a <slot_number,cmd> proposal that has been assigned
   a ballot number. The ballot number is necessary to ensure that a leader
   has secured a majority quorum on committing this command to this slot *)
type t = Ballot.t * slot_number * command

(* Function to check two pvals are equal *)
let equal pval pval' =
  let b, s, c = pval and b',s',c' = pval' in
  (Ballot.equal b b') && (s=s') && (commands_equal c c')

(* Function returns string representation of pval *)
let to_string pval =
  let b, s, c = pval in
    "<" ^ (Ballot.to_string b) ^ ", " ^ (string_of_int s) ^ 
    ", " ^ (string_of_command c) ^ ">"

(* Function takes a pval and serializes it into JSON *)
let serialize (pval : t) : Basic.json =
  let b, s, c = pval in
  let ballot_json = Basic.Util.to_assoc (Ballot.serialize b) in
  let command_json = Basic.Util.to_assoc (serialize_command c) in 
  let pvalue_json = `Assoc (List.concat [ballot_json; [("slot_number", `Int s)]; command_json])
  in `Assoc [ ("pvalue", pvalue_json) ]

(* Function takes a JSON type and deserializes it into a pval type. This may throw
   an exception if the JSON does not represent a pval as described *)
let deserialize (pvalue_json : Basic.json) : t =
  let inner_json = Basic.Util.member "pvalue" pvalue_json in
  let ballot_number_json = Basic.Util.member "ballot_num" inner_json in
  let slot_number = Basic.Util.member "slot_number" inner_json in
  let command_json = Basic.Util.member "command" inner_json in
  (Ballot.deserialize (`Assoc [("ballot_num",ballot_number_json)]),
   Basic.Util.to_int slot_number,
   deserialize_command command_json)

(* Serialize list of pvalues by mapping serialize over them *)
let serialize_list (pvals : t list) : Basic.json =
  `List (Core.List.map pvals ~f:serialize)

(* Deserialize JSON list of pvalues by mapping deserialize over elements in JSON list.
   This may throw an exception as we have to pattern match over the JSON `List type
*)
exception PvalListDeserializationError

let deserialize_list (pvals_json : Basic.json) : t list = 
  match pvals_json with
  | `List ls -> Core.List.map ls ~f:deserialize
  | _ -> raise PvalListDeserializationError





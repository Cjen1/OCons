(* ballot.ml *)

open Types
open Yojson

(* Ballots are identified uniquely by their ballot number

   This consists of a pair of an integer and the id of the leader that
   initiated the ballot. There is also the Bottom variant of ballot.

   Ballots are totally ordered, based on the lexicographical ordering of the
   integer component and the leader id.

   The Bottom ballot denotes the lowest possible ballot number (used for
   initialisation in accceptors for example).
*)

(* Types of ballots *)
type t = Bottom | Number of int * leader_id

(* Function bottom returns bottom ballot *)
let bottom () = Bottom

(* Generate an initial ballot number for a given leader id *)
let init id = Number (0, id)

(* Generates the successor of a given ballot. This can result in an exception if the ballot is
   Bottom, as the bottom ballot has no successor *)
let succ_exn = function
  | Bottom ->
      failwith "Error: Bottom ballot has no successor"
  | Number (n, l) ->
      Number (n + 1, l)

(* To ensure the total ordering on ballot, two functions are provided to test
   equality of ballots and the partial ordering of ballots.

   Since integers and ids are totally ordered the pairing here is totally
   ordered *)
let equal b b' =
  match (b, b') with
  | Bottom, Bottom ->
      true
  | Number (n, l), Number (n', l') ->
      n = n' && leader_ids_equal l l'
  | _, _ ->
      false

(* Comparison function for ballots.

  For reference on how comparisons should behave, from Real World OCaml...
    compare x y < 0    <=> x < y
    compare x y = 0    <=> x = y
    compare x y > 0    <=> x > y  *)
let compare b b' =
  match (b, b') with
  | Bottom, Bottom ->
      0
  | Bottom, _ ->
      -1
  | _, Bottom ->
      1
  | Number (n, lid), Number (n', lid') -> (
    match Core.Int.compare n n' with 0 -> Uuid.compare lid lid' | n -> n )

(* Function tests if ballot b is less than b'.
   Along with equalaity function we have a total order on ballots *)
let less_than b b' =
  compare b b' < 0

(* Convert a ballot to a string *)
let to_string = function
  | Bottom ->
      "Bottom"
  | Number (n, lid) ->
      "Number(" ^ string_of_int n ^ "," ^ string_of_id lid ^ ")"

(* Serialize a ballot into JSON *)
let serialize (b : t) : Basic.t =
  let ballot_json =
    match b with
    | Bottom ->
        `String "bottom"
    | Number (n, lid) ->
        `Assoc [("id", `Int n); ("leader_id", `String (string_of_id lid))]
  in
  `Assoc [("ballot_num", ballot_json)]

(* Deserialize a ballot, reutnring to the type of ballots.
   May throw an exception if the JSON does not represent a ballot *)

exception BallotDeserializationError

let deserialize (ballot_json : Basic.t) : t =
  match Basic.Util.member "ballot_num" ballot_json with
  | `String "bottom" ->
      Bottom
  | `Assoc [("id", `Int n); ("leader_id", `String lid)] ->
      Number (n, id_of_string lid)
  | _ ->
      raise BallotDeserializationError

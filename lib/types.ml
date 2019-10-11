(* types.ml *)

open Base

(* Types of unique identifiers *)
type unique_id = string [@@deriving protobuf]

(* Reproduce some of the Uuid manipulation functions we'll use.

   This is so if we change the type of unique ids in the future then we can update these functions
   without breaking the rest of the program *)
(*let unique_ids_equal = Uuid.equal
  *)

type leader_id = unique_id [@@deriving protobuf]

let create_id () : unique_id = Uuid_unix.create () |> Uuid.to_string

(* Unique identifier for a given command issued by a client *)
type command_id = int [@@deriving protobuf]

(* Type of operations that can be issued by a client:
    -   These are the initial simple operations for a kv store.
    -   Each operation is applied to a state giving a new state.
    -   Idea is that same sequence of operations, given same initial state
        produces the same final state.
    -   Represent transitions in replicated state machine.

  Note:
    -   Keys are unique in store.

    -   If create is applied and key already present, then state
        is unaffected (and failure is returned).

    -   If read is applied and key is not present, then state
        is unaffected (and failure is returned).

    -   If update is applied and key is not present, then state
        is unaffected (and failure is returned).

    -   If remove is applied and key is not present, then state
        is unaffected (and failure is returned).
*)

type key = string [@@deriving protobuf]

type value = string [@@deriving protobuf]

type operation =
  | Nop [@key 1]
  (* Idempotent no operation *)
  | Create of key * value [@key 2]
  (* Add (k,v) to the store *)
  | Read of key [@key 3]
  (* Read v for k from store *)
  | Update of key * value [@key 4]
  (* Update (k,_) to (k,v) in store *)
  | Remove of key [@key 5]
  | Reconfigure of string list (*leaders*) [@key 6]
[@@deriving protobuf]

(* Types of results that can be returned to a client

   Note:
    -   These represent application-specific results. A client receiving any of
        these results means that the replicated state machines decided upon
        these commands in the sequence, it is just that the command itself
        may have failed in an application context.
*)
type result =
  | Success [@key 1] (* Indicates operation was successfully performed by replicas *)
  | Failure [@key 2] (* Indicates operation was not successfully performed by replicas *)
  | ReadSuccess of value [@key 3]
[@@deriving protobuf]

(* Indicates read success with associated value *)

(* State of application is a dictionary of strings keyed by integers *)
type app_state = (string, string) Base.Hashtbl.t

(* Initial state of application is the empty dictionary *)
let initial_state : unit -> (key, value) Hashtbl.t =
 fun () -> Hashtbl.create (module String)

(* Apply takes a state and an operation and performs that operation on that
   state, returning the new state and a result *)
let apply (state : app_state) (op : operation) : result =
  match op with
  | Nop ->
      Success
  | Create (k, v) | Update (k, v) ->
      Base.Hashtbl.set state ~key:k ~data:v ;
      Success
  | Read k -> (
    match Base.Hashtbl.find state k with
    | None ->
        Failure
    | Some v ->
        ReadSuccess v )
  | Reconfigure _ ->
      Success
  | Remove k -> (
    match Base.Hashtbl.find_and_remove state k with
    | Some _ ->
        Success
    | None ->
        Failure )

type command = unique_id * operation [@@deriving protobuf]

(* Slots identify the ordering in which a replica wishes to assign commands *)
type slot_number = int [@@deriving protobuf]

(* Proposals are pairs of slot numbers and the command commited to that slot *)
type proposal = slot_number * command [@@deriving protobuf]

(* Equality functions for commands and proposals.
   These are necessary because Core.phys_equal does not seem to compare
   UUIDs correctly for equality. Hence we just do an equality check
   over the structure of commands and proposals *)
let commands_equal (c : command) (c' : command) : bool =
  let command_id, oper = c in
  let command_id', oper' = c' in
  String.equal command_id command_id' && Stdlib.compare oper oper' == 0

let proposals_equal (p : proposal) (p' : proposal) : bool =
  let slot_out, c = p in
  let slot_out', c' = p' in
  commands_equal c c' && slot_out = slot_out'

(* Pretty-printable string for a given operation *)
let string_of_operation oper =
  let string_of_kv (k, v) = Printf.sprintf "(%s,%s)" k v in
  match oper with
  | Nop ->
      "Nop"
  | Create (k, v) ->
      "Create " ^ string_of_kv (k, v)
  | Read k ->
      "Read (" ^ k ^ ")"
  | Update (k, v) ->
      "Update " ^ string_of_kv (k, v)
  | Remove k ->
      "Remove (" ^ k ^ ")"
  | Reconfigure _ ->
      "Reconfigure"

(* Pretty-printable string for a given command *)
let string_of_command (cmd : command) =
  let command_id, operation = cmd in
  "<" ^ command_id ^ "," ^ string_of_operation operation ^ ">"

(* Pretty-printable string for a given proposal *)
let string_of_proposal p =
  let slot_no, cmd = p in
  "<" ^ Int.to_string slot_no ^ ", " ^ string_of_command cmd ^ ">"

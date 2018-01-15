(* types.ml *)

(* Types of unique identifiers *)
type unique_id = Core.Uuid.t;;

let unique_ids_equal = Core.Uuid.equal
let string_of_id = Core.Uuid.to_string
let id_of_string = Core.Uuid.of_string
let create_id = Core.Uuid.create

(* Unique identifier for a given client node
    
   This is different to other nodes because replicas need to reply to a client based upon 
   the id of the client that issued the command, so the ID needs include the address to 
   which the response is sent *)
type client_id = unique_id * Uri.t

(* Unique identifier for a given replica node *)
type replica_id = unique_id

(* Unique identifier for a given leader node *)
type leader_id = unique_id

(* Equality of leader ids *)
let leader_ids_equal = unique_ids_equal

(* Unique identifier for a given command issued by a client *)
type command_id = int;;

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
type operation = Nop                     (* Idempotent no operation *)
               | Create of int * string  (* Add (k,v) to the store *)
               | Read   of int           (* Read v for k from store *)
               | Update of int * string  (* Update (k,_) to (k,v) in store *)
               | Remove of int;;         (* Remove (k,_) from store *)

(* Types of results that can be returned to a client

   Note:
    -   These represent application-specific results. A client receiving any of
        these results means that the replicated state machines decided upon
        these commands in the sequence, it is just that the command itself
        may have failed in an application context.
*)
type result = Success (* Indicates operation was successfully performed by replicas *)
            | Failure (* Indicates operation was not successfully performed by replicas *)
            | ReadSuccess of string;; (* Indicates read success with associated value *)

(* State of application is a dictionary of strings keyed by integers *)
type app_state = (int, string) Core.List.Assoc.t;;

(* Initial state of application is the empty dictionary *)
let initial_state = [];;

(* Apply takes a state and an operation and performs that operation on that
   state, returning the new state and a result *)
let apply (state : app_state) (op : operation) : (app_state * result)=
  match op with
  | Nop -> 
    (state, Success)
  | Create (k,v) -> 
    if Core.List.Assoc.mem state ~equal:(=) k then
      (state, Failure)
    else
      ((k,v) :: state, Success)
  | Read k ->
    (match Core.List.Assoc.find state ~equal:(=) k with
    | None -> (state, Failure)
    | Some v -> (state, ReadSuccess v))
  | Update (k,v) ->
    if Core.List.Assoc.mem state ~equal:(=) k then
      let state' = Core.List.Assoc.remove state ~equal:(=) k in
      ((k,v) :: state' , Success)
    else
      (state, Failure)
  | Remove k ->
    if Core.List.Assoc.mem state ~equal:(=) k then
      (Core.List.Assoc.remove state ~equal:(=) k, Success)
    else
      (state, Failure);;

(* Pretty-printable string of application state *)
let string_of_state (state : app_state) : string =
  "[" ^ (String.concat "," (Core.List.map state ~f:(fun (k,v) ->
      "(" ^ (string_of_int k) ^ ", " ^ v ^ ")"
     ))) ^ "]";;

(* A command consists of a triple containing:
    -   The id of the client that requested the command be performed
    -   The id of the command itself
    -   The operation which the command will apply to the state
*)
type command = client_id * command_id * operation;;

(* Slots identify the ordering in which a replica wishes to assign commands *)
type slot_number = int;;

(* Proposals are pairs of slot numbers and the command commited to that slot *)
type proposal = slot_number * command;;

(* Equality functions for commands and proposals.
   These are necessary because Core.phys_equal does not seem to compare
   UUIDs correctly for equality. Hence we just do an equality check
   over the structure of commands and proposals *)
let commands_equal (c : command) (c' : command) : bool =
  let ((id,uri), command_id, oper) = c in
  let ((id',uri'), command_id', oper') = c' in
    (Core.Uuid.equal id id') &&
    (Uri.equal uri uri') &&
    (command_id = command_id') &&
    (oper = oper');;

let proposals_equal (p : proposal) (p' : proposal) : bool =
  let (slot_out, c) = p in
  let (slot_out', c') = p' in
    (commands_equal c c') &&
    (slot_out = slot_out');;

(* Pretty-printable string for a given operation *)
let string_of_operation oper =
  let string_of_kv (k,v) = "(" ^ (string_of_int k) ^ "," ^ v ^ ")" in
  match oper with
  | Nop -> "Nop"
  | Create (k,v) -> "Create " ^ string_of_kv (k,v)
  | Read k -> "Read (" ^ (string_of_int k) ^ ")"
  | Update (k,v) -> "Update " ^ string_of_kv (k,v)
  | Remove k -> "Remove (" ^ (string_of_int k) ^ ")";;

(* Pretty-printable string for a given command *)
let string_of_command (cmd : command) = 
  let (client_id, command_id, operation) = cmd in
  let (id,uri) = client_id in
  "<(" ^ (Core.Uuid.to_string id) 
  ^ "," ^ (Uri.to_string uri) ^ "),"
  ^ (string_of_int command_id) ^ "," 
  ^ (string_of_operation operation) ^ ">";;

(* Pretty-printable string for a given proposal *)
let string_of_proposal p =
  let (slot_no, cmd) = p in
  "<" ^ (string_of_int slot_no) ^ ", " ^ (string_of_command cmd) ^ ">";;

(* Pretty-printable string for a given result *)
let string_of_result result =
  match result with
  | Failure -> "Failure"
  | Success -> "Success"
  | ReadSuccess v -> "Read success, value " ^ v;;

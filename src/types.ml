(* types.ml *)

(* Types of unique identifiers *)
type unique_id = Core.Uuid.t;;

(* Unique identifier for a given client node *)
type client_id = unique_id;;

(* Unique identifier for a given replica node *)
type replica_id = unique_id;;

(* Unique identifier for a given leader node *)
type leader_id = unique_id;;

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
    -   If create is applied and key already present, then  ...
    -   If read is applied and key is not present, then ...
    -   If update is applied and key is not present, then ...
    -   If remove is applied and key is not present, then ...
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
type result = Success          (* Indicates operation was successfully performed by replicas *)
            | Failure          (* Indicates operation was not successfully performed by replicas *)
            | ReadSuccess of string;; (* Indicates read success with associated value *)

(* State of application is a dictionary *)
type app_state = (int, string) Core.List.Assoc.t;;

(* Initial state of application is the empty dictionary *)
let initial_state = [];;

(* Apply takes a state and an operation and performs that 
   operation on that state, returning a result *)
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

(* Pretty-printable of app state *)
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
  let (client_id, command_id, oper) = c in
  let (client_id', command_id', oper') = c' in
    (Core.Uuid.equal client_id client_id') &&
    (command_id = command_id') &&
    (oper = oper');;

let proposals_equal (p : proposal) (p' : proposal) : bool =
  let (slot_out, c) = p' in
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
  let (client_id, command_id, operation) = cmd in "<" 
  ^ (Core.Uuid.to_string client_id) ^ ","
  ^ (string_of_int command_id) ^ "," 
  ^ (string_of_operation operation) ^ ">";;

(* Pretty-printable string for a given proposal *)
let string_of_proposal p =
  let (slot_no, cmd) = p in
  "<" ^ (string_of_int slot_no) ^ ", " ^ (string_of_command cmd) ^ ">";;

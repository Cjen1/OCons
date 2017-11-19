(* types.ml *)

(* Unique identifier for a given client node *)
type client_id = int;;

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
            | Read of string;; (* Indicates read success with associated value *)

(* A command consists of a triple containing:
    -   The id of the client that requested the command be performed
    -   The id of the command itself
    -   The operation which the command will apply to the state
*)
type command = client_id * command_id * operation;;

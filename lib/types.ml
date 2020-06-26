(* types.ml *)

open Base

type time = float

let time_now : unit -> time = fun () -> Unix.gettimeofday ()

let create_id () =
  Random.self_init () ;
  Random.int64 Int64.max_value

type node_id = int64 [@@deriving protobuf]

type node_addr = string

type client_id = node_id

type command_id = int64 [@@deriving protobuf]

module StateMachine : sig
  type t

  type key = string [@@deriving protobuf]

  type value = string [@@deriving protobuf]

  type op = Read of key [@key 1] | Write of key * value [@key 2]
  [@@deriving protobuf]

  type command = {op: op; id: command_id} [@@deriving protobuf]

  type op_result =
    | Success [@key 1]
    | Failure [@key 2]
    | ReadSuccess of key [@key 3]
  [@@deriving protobuf]

  val op_result_failure : unit -> op_result

  val update : t -> command -> op_result

  val create : unit -> t

  val command_equal : command -> command -> bool
end = struct
  type key = string [@@deriving protobuf]

  type value = string [@@deriving protobuf]

  type t = (key, value) Hashtbl.t

  type op = Read of key [@key 1] | Write of key * value [@key 2]
  [@@deriving protobuf]

  type command = {op: op [@key 1]; id: command_id [@key 2]}
  [@@deriving protobuf]

  type op_result =
    | Success [@key 1]
    | Failure [@key 2]
    | ReadSuccess of key [@key 3]
  [@@deriving protobuf]

  let op_result_failure () = Failure

  let update : t -> command -> op_result =
   fun t -> function
    | {op= Read key; _} -> (
      match Hashtbl.find t key with Some v -> ReadSuccess v | None -> Failure )
    | {op= Write (key, value); _} ->
        Hashtbl.set t ~key ~data:value ;
        Success

  let create () = Hashtbl.create (module String)

  let command_equal a b =
    Int64.(a.id = b.id)
    &&
    match (a.op, b.op) with
    | Read k, Read k' ->
        String.(k = k')
    | Write (k, v), Write (k', v') ->
        String.(k = k' && v = v')
    | Read _, Write _ | Write _, Read _ ->
        false
end

type command = StateMachine.command

type op_result = StateMachine.op_result

type term = int64 [@@deriving protobuf]

type log_index = int64 [@@deriving protobuf]

let log_index_mod : int Base__.Hashtbl_intf.Key.t = (module Int)

type log_entry = {
  command_id: command_id [@key 1];
  term: term [@key 2]
}
[@@deriving protobuf]

let pp_entry f entry = 
  Fmt.pf f "(id:%a term:%a)" Fmt.int64 entry.command_id Fmt.int64 entry.term

let string_of_entry entry =
  Fmt.str "%a" pp_entry entry

let string_of_entries entries =
  Fmt.str "(%a)" (Fmt.list pp_entry) entries

type partial_log = log_entry list

type persistent = Log_entry of log_entry

(* Messaging types *)
type request_vote_response =
  {term: term; voteGranted: bool; entries: log_entry list}

type append_entries_response = {term: term; success: bool}

open Core
module A = Accessor
open! A.O

type time = float

type node_addr = string

type command_id = int [@@deriving bin_io, compare, sexp]

type client_id = int [@@deriving bin_io]

type node_id = int [@@deriving sexp_of, bin_io]

type key = string [@@deriving bin_io, compare, sexp]

type value = string [@@deriving bin_io, compare, sexp]

type state_machine = (key, value) Hashtbl.t

type connection_creater = node_id * Ocons_conn_mgr.resolver

type sm_op =
  | Read of key
  | Write of key * value
  | CAS of {key: key; value: value; value': value}
  | NoOp
[@@deriving bin_io, compare, sexp]

let sm_op_pp ppf v =
  match v with
  | Read k ->
      Fmt.pf ppf "Read %s" k
  | Write (k, v) ->
      Fmt.pf ppf "Write (%s, %s)" k v
  | CAS s ->
      Fmt.pf ppf "CAS(%s, %s, %s)" s.key s.value s.value'
  | NoOp ->
      Fmt.pf ppf "NoOp"

module Command = struct
  type t = {op: sm_op; id: command_id; mutable trace_start: float}
  [@@deriving compare]

  let pp_mach ppf v =
    Fmt.pf ppf "Command(%a, %d, %.4f)" sm_op_pp v.op v.id v.trace_start

  let pp ppf v = Fmt.pf ppf "Command(%a, %d)" sm_op_pp v.op v.id

  let equal a b = a.id = b.id
end

type command = Command.t [@@deriving compare]

let update_command_time c = c.Command.trace_start <- Core_unix.gettimeofday ()

let get_command_trace_time c = c.Command.trace_start

let empty_command = Command.{op= NoOp; id= -1; trace_start= -1.}

let make_command_state = ref 0

let reset_make_command_state () = make_command_state := 0

let make_command c =
  make_command_state := !make_command_state + 1 ;
  Command.{op= c; id= !make_command_state; trace_start= -1.}

type op_result = Success | Failure of string | ReadSuccess of key
[@@deriving bin_io]

let op_result_pp ppf v =
  match v with
  | Success ->
      Fmt.pf ppf "Success"
  | Failure s ->
      Fmt.pf ppf "Failure(%s)" s
  | ReadSuccess s ->
      Fmt.pf ppf "ReadSuccess(%s)" s

let op_result_failure s = Failure s

let op_not_found_failure = Failure "Key not found"

let op_cas_match_fail k v v' =
  Failure (Fmt.str "Key (%s) has value (%s) rather than (%s)" k v v')

let update_state_machine : state_machine -> command -> op_result =
 fun t cmd ->
  match cmd.op with
  | NoOp ->
      Success
  | Read key -> (
    match Hashtbl.find t key with
    | Some v ->
        ReadSuccess v
    | None ->
        op_not_found_failure )
  | Write (key, value) ->
      Hashtbl.set t ~key ~data:value ;
      Success
  | CAS {key; value; value'} -> (
    match Hashtbl.find t key with
    | Some v when String.(v = value) ->
        Hashtbl.set t ~key ~data:value' ;
        Success
    | Some v ->
        op_cas_match_fail key v value
    | None ->
        op_not_found_failure )

let create_state_machine () = Hashtbl.create (module String)

type log_index = int [@@deriving bin_io, compare]

type term = int [@@deriving compare, compare, bin_io]

type log_entry = {command: command; term: term} [@@deriving accessors, compare]

let log_entry_pp ppf v =
  Fmt.pf ppf "{command: %a; term : %d}" Command.pp v.command v.term

let log_entry_pp_mach ppf v =
  Fmt.pf ppf "{command: %a; term : %d}" Command.pp_mach v.command v.term

type client_request = command

type client_response = (op_result, [`Unapplied]) Result.t [@@deriving bin_io]

open Core
module A = Accessor
open! A.O

type time = float

type node_addr = string

type command_id = Uuid.Stable.V1.t [@@deriving bin_io, compare, sexp]

type client_id = Uuid.t [@@deriving bin_io]

type node_id = int [@@deriving sexp_of, bin_io]

type key = string [@@deriving bin_io, compare, sexp]

type value = string [@@deriving bin_io, compare, sexp]

type state_machine = (key, value) Hashtbl.t

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
  type t = {op: sm_op; id: command_id} [@@deriving bin_io, compare, sexp]

  let pp ppf v =
    Fmt.pf ppf "Command(%a, %s)" sm_op_pp v.op (Uuid.to_string v.id)
end

type command = Command.t [@@deriving bin_io, compare, sexp]

let empty_command = Command.{op= NoOp; id= Uuid.create_random (Base.Random.State.make [||])}

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
  | NoOp -> Success
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

type log_entry = {command: command; term: term} [@@deriving bin_io, compare]

type client_request = command [@@deriving bin_io]

type client_response = (op_result, [`Unapplied]) Result.t [@@deriving bin_io]

type 'a iter = ('a -> unit) -> unit

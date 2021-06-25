open! Core
open! Async
module A = Accessor
open! A.O

type time = float

type node_addr = string

type command_id = Uuid.Stable.V1.t [@@deriving bin_io, sexp, compare]

type client_id = Uuid.Stable.V1.t [@@deriving bin_io, sexp]

type node_id = int [@@deriving bin_io, sexp]

type key = string [@@deriving bin_io, sexp, compare]

type value = string [@@deriving bin_io, sexp, compare]

type state_machine = (key, value) Hashtbl.t

type sm_op =
  | Read of key
  | Write of key * value
  | CAS of {key: key; value: value; value': value}
[@@deriving bin_io, sexp, compare]

module Command = struct
  type t = {op: sm_op; id: command_id} [@@deriving bin_io, sexp, compare]

  let compare a b = Uuid.compare a.id b.id

  let hash t = Uuid.hash t.id
end

type command = Command.t [@@deriving bin_io, sexp, compare]

type op_result = Success | Failure of string | ReadSuccess of key
[@@deriving bin_io, sexp]

let op_result_failure s = Failure s

let op_not_found_failure = Failure "Key not found"

let op_cas_match_fail k v v' =
  Failure (Fmt.str "Key (%s) has value (%s) rather than (%s)" k v v')

let update_state_machine : state_machine -> command -> op_result =
 fun t -> function
  | {op= Read key; _} -> (
    match Hashtbl.find t key with
    | Some v ->
        ReadSuccess v
    | None ->
        op_not_found_failure )
  | {op= Write (key, value); _} ->
      Hashtbl.set t ~key ~data:value ;
      Success
  | {op= CAS {key; value; value'}; _} -> (
    match Hashtbl.find t key with
    | Some v when String.(v = value) ->
        Hashtbl.set t ~key ~data:value' ;
        Success
    | Some v ->
        op_cas_match_fail key v value
    | None ->
        op_not_found_failure )

let create_state_machine () = Hashtbl.create (module String)

type log_index = int64 [@@deriving bin_io, sexp, compare]

type term = int [@@deriving compare, compare, bin_io, sexp]

type log_entry = {command: command; term: term}
[@@deriving bin_io, sexp, compare]

type client_request = command [@@deriving bin_io, sexp]

type client_response = (op_result, [`Unapplied]) Result.t
[@@deriving bin_io, sexp]

let client_rpc =
  Async.Rpc.Rpc.create ~name:"client_request" ~version:0
    ~bin_query:bin_client_request ~bin_response:bin_client_response

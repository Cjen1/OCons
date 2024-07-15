open Core
module A = Accessor
open! A.O

type time = float

type node_addr = string

type command_id = int [@@deriving bin_io, compare, sexp, hash]

type client_id = int [@@deriving bin_io]

type node_id = int [@@deriving sexp_of, bin_io, compare, equal, show]

type key = string [@@deriving bin_io, compare, sexp]

type value = string [@@deriving bin_io, compare, sexp]

type state_machine = (key, value) Hashtbl.t

type connection_creater = node_id * Ocons_conn_mgr.resolver

type sm_op = Read of key | Write of key * value
[@@deriving bin_io, compare, sexp]

let sm_op_pp ppf v =
  match v with
  | Read k ->
      Fmt.pf ppf "Read %s" k
  | Write (k, v) ->
      Fmt.pf ppf "Write (%s, %s)" k v

let pp_list pp = Fmt.(parens @@ list ~sep:comma pp)

let pp_array pp = Fmt.(parens @@ array ~sep:comma pp)

module Command = struct
  module T = struct
    type t =
      { op: sm_op array
      ; id: command_id
      ; submitted: float
      ; mutable trace_start: float }
    [@@deriving sexp, bin_io]

    let hash t = hash_command_id t.id

    let hash_fold_t s t = hash_fold_command_id s t.id

    let pp_mach ppf v =
      Fmt.pf ppf "Command(%a, %d, %.4f, %.4f)" (pp_array sm_op_pp) v.op v.id
        v.submitted v.trace_start

    let pp ppf v = Fmt.pf ppf "Command(%a, %d)" (pp_array sm_op_pp) v.op v.id

    let compare a b = Int.compare a.id b.id

    let equal a b = a.id = b.id
  end

  include T
  include Comparable.Make (T)
end

type command = Command.t [@@deriving hash, sexp, compare, equal, bin_io]

let update_command_time c = c.Command.trace_start <- Core_unix.gettimeofday ()

let get_command_trace_time c = c.Command.trace_start

let empty_command = Command.{op= [||]; id= -1; submitted= -1.; trace_start= -1.}

let make_command_state = ref 0

let reset_make_command_state () = make_command_state := 0

(* Used for tests *)
let make_command c =
  make_command_state := !make_command_state + 1 ;
  Command.{op= c; id= !make_command_state; trace_start= -1.; submitted= -1.}

type op_result = Success of (key * value option) list | Failure of string
[@@deriving bin_io]

let op_result_pp ppf v =
  match v with
  | Success results ->
      Fmt.pf ppf "Success%a"
        Fmt.(pp_list @@ parens (pair string (option string)))
        results
  | Failure s ->
      Fmt.pf ppf "Failure(%s)" s

let op_result_failure s = Failure s

let op_not_found_failure = Failure "Key not found"

let op_cas_match_fail k v v' =
  Failure (Fmt.str "Key (%s) has value (%s) rather than (%s)" k v v')

let apply_sm_op : state_machine -> sm_op -> (key * value option) option =
 fun t op ->
  match op with
  | Read key ->
      Some (key, Hashtbl.find t key)
  | Write (key, value) ->
      Hashtbl.set t ~key ~data:value ;
      None

let update_state_machine : state_machine -> command -> op_result =
 fun t cmd ->
  Success
    (Iter.of_array cmd.op |> Iter.filter_map (apply_sm_op t) |> Iter.to_list)

let create_state_machine () = Hashtbl.create (module String)

type log_index = int [@@deriving compare, equal, bin_io, hash, sexp, show]

type term = int [@@deriving compare, equal, bin_io, hash, show]

type log_entry = {command: command; term: term} [@@deriving accessors, compare]

let log_entry_pp ppf v =
  Fmt.pf ppf "{command: %a; term : %d}" Command.pp v.command v.term

let log_entry_pp_mach ppf v =
  Fmt.pf ppf "{command: %a; term : %d}" Command.pp_mach v.command v.term

type client_request = command

type client_response = (op_result, [`Unapplied]) Result.t [@@deriving bin_io]

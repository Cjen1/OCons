open! Core
open! Async
module A = Accessor_async
open! A.O

type time = float

type node_addr = string

module Id = Unique_id.Int63 ()

type command_id = Id.t [@@deriving bin_io, sexp, compare]

type client_id = Id.t [@@deriving bin_io, sexp]

type node_id = int [@@deriving bin_io, sexp]

type key = string [@@deriving bin_io, sexp, compare]

type value = string [@@deriving bin_io, sexp, compare]

type state_machine = (key, value) Hashtbl.t

type sm_op = Read of key | Write of key * value
[@@deriving bin_io, sexp, compare]

module Command = struct
  type t = {op: sm_op; id: command_id} [@@deriving bin_io, sexp, compare]

  let compare a b = Id.compare a.id b.id

  let hash t = Id.hash t.id
end

type command = Command.t [@@deriving bin_io, sexp, compare]

type op_result = Success | Failure | ReadSuccess of key
[@@deriving bin_io, sexp]

let op_result_failure () = Failure

let update_state_machine : state_machine -> command -> op_result =
 fun t -> function
  | {op= Read key; _} -> (
    match Hashtbl.find t key with Some v -> ReadSuccess v | None -> Failure )
  | {op= Write (key, value); _} ->
      Hashtbl.set t ~key ~data:value ;
      Success

let create_state_machine () = Hashtbl.create (module String)

type log_index = int64 [@@deriving bin_io, sexp, compare]

type term = int [@@deriving compare, compare, bin_io, sexp]

type log_entry = {command: command; term: term}
[@@deriving bin_io, sexp, compare]

type client_request = command [@@deriving bin_io, sexp]

type client_response = (op_result, [`Unapplied]) Result.t
[@@deriving bin_io, sexp]

let client_rpc = Async.Rpc.Rpc.create ~name:"client_request" ~version:0
    ~bin_query:bin_client_request ~bin_response:bin_client_response

(*
module RPCs = struct
  let request_vote =
    Async.Rpc.One_way.create ~name:"request_vote" ~version:0
      ~bin_msg:bin_request_vote

  let request_vote_response =
    Async.Rpc.One_way.create ~name:"request_vote_response" ~version:0
      ~bin_msg:bin_request_vote_response

  let append_entries =
    Async.Rpc.One_way.create ~name:"append_entries" ~version:0
      ~bin_msg:bin_append_entries

  let append_entries_response =
    Async.Rpc.One_way.create ~name:"append_entries_response" ~version:0
      ~bin_msg:bin_append_entries_response

end
   *)

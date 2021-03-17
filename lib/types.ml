open! Core
open! Async
open! Ppx_log_async
module A = Accessor_async
open! A.O

let logger =
  let open Async_unix.Log in
  create ~level:`Info ~output:[] ~on_error:`Raise
    ~transform:(fun m -> Message.add_tags m [("src", "Types")])
    ()

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

(*---- Immutable API -------------------------------*)

module ILog = struct
  module IdSet = Set.Make (Id)

  module I = struct
    (* store is in newest to oldest order*)
    type t = {store: log_entry list; command_set: IdSet.t; length: int64}
    [@@deriving sexp, accessors, compare]

    let nth_of_index t i = Int64.(t.length - i)

    let drop_of_index t i = Int64.(nth_of_index t i + one)

    type op = Add of log_entry | RemoveGEQ of log_index
    [@@deriving bin_io, sexp, compare]

    let init () = {store= []; command_set= IdSet.empty; length= Int64.zero}

    let add t entry =
      let command_set = Set.add t.command_set entry.command.id in
      {store= entry :: t.store; command_set; length= Int64.(t.length + of_int 1)}

    let remove_geq t i =
      let drop = drop_of_index t i |> Int64.(max zero) in
      let removed, store = List.split_n t.store (Int64.to_int_exn drop) in
      let command_set =
        List.fold_left removed ~init:t.command_set ~f:(fun cset entry ->
            Set.remove cset entry.command.id )
      in
      let length = Int64.(max zero (t.length - drop)) in
      {store; command_set; length}

    let apply t op =
      match op with Add entry -> add t entry | RemoveGEQ i -> remove_geq t i
  end

  type t = I.t [@@deriving sexp, compare]

  type op = I.op [@@deriving bin_io, sexp, compare]

  let init = I.init

  let apply = I.apply

  let get_index (t : t) index =
    let nth = I.nth_of_index t index |> Int64.to_int_exn in
    List.nth t.store nth
    |> Result.of_option
         ~error:(Not_found_s (Sexp.Atom (Fmt.str "%a" Fmt.int64 index)))

  let get_index_exn t i = get_index t i |> Result.ok_exn

  let get_term t index =
    match index with
    | i when Int64.(i = zero) ->
        Ok 0
    | _ ->
        let open Result.Monad_infix in
        get_index t index >>= fun entry -> Ok entry.term

  let get_term_exn t i = get_term t i |> Result.ok_exn

  let apply_wrap t op = (I.apply t op, op)

  let add_entry t e = apply_wrap t (I.Add e)

  let remove_geq t index = apply_wrap t (I.RemoveGEQ index)

  let get_max_index t = t.I.length

  let mem_id t id = Set.mem t.I.command_set id

  let entries_after_inc t index =
    let drop = I.drop_of_index t index |> Int64.to_int_exn in
    List.split_n t.store drop |> fst

  let entries_after_inc_size t index =
    let size = Int64.(get_max_index t - index + one) in
    (entries_after_inc t index, size)

  let to_string t =
    let entries = entries_after_inc t Int64.zero in
    [%sexp_of: log_entry list] entries |> Sexp.to_string_hum

  let add_entries_remove_conflicts t ~start_index new_entries =
    let relevant_entries = entries_after_inc t start_index in
    (* Takes two lists of entries lowest index first
         iterates through the lists until there is a conflict
         at which point it returns the conflict index and the entries to add
    *)
    let rec merge_y_into_x idx :
        log_entry list * log_entry list -> int64 option * log_entry list =
      function
      | _, [] ->
          (None, [])
      | [], ys ->
          (None, ys)
      | x :: _, (y :: _ as ys) when not @@ [%compare.equal: term] x.term y.term
        ->
          [%log.debug
            logger "Mismatch while merging" (x : log_entry) (y : log_entry)] ;
          Logs.debug (fun m -> m "Mismatch at %a" Fmt.int64 idx) ;
          (Some idx, ys)
      | _ :: xs, _ :: ys ->
          merge_y_into_x Int64.(succ idx) (xs, ys)
    in
    (* entries_to_add is in oldest first order *)
    let removeGEQ_o, entries_to_add =
      merge_y_into_x start_index
        (List.rev relevant_entries, List.rev new_entries)
    in
    let t, ops =
      match removeGEQ_o with
      | Some i ->
          let t', op' = apply_wrap t (RemoveGEQ i) in
          (t', [op'])
      | None ->
          (t, [])
    in
    let t, ops =
      List.fold_left entries_to_add ~init:(t, ops) ~f:(fun (t, ops) v ->
          let t', ops' = apply_wrap t (Add v) in
          (t', ops' :: ops) )
    in
    (t, ops)

  let add_cmd t command term = add_entry t {command; term}

  let add_cmds t cmds term =
    List.fold cmds ~init:(t, []) ~f:(fun (t, ops) command ->
        let t, op = add_cmd t command term in
        let ops = op :: ops in
        (t, ops) )
end

module ITerm = struct
  type t = term [@@deriving bin_io, sexp, compare]

  let init () = 0

  type op = t [@@deriving bin_io, sexp, compare]

  let apply _t op = op
end

module type IStorage = sig
  type data [@@deriving sexp_of, compare]

  type op [@@deriving sexp, bin_io]

  type t [@@deriving sexp_of, compare]

  val init : unit -> t

  val apply : t -> op -> t

  val get_current_term : t -> term

  val get_data : t -> data

  (* newest to oldest *)
  val get_ops : t -> op list

  val reset_ops : t -> t

  val update_term : t -> term:term -> t

  val add_entry : t -> entry:log_entry -> t

  val remove_geq : t -> index:log_index -> t

  val get_index : t -> log_index -> (log_entry, exn) result

  val get_index_exn : t -> log_index -> log_entry

  val get_term : t -> log_index -> (term, exn) result

  val get_term_exn : t -> log_index -> term

  val get_max_index : t -> log_index

  val mem_id : t -> command_id -> bool

  val entries_after_inc : t -> log_index -> log_entry list

  val entries_after_inc_size : t -> log_index -> log_entry list * int64

  val to_string : t -> string

  val add_entries_remove_conflicts :
    t -> start_index:log_index -> entries:log_entry list -> t

  val add_cmd : t -> cmd:command -> term:term -> t

  val add_cmds : t -> cmds:command list -> term:term -> t
end

module IStorage : IStorage = struct
  module T = ITerm
  module L = ILog

  (*---- Immutable API -------------------------------*)

  type op = Term of T.op | Log of L.op
  [@@deriving bin_io, accessors, sexp, compare]

  type data = {current_term: T.t; log: L.t}
  [@@deriving sexp_of, accessors, compare]

  (* Newest at head of op list *)
  type t = {data: data; ops: op list} [@@deriving sexp_of, accessors, compare]

  let init () = {data= {current_term= T.init (); log= L.init ()}; ops= []}

  let apply t op =
    match op with
    | Term op ->
        A.set (data @> current_term) t ~to_:(T.apply t.data.current_term op)
    | Log op ->
        A.set (data @> log) t ~to_:(L.apply t.data.log op)

  (* Actual API *)

  let get_current_term t = t.data.current_term

  let get_data t = t.data

  let get_ops t = t.ops

  let reset_ops t = {t with ops= []}

  let update_term t ~term =
    A.set (data @> current_term) t ~to_:term
    |> A.set ops ~to_:(Term term :: t.ops)

  let add_entry t ~entry =
    let l, op = L.add_entry t.data.log entry in
    A.set (data @> log) t ~to_:l |> A.set ops ~to_:(Log op :: t.ops)

  let remove_geq t ~index =
    let l, op = L.remove_geq t.data.log index in
    A.set (data @> log) t ~to_:l |> A.set ops ~to_:(Log op :: t.ops)

  let get_index t index = L.get_index t.data.log index

  let get_index_exn t index = L.get_index_exn t.data.log index

  let get_term t index = L.get_term t.data.log index

  let get_term_exn t index = L.get_term_exn t.data.log index

  let get_max_index t = L.get_max_index t.data.log

  let mem_id t id = L.mem_id t.data.log id

  let entries_after_inc t index = L.entries_after_inc t.data.log index

  let entries_after_inc_size t index = L.entries_after_inc_size t.data.log index

  let to_string t = [%message (t : t)] |> Sexp.to_string_hum

  let add_entries_remove_conflicts t ~start_index ~entries =
    let l, ops' =
      L.add_entries_remove_conflicts t.data.log ~start_index entries
    in
    A.set (data @> log) t ~to_:l
    |> A.set ops ~to_:(List.map ~f:(fun op -> Log op) ops' @ t.ops)

  let add_cmd t ~cmd ~term =
    let l, op = L.add_cmd t.data.log cmd term in
    A.set (data @> log) t ~to_:l |> A.set ops ~to_:(Log op :: t.ops)

  let add_cmds t ~cmds ~term =
    let l, ops' = L.add_cmds t.data.log cmds term in
    A.set (data @> log) t ~to_:l
    |> A.set ops ~to_:(List.map ~f:(fun op -> Log op) ops' @ t.ops)
end

module MutableStorage (I : IStorage) : sig
  type wal

  type t = {mutable state: I.t; wal: wal} [@@deriving sexp_of, accessors]

  val get_state : t -> I.t

  val update : t -> I.t -> [`SyncPossible | `NoSync]

  val of_path : ?file_size:int64 -> string -> t Deferred.t

  val datasync : t -> log_index Deferred.t

  val close : t -> unit Deferred.t
end = struct
  module P = Owal.Persistant (I)

  type wal = P.t

  type t = {mutable state: I.t; wal: wal} [@@deriving accessors]

  let get_state t = t.state

  let sexp_of_t t = [%message (t.state : I.t)]

  let update t i' =
    t.state <- I.reset_ops i' ;
    match I.get_ops i' with
    | [] ->
        `NoSync
    | ops ->
        (* get oldest to newest ordering of ops *)
        List.iter ~f:(P.write t.wal) (List.rev ops) ;
        `SyncPossible

  let of_path ?file_size path =
    let%map wal, state = P.of_path ?file_size path in
    {state; wal}

  let datasync t =
    let max_index = I.get_max_index t.state in
    let%bind () = P.datasync t.wal in
    return max_index

  let close t = P.close t.wal
end

module MessageTypes = struct
  type request_vote = {src: node_id; term: term; leader_commit: log_index}
  [@@deriving bin_io, sexp]

  type request_vote_response =
    { src: node_id
    ; term: term
    ; vote_granted: bool
    ; entries: log_entry list
    ; start_index: log_index }
  [@@deriving bin_io, sexp]

  type append_entries =
    { src: node_id
    ; term: term
    ; prev_log_index: log_index
    ; prev_log_term: term
    ; entries: log_entry list
    ; entries_length: log_index
    ; leader_commit: log_index }
  [@@deriving bin_io, sexp]

  (* success is either the highest replicated term (match index) or prev_log_index *)
  type append_entries_response =
    {src: node_id; term: term; success: (log_index, log_index) Result.t}
  [@@deriving bin_io, sexp]

  type client_request = command [@@deriving bin_io, sexp]

  type client_response = (op_result, [`Unapplied]) Result.t
  [@@deriving bin_io, sexp]
end

module RPCs = struct
  open MessageTypes

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

  let client_request =
    Async.Rpc.Rpc.create ~name:"client_request" ~version:0
      ~bin_query:bin_client_request ~bin_response:bin_client_response
end

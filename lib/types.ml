open! Core
open! Ppx_log_async

let logger =
  let open Async_unix.Log in
  create ~level:`Info ~output:[] ~on_error:`Raise
    ~transform:(fun m -> Message.add_tags m [("src", "Types")])
    ()

type time = float

type node_addr = string

module Id = Unique_id.Int63 ()

type command_id = Id.t [@@deriving bin_io, sexp]

type client_id = Id.t [@@deriving bin_io, sexp]

type node_id = int [@@deriving bin_io, sexp]

type key = string [@@deriving bin_io, sexp]

type value = string [@@deriving bin_io, sexp]

type state_machine = (key, value) Hashtbl.t

type op = Read of key | Write of key * value [@@deriving bin_io, sexp]

module Command = struct
  type t = {op: op; id: command_id} [@@deriving bin_io, sexp]

  let compare a b = Id.compare a.id b.id

  let hash t = Id.hash t.id
end

type command = Command.t [@@deriving bin_io, sexp]

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

type log_index = int64 [@@deriving bin_io, sexp]

type term = int [@@deriving compare, equal, bin_io, sexp]

type log_entry = {command: command; term: term} [@@deriving bin_io, sexp]

module Wal = struct
  module Term = struct
    module T = struct
      type t = term [@@deriving bin_io]

      let init () = 0

      type op = t [@@deriving bin_io]

      let apply _t op = op
    end

    let update_term t op = (T.apply t op, [op])

    include T
  end

  module Log = struct
    module L = struct
      module IdSet = Set.Make (Id)

      type t = {store: log_entry list; command_set: IdSet.t; length: int64}
      [@@deriving sexp]

      let nth_of_index t i = Int64.(t.length - i)

      let drop_of_index t i = Int64.(nth_of_index t i + one)

      let init () = {store= []; command_set= IdSet.empty; length= Int64.zero}

      type op = Add of log_entry | RemoveGEQ of log_index [@@deriving bin_io]

      let add t entry =
        let command_set = Set.add t.command_set entry.command.id in
        { store= entry :: t.store
        ; command_set
        ; length= Int64.(t.length + of_int 1) }

      let remove_geq t i =
        let drop = drop_of_index t i |> Int64.(max zero) in
        let removed, store = List.split_n t.store (Int64.to_int_exn drop) in
        let command_set =
          List.fold_left removed ~init:t.command_set ~f:(fun cset entry ->
              Set.remove cset entry.command.id)
        in
        let length = Int64.(max zero (t.length - drop)) in
        {store; command_set; length}

      let apply t = function
        | Add entry ->
            add t entry
        | RemoveGEQ i ->
            remove_geq t i
    end

    type t = L.t [@@deriving sexp]

    type op = L.op

    let get (t : L.t) index =
      let nth = L.nth_of_index t index |> Int64.to_int_exn in
      List.nth t.store nth
      |> Result.of_option
           ~error:(Not_found_s (Sexp.Atom (Fmt.str "%a" Fmt.int64 index)))

    let get_exn t i = get t i |> Result.ok_exn

    let get_term t index =
      match index with
      | i when Int64.(i = zero) ->
          Ok 0
      | _ ->
          let open Result.Monad_infix in
          get t index >>= fun entry -> Ok entry.term

    let get_term_exn t index = get_term t index |> Result.ok_exn

    let apply_wrap t op = (L.apply t op, [op])

    let add entry t = (L.add t entry, L.Add entry)

    let addv cs t term =
      List.fold cs ~init:(t, []) ~f:(fun (t, ops) command ->
          let t, op = add {term; command} t in
          let ops = op :: ops in
          (t, ops))

    let removeGEQ index t = (L.remove_geq t index, L.RemoveGEQ index)

    let get_max_index (t : L.t) = t.length

    let id_in_log (t : L.t) id = Set.mem t.command_set id

    let entries_after_inc t index =
      let drop = L.drop_of_index t index |> Int64.to_int_exn in
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
        | x :: _, (y :: _ as ys) when equal_term x.term y.term ->
            [%log.debug
              logger "Mismatch while merging" (x : log_entry) (y : log_entry)] ;
            Logs.debug (fun m -> m "Mismatch at %a" Fmt.int64 idx) ;
            (Some idx, ys)
        | _ :: xs, _ :: ys ->
            merge_y_into_x Int64.(succ idx) (xs, ys)
      in
      let removeGEQ_o, entries_to_add =
        merge_y_into_x start_index
          (List.rev relevant_entries, List.rev new_entries)
      in
      (* entries_to_add is in oldest first order *)
      let entries_to_add = entries_to_add in
      let ops = [] in
      let t, ops =
        match removeGEQ_o with
        | Some i ->
            let t', op' = apply_wrap t (RemoveGEQ i) in
            (t', op' @ ops)
        | None ->
            (t, ops)
      in
      let t, ops =
        List.fold_left entries_to_add ~init:(t, ops) ~f:(fun (t, ops) v ->
            let t', ops' = apply_wrap t (Add v) in
            (t', ops' @ ops))
      in
      (t, List.rev ops)

    let append t command term =
      let entry = {command; term} in
      apply_wrap t (Add entry)
  end

  module P = struct
    type t = {term: Term.t; log: Log.t}

    let init () = {term= Term.init (); log= Log.L.init ()}

    type op = Term of Term.T.op | Log of Log.L.op [@@deriving bin_io]

    let apply t = function
      | Term op ->
          {t with term= Term.T.apply t.term op}
      | Log op ->
          {t with log= Log.L.apply t.log op}
  end

  include Owal.Persistant (P)
end

type log = Wal.Log.L.t

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

  type client_response = op_result [@@deriving bin_io, sexp]
end

module RPCs = struct
  open MessageTypes

  let request_vote =
    Async.Rpc.One_way.create ~name:"request_vote" ~version:0 ~bin_msg:bin_request_vote

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

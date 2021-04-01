open! Core
open! Types
open! Ppx_log_async
module A = Accessor
open! A.O

let logger =
  let open Async_unix.Log in
  create ~level:`Info ~output:[] ~on_error:`Raise
    ~transform:(fun m -> Message.add_tags m [("src", "ImmutableLog")])
    ()


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

module T = ITerm
module L = ILog

(*---- Immutable API -------------------------------*)

type op = Term of T.op | Log of L.op
[@@deriving bin_io, sexp_of, accessors, compare]

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

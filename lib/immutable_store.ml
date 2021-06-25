open! Core
open! Types
open! Ppx_log_async
module A = Accessor
open! A.O

let _logger =
  let open Async_unix.Log in
  create ~level:`Info ~output:[] ~on_error:`Raise
    ~transform:(fun m -> Message.add_tags m [("src", "ImmutableLog")])
    ()

module ILog = struct
  module IdSet = Set.Make (Uuid.Stable.V1)

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

  let fold_geq t ~idx ~init ~f =
    let drop = I.drop_of_index t idx |> Int64.to_int_exn in
    let relevant_entries = List.split_n t.store drop |> fst |> List.rev in
    List.fold_left relevant_entries ~init ~f

  let fold_until_geq ~idx ~init ~f ~finish t =
    Container.fold_until ~fold:(fold_geq ~idx) ~init ~f ~finish t

  let foldi_geq t ~idx ~init ~f =
    let open Int64 in
    snd
      (fold_geq t ~idx ~init:(idx, init) ~f:(fun (i, acc) v ->
           (succ i, f i acc v) ) )

  let foldi_until_geq :
         idx:log_index
      -> init:'acc
      -> f:(log_index -> 'acc -> log_entry -> ('b, 'final) Continue_or_stop.t)
      -> finish:(log_index -> 'acc -> 'final)
      -> t
      -> 'final =
   fun ~idx ~init ~f ~finish t ->
    let open Continue_or_stop in
    let open Int64 in
    let finish (i, v) = finish i v in
    let f (i, acc) v =
      match f i acc v with
      | Continue a ->
          Continue (succ i, a)
      | Stop v ->
          Stop v
    in
    Container.fold_until ~fold:(fold_geq ~idx) ~init:(idx, init) ~f ~finish t

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
  A.set (data @> current_term) t ~to_:term |> A.set ops ~to_:(Term term :: t.ops)

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

let fold_geq t = L.fold_geq t.data.log

let foldi_geq t = L.foldi_geq t.data.log

let fold_until_geq ~idx ~init ~f ~finish t =
  L.fold_until_geq ~idx ~init ~f ~finish t.data.log

let foldi_until_geq ~idx ~init ~f ~finish t =
  L.foldi_until_geq ~idx ~init ~f ~finish t.data.log

let to_string t = [%message (t : t)] |> Sexp.to_string_hum

let add_cmd t ~cmd ~term =
  let l, op = L.add_cmd t.data.log cmd term in
  A.set (data @> log) t ~to_:l |> A.set ops ~to_:(Log op :: t.ops)

let add_cmds t ~cmds ~term =
  let l, ops' = L.add_cmds t.data.log cmds term in
  A.set (data @> log) t ~to_:l
  |> A.set ops ~to_:(List.map ~f:(fun op -> Log op) ops' @ t.ops)

open Owal
open Types
open Base

(* Common operations:
        - Get max_index
        - Indices after
        - Get specific index (close to end generally)
        - Append to end of log
    *)
module L_t = struct
  type t = {store: log_entry list; length: int64}

  let nth_of_index t i = Int64.(t.length - i) 

  let drop_of_index t i = Int64.(nth_of_index t i + one)

  let init () = {store= []; length= Int64.zero}

  type op = Add of log_entry [@key 1] | RemoveGEQ of log_index [@key 2]
  [@@deriving protobuf]

  let encode_blit op =
    let res = Protobuf.Encoder.encode_exn op_to_protobuf op in
    let p_len = Bytes.length res in
    let blit buf ~offset =
      Bytes.blit ~src:res ~src_pos:0 ~dst:buf ~dst_pos:offset ~len:p_len
    in
    (p_len, blit)

  let decode buf ~offset =
    let decode_buf =
      Bytes.sub buf ~pos:offset ~len:(Bytes.length buf - offset)
    in
    Protobuf.Decoder.decode_exn op_from_protobuf decode_buf

  let apply t = function
    | Add entry ->
      {store= entry :: t.store; length= Int64.(t.length + of_int 1)}
    | RemoveGEQ i ->
        let drop = drop_of_index t i |> Int64.(max zero) in
        let _, store = List.split_n t.store (Int64.to_int_exn drop) in
        let length = Int64.(max zero (t.length - drop)) in
        {store; length}
end

module P = Persistant (L_t)
open L_t
include P

type t = P.t

let get (t : t) index =
  let nth = nth_of_index t.t index |> Int64.to_int_exn in
  List.nth t.t.store nth |> Result.of_option ~error:(Not_found_s (Sexp.Atom (Fmt.str "%a" Fmt.int64 index)))

let get_exn t i = get t i |> Result.ok_exn

let get_term t index =
  match index with
  | i when Int64.(i = zero) ->
      Ok Int64.zero
  | _ ->
      let open Result.Monad_infix in
      get t index >>= fun entry -> Ok entry.term

let get_term_exn t index = get_term t index |> Result.ok_exn

let add entry t = change (Add entry) t

let removeGEQ index t = change (RemoveGEQ index) t

let get_max_index (t : t) = t.t.length

let entries_after_inc t index =
  let drop = drop_of_index t.t index |> Int64.to_int_exn in
  List.split_n t.t.store drop |> fst

let to_string t =
  let entries = entries_after_inc t Int64.zero in
  string_of_entries entries

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
    | x :: _, (y :: _ as ys) when Int64.(x.term <> y.term) ->
        (Some idx, ys)
    | _ :: xs, _ :: ys ->
        merge_y_into_x Int64.(succ idx) (xs, ys)
  in
  let removeGEQ_o, entries_to_add =
    merge_y_into_x start_index (List.rev relevant_entries, List.rev new_entries)
  in
  let entries_to_add = entries_to_add in
  let t =
    match removeGEQ_o with Some i -> change (RemoveGEQ i) t | None -> t
  in
  List.fold_left entries_to_add ~init:t ~f:(fun t v -> change (Add v) t)

let append t command_id term =
  let entry = {command_id; term} in
  change (Add entry) t

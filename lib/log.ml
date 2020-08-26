open Odbutils.Owal
open Types
open Base

(* Common operations:
   - Get max_index
   - Indices after
   - Get specific index (close to end generally)
   - Append to end of log
*)
module L = struct
  module Persistant = struct
    type t =
      { store: log_entry list
      ; command_set: (command_id, Int64.comparator_witness) Set.t
      ; length: int64 }

    let nth_of_index t i = Int64.(t.length - i)

    let drop_of_index t i = Int64.(nth_of_index t i + one)

    let init () =
      {store= []; command_set= Set.empty (module Int64); length= Int64.zero}

    type op = Add of log_entry | RemoveGEQ of log_index

    let encode buf offset = function
      | Add {term; command} ->
        Logs.debug (fun m -> m "Encoding Add"); 
          EndianBytes.LittleEndian.set_int8 buf offset 0 ;
          EndianBytes.LittleEndian.set_int64 buf (offset + 1) term ;
          StateMachine.blit_command buf command ~offset:(offset + 9)
      | RemoveGEQ index ->
        Logs.debug (fun m -> m "Encoding Remove GEQ"); 
          EndianBytes.LittleEndian.set_int8 buf offset 1 ;
          EndianBytes.LittleEndian.set_int64 buf (offset + 1) index

    let get_encoded_length = function
      | Add {term= _; command} ->
          9 + StateMachine.get_encoded_length command
      | RemoveGEQ _index ->
          9

    let encode_blit op =
      let p_len = get_encoded_length op in
      let blit buf ~offset = encode buf offset op in
      (p_len, blit)

    let decode buf ~offset =
      match EndianBytes.LittleEndian.get_int8 buf offset with
      | 0 ->
          let term = EndianBytes.LittleEndian.get_int64 buf (offset + 1) in
          let command = StateMachine.decode_command buf ~offset:(offset + 9) in
          Add {term; command}
      | 1 ->
          let index = EndianBytes.LittleEndian.get_int64 buf (offset + 1) in
          RemoveGEQ index
      | _ ->
          assert false

    let apply t = function
      | Add entry ->
          let command_set = Set.add t.command_set entry.command.id in
          { store= entry :: t.store
          ; command_set
          ; length= Int64.(t.length + of_int 1) }
      | RemoveGEQ i ->
          let drop = drop_of_index t i |> Int64.(max zero) in
          let removed, store = List.split_n t.store (Int64.to_int_exn drop) in
          let command_set =
            List.fold_left removed ~init:t.command_set ~f:(fun cset entry ->
                Set.remove cset entry.command.id)
          in
          let length = Int64.(max zero (t.length - drop)) in
          {store; command_set; length}
  end

  type t = Persistant.t

  type op = Persistant.op

  open Persistant

  let get (t : t) index =
    let nth = nth_of_index t index |> Int64.to_int_exn in
    List.nth t.store nth
    |> Result.of_option
         ~error:(Not_found_s (Sexp.Atom (Fmt.str "%a" Fmt.int64 index)))

  let get_exn t i = get t i |> Result.ok_exn

  let get_term t index =
    match index with
    | i when Int64.(i = zero) ->
        Ok Int64.zero
    | _ ->
        let open Result.Monad_infix in
        get t index >>= fun entry -> Ok entry.term

  let get_term_exn t index = get_term t index |> Result.ok_exn

  let apply_wrap t op = (apply t op, [op])

  let add entry t = apply_wrap t (Add entry)

  let removeGEQ index t = apply_wrap t (RemoveGEQ index)

  let get_max_index (t : t) = t.length

  let id_in_log t id = Set.mem t.command_set id

  let entries_after_inc t index =
    let drop = drop_of_index t index |> Int64.to_int_exn in
    List.split_n t.store drop |> fst

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
        Logs.debug (fun m -> m "Mismatch at %a" Fmt.int64 idx);
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
    let ops = OpsList.empty in
    let t, ops =
      match removeGEQ_o with
      | Some i ->
        let t', op' = apply_wrap t (RemoveGEQ i) in
        t', OpsList.appendv ops op'
      | None ->
          (t, ops)
    in
    let t, ops =
      List.fold_left entries_to_add ~init:(t, ops) ~f:(fun (t, ops) v ->
          let t', ops' = apply_wrap t (Add v) in
          (t', OpsList.appendv ops ops'))
    in
    (t, OpsList.get_list ops)

  let append t command term =
    let entry = {command; term} in
    apply_wrap t (Add entry)
end

module P = Persistant (L.Persistant)
include P
type t_wal = P.t
include L

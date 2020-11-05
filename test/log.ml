open! Core
open! Async
open! Ocamlpaxos
open Types
module L = Types.Wal.Log

let with_file f path =
  let%bind wal, Wal.P.{log; _} = Wal.of_path path in
  let%bind () = f (wal, log) in
  Wal.close wal

let make_entry id term key =
  let id = Types.Id.of_int_exn id in
  let command = Types.Command.{op= Read (Int.to_string key); id} in
  Types.{command; term}

let init_log (wal, log) =
  let init_state =
    [(1, 1); (1, 2); (3, 3); (3, 4)]
    |> List.mapi ~f:(fun id (term, key) -> make_entry (id + 1) term key)
  in
  let fold log entry =
    let log, op = L.add entry log in
    Wal.write wal (Log op) ; log
  in
  let log = List.fold_left init_state ~init:log ~f:fold in
  let%bind () = Wal.datasync wal in
  return log

let%expect_test "id_in_log" =
  let path = "id_in_log.wal" in
  let f (wal, log) =
    L.id_in_log log (Types.Id.of_int_exn 1) |> Bool.to_string |> print_endline ;
    let%bind () = [%expect {| false |}] in
    let%bind log = init_log (wal, log) in
    L.id_in_log log (Types.Id.of_int_exn 1) |> Bool.to_string |> print_endline ;
    let%bind () = [%expect {| true |}] in
    return ()
  in
  with_file f path

let%expect_test "get_term" =
  let path = "term_test.wal" in
  let f (wal, log) =
    L.get_term_exn log (Int64.of_int 0)
    |> [%sexp_of: Types.term] |> Sexp.to_string_hum |> print_endline ;
    let%bind () = [%expect {| 0 |}] in
    let%bind log = init_log (wal, log) in
    L.get_term_exn log (Int64.of_int 0)
    |> [%sexp_of: Types.term] |> Sexp.to_string_hum |> print_endline ;
    let%bind () = [%expect {| 0 |}] in
    L.get_term_exn log (Int64.of_int 1)
    |> [%sexp_of: Types.term] |> Sexp.to_string_hum |> print_endline ;
    let%bind () = [%expect {| 1 |}] in
    L.get_term_exn log (Int64.of_int 2)
    |> [%sexp_of: Types.term] |> Sexp.to_string_hum |> print_endline ;
    let%bind () = [%expect {| 1 |}] in
    L.get_term_exn log (Int64.of_int 3)
    |> [%sexp_of: Types.term] |> Sexp.to_string_hum |> print_endline ;
    [%expect {| 3 |}]
  in
  with_file f path

let%expect_test "to_string" =
  let path = "to_string.wal" in
  let f (wal, log) =
    let%bind log = init_log (wal, log) in
    log |> L.to_string |> print_endline ;
    [%expect
      {|
      (((command ((op (Read 4)) (id 4))) (term 3))
       ((command ((op (Read 3)) (id 3))) (term 3))
       ((command ((op (Read 2)) (id 2))) (term 1))
       ((command ((op (Read 1)) (id 1))) (term 1))) |}]
  in
  with_file f path

let%expect_test "max_index" =
  let path = "max_index.wal" in
  let f (wal, log) =
    log |> L.get_max_index |> Int64.to_string |> print_endline ;
    let%bind () = [%expect {| 0 |}] in
    let%bind log = init_log (wal, log) in
    log |> L.get_max_index |> Int64.to_string |> print_endline ;
    [%expect {| 4 |}]
  in
  with_file f path

let%expect_test "add_entries_rem_conflicts" =
  let path = "add_entries" in
  let f (wal, log) =
    let%bind log = init_log (wal, log) in
    let entries =
      List.map [(3, 3, 3); (2, 1, 2)] ~f:(fun (a, b, c) -> make_entry a b c)
    in
    let log, _ =
      L.add_entries_remove_conflicts log ~start_index:(Int64.of_int 2) entries
    in
    log |> L.to_string |> print_endline ;
    [%expect
      {|
      (((command ((op (Read 3)) (id 3))) (term 3))
       ((command ((op (Read 2)) (id 2))) (term 1))
       ((command ((op (Read 1)) (id 1))) (term 1))) |}]
  in
  with_file f path

let%expect_test "rem_geq" =
  let path = "rem_get.wal" in
  let f (wal, log) =
    let%bind log = init_log (wal, log) in
    let log, _ = L.removeGEQ Int64.(of_int 3) log in
    log |> L.to_string |> print_endline ;
    [%expect
      {|
       (((command ((op (Read 2)) (id 2))) (term 1))
        ((command ((op (Read 1)) (id 1))) (term 1))) |}]
  in
  with_file f path

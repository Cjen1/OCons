open Lwt.Infix
open Ocamlpaxos
module L = Log

let test_file = "test.tmp"

let i64_of_pair (a, b) = Int64.(of_int a, of_int b)

let entries_of_list xs =
  xs |> List.map i64_of_pair
  |> List.map (fun (command_id, term) -> Types.{command_id; term})

let log_entries = [(4, 2); (3, 2); (2, 1); (1, 1)] |> List.map i64_of_pair

let log_entry = Alcotest.testable Types.pp_entry ( = )

let open_file f =
  L.of_file test_file
  >>= fun t ->
  f t ;
  L.sync t >|= Result.get_ok >>= fun () -> L.close t

let test_change_sync _ () =
  open_file (fun t ->
      let t =
        List.fold_right (fun (id, term) t -> L.append t id term) log_entries t
      in
      let log_repr =
        let map (id, _term) = L.get_exn t id in
        List.map map log_entries
      in
      let test_repr =
        let map (command_id, term) = Types.{command_id; term} in
        List.map map log_entries
      in
      Alcotest.(check @@ list log_entry)
        "Check values set correctly" log_repr test_repr)

let test_reload _ () =
  open_file (fun t ->
      let log_repr = L.entries_after_inc t Int64.zero in
      let test_repr =
        let map (command_id, term) = Types.{command_id; term} in
        List.map map log_entries
      in
      Alcotest.(check @@ list log_entry)
        "Check reloaded values correctly" log_repr test_repr)

let test_get_term _ () =
  open_file (fun t ->
      let log_repr =
        let map (id, _) =
          let entry = L.get_exn t id in
          entry.term
        in
        List.map map log_entries
      in
      let test_repr = List.map snd log_entries in
      Alcotest.(check @@ list int64)
        "Check reloaded values correctly" log_repr test_repr)

let test_get_max_index _ () =
  open_file (fun t ->
      let v = L.get_max_index t in
      Alcotest.(check int64) "Get max index" (Int64.of_int 4) v)

let test_entries_after_inc _ () =
  open_file (fun t ->
      let list_repr = L.entries_after_inc t Int64.(of_int 2) in
      let test_repr =
        let map (command_id, term) = Types.{command_id; term} in
        Base.List.split_n log_entries 3 |> fst |> List.map map
      in
      Alcotest.(check @@ list log_entry)
        "Check entries after equal" list_repr test_repr)

let test_add_entries_remove_conflicts _ () =
  open_file (fun t ->
      let start_index = Int64.of_int 2 in
      let new_entries = [(3, 3); (2, 1)] |> entries_of_list in
      let test_repr = [(3, 3); (2, 1); (1, 1)] |> entries_of_list in
      let t = L.add_entries_remove_conflicts t ~start_index new_entries in
      let log_repr = L.entries_after_inc t Int64.zero in
      Alcotest.(check @@ list log_entry)
        "Check conflicts resolved" log_repr test_repr)

let test_remove_geq _ () =
  open_file (fun t ->
      let index = Int64.of_int 3 in
      let test_repr = [(2, 1); (1, 1)] |> entries_of_list in
      let t = L.removeGEQ index t in
      let log_repr = L.entries_after_inc t Int64.zero in
      Alcotest.(check @@ list log_entry)
        "Check conflicts resolved" log_repr test_repr)

let reporter =
  let open Core in
  let report src level ~over k msgf =
    let k _ = over () ; k () in
    let src = Logs.Src.name src in
    msgf
    @@ fun ?header ?tags:_ fmt ->
    Fmt.kpf k Fmt.stdout
      ("[%a] %a %a @[" ^^ fmt ^^ "@]@.")
      Time.pp (Time.now ())
      Fmt.(styled `Magenta string)
      (Printf.sprintf "%14s" src)
      Logs_fmt.pp_header (level, header)
  in
  {Logs.report}

let () =
  Logs.(set_level (Some Debug)) ;
  Logs.set_reporter reporter ;
  let _ =
    Sys.signal Sys.sigpipe
      (Sys.Signal_handle
         (fun _ -> raise @@ Unix.Unix_error (Unix.EPIPE, "", "")))
  in
  let open Alcotest_lwt in
  let run () =
    Lwt_main.run
    @@ run "Log tests"
         [ ( "Basic functionality"
           , [ test_case "Change" `Quick test_change_sync
             ; test_case "Reload" `Quick test_reload
             ; test_case "get_term" `Quick test_get_term
             ; test_case "get_max_index" `Quick test_get_max_index
             ; test_case "entries_after" `Quick test_entries_after_inc
             ; test_case "add_remove_conficts" `Quick
                 test_add_entries_remove_conflicts
             ; test_case "remove geq" `Quick test_remove_geq ] ) ]
  in
  let finally () = Unix.unlink test_file in
  try run () ; finally () with e -> finally () ; raise e

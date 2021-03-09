open! Shexp_process
open! Shexp_process.Infix
open! Shexp_process.Let_syntax
module List' = List
open! Core

let kill bg =
  let pid = Background_command.pid bg in
  let%bind () = call ["kill"; Int.to_string pid] in
  wait bg |> map ~f:(fun _ -> ())

let set_core_profiler output = set_env "CORE_PROFILER" ("OUTPUT_FILE=" ^ output)

let if_exists f file = if%bind file_exists file then f file else return ()

let start_server node_id =
  let data_dir = Fmt.str "%d.datadir" node_id in
  let%bind () = if_exists rm_rf data_dir in
  let log_dir = Fmt.str "%d.log" node_id in
  let%bind () = if_exists rm_rf log_dir in
  let%bind () = mkdir ~perm:0o0777 log_dir in
  let prof_file = Fmt.str "%d.dat" node_id in
  let%bind () = if_exists rm prof_file in
  let port = node_id + 4 |> Int.to_string in
  let node_id = Int.to_string node_id in
  let bg =
    spawn "dune"
    @@ ["exec"; "bin/main.exe"; "--"]
    @ [node_id; "1:127.0.0.1:5002,2:127.0.0.1:6002,3:127.0.0.1:7002"; data_dir]
    @ [port ^ "001"; port ^ "002"]
    @ ["5"; "0.1"] @ ["-s"; "100"] @ ["-log-level"; "debug"]
    @ ["-log-to-file"; log_dir ^ "/logger"]
    |> set_core_profiler prof_file
    |> redirect
         Std_io.[Stdout]
         ~perm:0o777
         ~flags:Unix.[O_RDWR; O_CREAT]
         (log_dir ^ "/stdout")
    |> redirect
         Std_io.[Stderr]
         ~perm:0o777
         ~flags:Unix.[O_RDWR; O_CREAT]
         (log_dir ^ "/stderr")
  in
  return @@ eval bg

let client_test nodes rate =
  let nodes = List.map nodes ~f:(fun i -> ((i + 4) * 1000) + 1) in
  let nodes = nodes |> [%sexp_of: int list] |> Sexp.to_string in
  call_exit_code @@ ["timeout"; "1m"]
  @ ["dune"; "exec"; "bin/bench.exe"; "--"]
  @ ["-p"; nodes] @ ["-t"; Int.to_string rate] @ ["-n"; "100000"]
  @ ["-log-level"; "debug"]

let print_summary file =
  call ["core-profiler-tool"; "summary"; "-filename"; file]

let test target_rate =
  let%bind () = call ["dune"; "build"; "@install"] in
  let%bind () = print @@ Fmt.str "Built main\n" in
  let%bind () = sleep 1. in
  let%bind bgs = fork_all [start_server 1; start_server 2] in
  let%bind exit_code = client_test [1; 2] target_rate in
  let%bind () =
    match exit_code with
    | 0 ->
        List'.iter [1; 2] ~f:(fun node -> print_summary (Fmt.str "%d.dat" node))
    | 124 ->
        print @@ Fmt.str "Benchmark timed out\n"
    | v ->
        print @@ Fmt.str "Test was not successful with code %d\n" v
  in
  List'.iter bgs ~f:(fun bg -> kill bg)

let () =
  Command.basic ~summary:"Script for automating benchmarking"
    [%map_open.Command
      let rate =
        flag "rate" (optional_with_default 5000 int) ~doc:"N Target rate"
      and trace = flag "trace" no_arg ~doc:" Print a trace of the test" in
      fun () ->
        match trace with
        | true ->
            let (), trace = Traced.eval_exn @@ test rate in
            trace |> Sexp.to_string_hum |> print_endline
        | false ->
            eval (test rate)]
  |> Command.run

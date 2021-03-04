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

let rm_f file = if%bind file_exists file then rm_rf file else return ()

let start_server node_id =
  let data_dir = Fmt.str "%d.datadir" node_id in
  let%bind () = rm_f data_dir in
  let log_dir = Fmt.str "%d.log" node_id in
  let%bind () = rm_f log_dir in
  let%bind () = mkdir ~perm:0o0777 log_dir in
  let port = node_id + 4 |> Int.to_string in
  let node_id = Int.to_string node_id in
  spawn "dune"
  @@ ["exec"; "bin/main.exe"; "--"]
  @ [node_id; "1:127.0.0.1:5002,2:127.0.0.1:6002,3:127.0.0.1:7002"; data_dir]
  @ [port ^ "001"; port ^ "002"]
  @ ["5"; "5"] @ ["-s"; "100"]
  |> set_core_profiler (node_id ^ ".dat")
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

let client_test nodes rate =
  let nodes = List.map nodes ~f:(fun i -> ((i + 5) * 1000) + 1) in
  let nodes = nodes |> [%sexp_of: int list] |> Sexp.to_string in
  call
    [ "dune"
    ; "exec"
    ; "bin/bench.exe"
    ; "--"
    ; "-p"
    ; nodes
    ; "-t"
    ; Int.to_string rate
    ; "-n"
    ; "100000" ]

let print_summary file =
  call ["core-profiler-tool"; "summary"; "-filename"; file]

let test target_rate =
  let%bind () = call ["dune"; "build"; "@install"] in
  let%bind () = print @@ Fmt.str "Built main\n" in
  let%bind () = sleep 1. in
  let%bind bgs = fork_all [start_server 1; start_server 2] in
  let%bind () = client_test [1; 2] target_rate in
  let%bind () =
    List'.iter [1; 2] ~f:(fun node -> print_summary (Fmt.str "%d.dat" node))
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

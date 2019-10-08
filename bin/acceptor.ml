(* main.ml *)
open Lib

(* Sample acceptor code *)
let run_acceptor host p1_port p2_port wal_location =
  let host_inet_addr = Unix.inet_addr_of_string host in
  Lwt_main.run
  @@ Acceptor.create_and_start_acceptor host_inet_addr p1_port p2_port
       wal_location

(* Handle the command line arguments and run application is specified mode *)
let command =
  Core.Command.basic ~summary:"Acceptor for Ocaml Paxos"
    Core.Command.Let_syntax.(
      let%map_open p1_port = anon ("phase_1_port" %: int)
      and p2_port = anon ("phase_2_port" %: int)
      and log_dir = anon ("log_directory" %: string) in
      fun () -> run_acceptor "127.0.0.1" p1_port p2_port log_dir)

let reporter =
  let report src level ~over k msgf =
    let k _ = over () ; k () in
    let src = Logs.Src.name src in
    msgf
    @@ fun ?header ?tags:_ fmt ->
    Fmt.kpf k Fmt.stdout
      ("%a %a @[" ^^ fmt ^^ "@]@.")
      Fmt.(styled `Magenta string)
      (Printf.sprintf "%14s" src)
      Logs_fmt.pp_header (level, header)
  in
  {Logs.report}

let () =
  Fmt_tty.setup_std_outputs () ;
  Logs.(set_level (Some Info)) ;
  Logs.set_reporter reporter ; Core.Command.run command

(* main.ml *)
open Lib

(* Handle the command line arguments and run application is specified mode *)
let command =
  Core.Command.basic ~summary:"Acceptor for Ocaml Paxos"
    Core.Command.Let_syntax.(
      let%map_open leader_uris = anon ("Leader uris" %: string)
      and client_port = anon ("client_port" %: int)
      and decision_port = anon ("decision_port" %: int) in
      fun () ->
        let leader_uris =
          leader_uris |> Base.String.split ~on:','
          |> Base.List.map ~f:Utils.uri_of_string
        in
        let host_inet_addr = Unix.inet_addr_of_string "127.0.0.1" in
        Lwt_main.run
        @@ Replica.create_and_start host_inet_addr client_port decision_port
             leader_uris)

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
  Logs.(set_level (Some Debug)) ;
  Logs.set_reporter reporter ; Core.Command.run command

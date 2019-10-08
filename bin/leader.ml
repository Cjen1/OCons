(* main.ml *)
open Lib

(* Handle the command line arguments and run application is specified mode *)
let command =
  Core.Command.basic ~summary:"Acceptor for Ocaml Paxos"
    Core.Command.Let_syntax.(
      let%map_open endpoints = anon ("endpoints" %: string)
      and client_port = anon ("client_port" %: int)
      and replica_request_port = anon ("replica_port" %: int)
      and p1_port = anon ("phase_1_port" %: int)
      and p2_port = anon ("phase_2_port" %: int) in
      fun () ->
        let endpoints = Base.String.split ~on:',' endpoints in
        let acceptor_uris_p1 =
          Base.List.map endpoints ~f:(fun ip ->
              Utils.uri_of_string_and_port ip p1_port)
        in
        let acceptor_uris_p2 =
          Base.List.map endpoints ~f:(fun ip ->
              Utils.uri_of_string_and_port ip p2_port)
        in
        let replica_uris =
          Base.List.map endpoints ~f:(fun ip ->
              Utils.uri_of_string_and_port ip replica_request_port)
        in
        let initial_timeout = 5. in
        let host_inet_addr = Unix.inet_addr_of_string "127.0.0.1" in
        Lwt_main.run
        @@ Leader.create_and_start_leader host_inet_addr client_port
             acceptor_uris_p1 acceptor_uris_p2 replica_uris initial_timeout)

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

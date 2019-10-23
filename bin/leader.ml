(* main.ml *)
open Lib

(* Handle the command line arguments and run application is specified mode *)
let command =
  Core.Command.basic ~summary:"Leader for Ocaml Paxos"
    Core.Command.Let_syntax.(
      let%map_open acceptor_uris_p1 = anon ("Phase 1 acceptor uris" %: string)
      and acceptor_uris_p2 = anon ("Phase 2 acceptor uris" %: string)
      and replica_decision_uris = anon ("Replica uris" %: string)
      and replica_port = anon ("Replica request port" %: int) in
      fun () ->
        let acceptor_uris_p1 =
          acceptor_uris_p1 |> Base.String.split ~on:','
          |> Base.List.map ~f:Utils.uri_of_string
        in
        let acceptor_uris_p2 =
          acceptor_uris_p2 |> Base.String.split ~on:','
          |> Base.List.map ~f:Utils.uri_of_string
        in
        let replica_decision_uris =
          replica_decision_uris |> Base.String.split ~on:','
          |> Base.List.map ~f:Utils.uri_of_string
        in
        let initial_timeout = 5. in
        let host_inet_addr = Unix.inet_addr_of_string "127.0.0.1" in
        Lwt_main.run
        @@ Leader.create_and_start_leader host_inet_addr replica_port
             acceptor_uris_p1 acceptor_uris_p2 replica_decision_uris initial_timeout)

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
  Lwt_engine.set (new Lwt_engine.libev ());
  Fmt_tty.setup_std_outputs () ;
  Logs.(set_level (Some Debug)) ;
  Logs.set_reporter reporter ; Core.Command.run command

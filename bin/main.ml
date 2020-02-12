open Lib
open Core

let command =
  Core.Command.basic ~summary:"Ocaml_paxos"
    Core.Command.Let_syntax.(
      let%map_open client_port = anon ("client_port" %: int)
      and wal_loc = anon ("Log_location" %: string)
      and id = anon ("id" %: string)
      and endpoints = anon ("endpoints" %: string)
      and election_timeout = anon ("election_timeout" %: float) in
      fun () ->
        let p =
          let p, r = Lwt.task () in
          Lwt.wakeup_later r () ;
          let%lwt () = p in
          let endpoints =
            let pairs = Base.String.split ~on:',' endpoints in
            Base.List.mapi pairs ~f:(fun i xs ->
                match Base.String.split ~on:'=' xs with
                | [id; addr] ->
                    (id, addr, i)
                | _ ->
                    raise
                      (Invalid_argument "Need endpoints of the form [id=uri]"))
          in
          Printf.printf "" ;
          let client_port = Int.to_string client_port in
          Paxos.create_and_start ~data_path:wal_loc ~node_list:endpoints
            ~node_addr:id ~client_port ~election_timeout
        in
        Lwt_main.run p)

let reporter =
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
  Lwt_engine.set (new Lwt_engine.libev ()) ;
  Fmt_tty.setup_std_outputs () ;
  Logs.(set_level (Some Debug)) ;
  Logs.set_reporter reporter ; Core.Command.run command

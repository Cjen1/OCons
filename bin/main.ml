open Lib

let command =
  Core.Command.basic ~summary:"Ocaml_paxos"
    Core.Command.Let_syntax.(
      let%map_open system_port = anon ("system_port" %: int)
      and wal_loc = anon ("Log_location" %: string)
      and local_address = anon ("Local_address" %: string)
      and endpoints = anon ("endpoints" %: string)
      and client_port = anon ("client_port" %: int)
      and alive_timeout = anon ("alive_timeout" %: float) in
      fun () ->
        let p =
          let p, r = Lwt.task () in
          Lwt.wakeup_later r () ;
          let%lwt () = p in
          let local = local_address ^ ":" ^ Int.to_string system_port in
          let client = local_address ^ ":" ^ Int.to_string client_port in
          let endpoints = endpoints |> Base.String.split ~on:',' in
          let msg_layer,psml =
            Msg_layer.create ~node_list:endpoints ~local ~alive_timeout
          in
          let _,psl = Leader.create msg_layer local endpoints client in
          let _,psa = Acceptor.create wal_loc msg_layer local in
          Lwt.join [psl; psa; psml]
        in
        Lwt_main.run p)

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
  Lwt_engine.set (new Lwt_engine.libev ()) ;
  Fmt_tty.setup_std_outputs () ;
  Logs.(set_level (Some Debug)) ;
  Logs.set_reporter reporter ; Core.Command.run command

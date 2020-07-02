open Ocamlpaxos
open Core
open Unix_capnp_messaging

let network_address =
  let parse s =
    match Conn_manager.addr_of_string s with
    | Ok addr ->
        addr
    | Error (`Msg e) ->
        Fmt.failwith "Expected ip:port | path rather than %s " e
  in
  Command.Arg_type.create parse

let node_list =
  Command.Arg_type.create (fun ls ->
      String.split ls ~on:','
      |> List.map ~f:(String.split ~on:':')
      |> List.map ~f:(function
           | id :: xs -> (
               let rest = String.concat ~sep:":" xs in
               match Conn_manager.addr_of_string rest with
               | Ok addr ->
                   (Int64.of_string id, addr)
               | Error (`Msg e) ->
                   Fmt.failwith "Expected id:(ip:port|path) not %s" e )
           | _ ->
               assert false))

let command =
  Core.Command.basic ~summary:"Ocaml_paxos"
    Core.Command.Let_syntax.(
      let%map_open listen_address = anon ("listen_address" %: network_address)
      and data_path = anon ("data_path" %: string)
      and node_id = anon ("node_id" %: int)
      and node_list = anon ("node_list" %: node_list)
      and election_timeout = anon ("election_timeout" %: int)
      and tick_time = anon ("tick_time" %: float) in
      fun () ->
        let node_id = Int64.of_int node_id in
        let log_path = data_path ^ ".log" in
        let term_path = data_path ^ ".term" in
        Infra.create ~listen_address ~node_list ~election_timeout ~tick_time
          ~log_path ~term_path node_id
        |> Lwt_main.run)

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
  Logs.Src.list () |> List.iter ~f:(fun e -> Format.printf "%a\n" Logs.Src.pp e) ;
  Fmt.pr "V%d\n" 1 ;
  Logs.(set_level (Some Debug)) ;
  Logs.set_reporter reporter ; Core.Command.run command

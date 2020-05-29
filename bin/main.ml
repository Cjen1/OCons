open Ocamlpaxos
open Core

let network_address =
  Command.Arg_type.create (fun address ->
      match String.split address ~on:':' with
      | [ip; port] ->
          `TCP (ip, Int.of_string port)
      | _ ->
          eprintf "%s is not a valid address" address ;
          exit 1)

let node_list =
  Command.Arg_type.create (fun ls ->
      String.split ls ~on:','
      |> List.map ~f:(String.split ~on:':')
      |> List.map ~f:(function
           | [id; path] ->
               (Int.of_string id, path)
           | _ ->
               assert false))

let command =
  Core.Command.basic ~summary:"Ocaml_paxos"
    Core.Command.Let_syntax.(
      let%map_open public_address = anon ("public_address" %: network_address)
      and listen_address = anon ("listen_address" %: network_address)
      and data_path = anon ("data_path" %: string)
      and client_cap_file = anon ("client_cap_file" %: string)
      and node_id = anon ("node_id" %: int)
      and node_list = anon ("node_list" %: node_list)
      and election_timeout = anon ("election_timeout" %: float)
      and idle_timeout = anon ("idle_timeout" %: float) in
      fun () ->
        let secret_key = `File (data_path ^ ".key") in
        let log_path = data_path ^ ".log" in
        let term_path = data_path ^ ".term" in
        let cap_file =
          match List.Assoc.find node_list ~equal:Int.equal node_id with
          | Some file ->
              file
          | None ->
              eprintf "%d not found in node_list" node_id ;
              exit 1
        in
        Paxos.create ~public_address ~listen_address ~secret_key ~node_list
          ~election_timeout ~idle_timeout ~log_path ~term_path ~node_id
          ~cap_file ~client_cap_file
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
  Logs.(set_level (Some Debug)) ;
  (*Logs.Src.set_level Capnp_rpc.Debug.src (Some Info);*)
  Logs.set_reporter reporter ; Core.Command.run command

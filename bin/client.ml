(* main.ml *)
open Core
open Ocamlpaxos.Client

let node_list = Command.Arg_type.create @@ String.split ~on:','

let bytes = Command.Arg_type.create @@ Bytes.of_string

let print_res res =
  match%lwt res with
  | Ok `Success ->
      Printf.printf "Success\n" |> Lwt.return
  | Ok (`ReadSuccess s) ->
      Printf.printf "Read Success: %s\n" s |> Lwt.return
  | Error (`Msg s) ->
      Printf.printf "Failure of %s\n" s |> Lwt.return

let put =
  Command.basic ~summary:"Put operation"
    Command.Let_syntax.(
      let%map_open client_files = anon ("capacity_files" %: node_list)
      and key = anon ("key" %: bytes)
      and value = anon ("value" %: bytes) in
      fun () ->
        Lwt_main.run
        @@ let%lwt c = new_client ~client_files () in
           op_write c key value |> print_res)

let get =
  Command.basic ~summary:"Get operation"
    Command.Let_syntax.(
      let%map_open client_files = anon ("capacity_files" %: node_list)
      and key = anon ("key" %: bytes) in
      fun () ->
        Lwt_main.run
        @@ let%lwt c = new_client ~client_files () in
           op_read c key |> print_res)

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

(* Handle the command line arguments and run application is specified mode *)
let cmd =
  Command.group ~summary:"Cli client for Ocaml Paxos"
    [("put", put); ("get", get)]

let () =
  Lwt_engine.set (new Lwt_engine.libev ()) ;
  Fmt_tty.setup_std_outputs () ;
  Logs.(set_level (Some Info)) ;
  Logs.set_reporter reporter ; Core.Command.run cmd

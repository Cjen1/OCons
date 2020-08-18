(* main.ml *)
open Core
open Lwt.Infix
open Ocamlpaxos
open Client
open Unix_capnp_messaging

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

let bytes = Command.Arg_type.create @@ Bytes.of_string

let print_res = function
  | Ok `Success ->
      Printf.printf "Success\n" |> Lwt.return
  | Ok (`ReadSuccess s) ->
      Printf.printf "Read Success: %s\n" s |> Lwt.return
  | Error (`Msg s) ->
      Printf.printf "Failure of %s\n" s |> Lwt.return

let put =
  Command.basic ~summary:"Put operation"
    Command.Let_syntax.(
      let%map_open addresses = anon ("addresses" %: node_list)
      and key = anon ("key" %: bytes)
      and value = anon ("value" %: bytes) in
      fun () ->
        Random.self_init () ;
        Lwt_main.run
          ( new_client addresses ()
          >>= fun client -> op_write client key value >>= print_res ))

let get =
  Command.basic ~summary:"Get operation"
    Command.Let_syntax.(
      let%map_open addresses = anon ("addresses" %: node_list)
      and key = anon ("key" %: bytes) in
      fun () ->
        Random.self_init () ;
        Lwt_main.run
          ( new_client addresses ()
          >>= fun client -> op_read client key >>= print_res ))

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
  Stdlib.Random.self_init () ;
  Random.self_init () ;
  Lwt_engine.set (new Lwt_engine.libev ()) ;
  Fmt_tty.setup_std_outputs () ;
  Logs.(set_level (Some Debug)) ;
  Logs.set_reporter reporter ; Core.Command.run cmd

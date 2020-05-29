(* main.ml *)
open Core
open Lwt.Infix
open Ocamlpaxos
open Client

let node_list = 
  let parse_individual s =
    match Messaging.ConnUtils.addr_of_string s with
    | Ok addr -> addr
    | Error (`Msg e) -> 
        Fmt.failwith "Expected ip:port | path rather than %s " e
  in 
  let parse s = 
    String.split ~on:',' s 
    |> List.map ~f:parse_individual
  in 
  Command.Arg_type.create parse

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
      let%map_open addresses = anon ("capacity_files" %: node_list)
      and key = anon ("key" %: bytes)
      and value = anon ("value" %: bytes) in
      fun () ->
        Random.self_init ();
        Lwt_main.run begin
          new_client addresses () >>= fun client ->
          op_write 
            client key value >>= print_res
        end 
    )

let get =
  Command.basic ~summary:"Get operation"
    Command.Let_syntax.(
      let%map_open addresses = anon ("capacity_files" %: node_list)
      and key = anon ("key" %: bytes) in
      fun () ->
        Random.self_init ();
        Lwt_main.run begin
          new_client addresses () >>= fun client ->
          op_read client key >>= print_res
        end 
    )

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
  Logs.(set_level (Some Debug)) ;
  Logs.set_reporter reporter ; Core.Command.run cmd

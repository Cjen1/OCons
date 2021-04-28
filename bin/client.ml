open! Core
open! Async
module O = Ocons_core

let bytes = Command.Arg_type.create @@ Bytes.of_string

let network_address =
  let parse s =
    match String.split ~on:':' s with
    | [addr; port] ->
        (Fmt.str "%s:%s" addr port)
    | _ ->
        Fmt.failwith "Expected ip:port | path rather than %s " s
  in
  Command.Arg_type.create parse

let node_list =
  Command.Arg_type.comma_separated ~allow_empty:false network_address

let print_res res = res |> [%sexp_of: O.Types.op_result] |> Sexp.to_string_hum

let log_param =
  Log_extended.Command.(
    setup_via_params ~log_to_console_by_default:(Stderr Color)
      ~log_to_syslog_by_default:false ())

let put =
  Command.async_spec ~summary:"Put request"
    Command.Spec.(
      empty
      +> anon ("node list" %: node_list)
      +> anon ("key" %: bytes)
      +> anon ("value" %: bytes)
      +> log_param)
    (fun ps k v () () ->
      let global_level = Async.Log.Global.level () in
      let global_output = Async.Log.Global.get_output () in
      List.iter
        [ O.Utils.logger
        ; O.Client.logger
        ] ~f:(fun log ->
          Async.Log.set_level log global_level ;
          Async.Log.set_output log global_output ) ;
      let c = O.Client.new_client ps in
      let%map res = O.Client.op_write c ~k ~v in
      res |> print_res |> print_endline
      )

let get =
  Command.async_spec ~summary:"Get request"
    Command.Spec.(
      empty
      +> anon ("node list" %: node_list)
      +> anon ("key" %: bytes)
      +> log_param)
    (fun ps k () () ->
      let global_level = Async.Log.Global.level () in
      let global_output = Async.Log.Global.get_output () in
      List.iter
        [ O.Utils.logger
        ; O.Client.logger
        ] ~f:(fun log ->
          Async.Log.set_level log global_level ;
          Async.Log.set_output log global_output ) ;
      let c = O.Client.new_client ps in
      let%map res = O.Client.op_read c k in
      res |> print_res |> print_endline
      )

(* Handle the command line arguments and run application is specified mode *)
let cmd =
  Command.group ~summary:"Cli client for Ocaml Paxos"
    [("put", put); ("get", get)]

let () =
  Fmt_tty.setup_std_outputs () ;
  Fmt.pr "%a" (Fmt.array ~sep:Fmt.sp Fmt.string) (Sys.get_argv ()) ;
  Rpc_parallel.start_app cmd

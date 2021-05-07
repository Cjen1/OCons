open! Core
open! Async
module O = Ocons_core

let bytes = Command.Arg_type.create @@ Bytes.of_string

let network_address =
  let parse s =
    match String.split ~on:':' s with
    | [addr; port] ->
        Fmt.str "%s:%s" addr port
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
      +> anon ("node_list" %: node_list)
      +> anon ("key" %: bytes)
      +> anon ("value" %: bytes)
      +> log_param)
    (fun ps k v () () ->
      let global_level = Async.Log.Global.level () in
      let global_output = Async.Log.Global.get_output () in
      List.iter [O.Utils.logger; O.Client.logger] ~f:(fun log ->
          Async.Log.set_level log global_level ;
          Async.Log.set_output log global_output ) ;
      let c = O.Client.new_client ps in
      match%bind O.Client.op_write c ~k ~v with
      | ReadSuccess _ ->
          prerr_endline "Got read success on write operation" ;
          Async.exit 1
      | Failure s ->
          prerr_endline s ; Async.exit 1
      | Success ->
          Async.exit 0 )

let get =
  Command.async_spec ~summary:"Get request"
    Command.Spec.(
      empty
      +> anon ("node_list" %: node_list)
      +> anon ("key" %: bytes)
      +> log_param)
    (fun ps k () () ->
      let global_level = Async.Log.Global.level () in
      let global_output = Async.Log.Global.get_output () in
      List.iter [O.Utils.logger; O.Client.logger] ~f:(fun log ->
          Async.Log.set_level log global_level ;
          Async.Log.set_output log global_output ) ;
      let c = O.Client.new_client ps in
      let open O.Types in
      match%bind O.Client.op_read c k with
      | Success ->
          prerr_endline "Got write success on read operation" ;
          Async.exit 1
      | Failure s ->
          prerr_endline s ; Async.exit 1
      | ReadSuccess v ->
          print_endline v ; Async.exit 0 )

let cas =
  Command.async_spec ~summary:"Compare and Swap"
    Command.Spec.(
      empty
      +> anon ("node_list" %: node_list)
      +> anon ("key" %: bytes)
      +> anon ("value" %: bytes)
      +> anon ("new_value" %: bytes)
      +> log_param)
    (fun ps key value value' () () ->
      let global_level = Async.Log.Global.level () in
      let global_output = Async.Log.Global.get_output () in
      List.iter [O.Utils.logger; O.Client.logger] ~f:(fun log ->
          Async.Log.set_level log global_level ;
          Async.Log.set_output log global_output ) ;
      let c = O.Client.new_client ps in
      let open O.Types in
      match%bind O.Client.op_cas c ~key ~value ~value' with
      | ReadSuccess _ ->
          prerr_endline "Got read success on cas operation" ;
          Async.exit 1
      | Failure s ->
          prerr_endline s ; Async.exit 1
      | Success ->
          Async.exit 0 )

(* Handle the command line arguments and run application is specified mode *)
let cmd =
  Command.group ~summary:"Cli client for Ocaml Paxos"
    [("put", put); ("get", get); ("cas", cas)]

let () = Rpc_parallel.start_app cmd

open! Core
open! Async
open Ocamlpaxos

let network_address =
  let parse s =
    match String.split ~on:':' s with
    | [id; addr; port] ->
        (Int.of_string id, Fmt.str "%s:%s" addr port)
    | _ ->
        Fmt.failwith "Expected ip:port | path rather than %s " s
  in
  Command.Arg_type.create parse

let node_list =
  Command.Arg_type.comma_separated ~allow_empty:false network_address

let log_param =
  Log_extended.Command.(
    setup_via_params ~log_to_console_by_default:(Stderr Color)
      ~log_to_syslog_by_default:false ())

let command =
  Command.async_spec ~summary:"A Paxos implementation in OCaml"
    Command.Spec.(
      empty
      +> anon ("node_id" %: int)
      +> anon ("node_list" %: node_list)
      +> anon ("datadir" %: string)
      +> anon ("external_port" %: int)
      +> anon ("internal_port" %: int)
      +> anon ("election_timeout" %: int)
      +> anon ("tick_speed" %: float)
      +> flag "-s" ~doc:" Size limit of batches" (optional_with_default 1 int)
      +> flag "-d" ~doc:" Time limit of batches in ms"
           (optional_with_default 50. float)
      +> log_param)
    (fun node_id node_list datadir external_port internal_port election_timeout
         tick_speed batch_size batch_timeout () () ->
      let global_level = Async.Log.Global.level () in
      let global_output = Async.Log.Global.get_output () in
      List.iter
        [ Infra.logger
        ; Utils.logger
        ; Owal.logger
        ; Paxos_core.logger
        ; Paxos_core.io_logger
        ; Client_handler.logger ] ~f:(fun log ->
          Async.Log.set_level log global_level ;
          Async.Log.set_output log global_output ) ;
      let tick_speed = Time.Span.of_sec tick_speed in
      let batch_timeout = Time.Span.of_ms batch_timeout in
      let%bind () =
        match%bind Sys.file_exists_exn datadir with
        | true ->
            return ()
        | false ->
            Unix.mkdir datadir
      in
      let%bind (_ : Infra.t) =
        Infra.create ~node_id ~node_list ~datadir ~external_port ~internal_port
          ~election_timeout ~tick_speed ~batch_size ~batch_timeout
      in
      let i = Ivar.create () in
      Signal.handle Signal.terminating ~f:(fun _ -> Ivar.fill i ()) ;
      Ivar.read i )

let () =
  Fmt_tty.setup_std_outputs () ;
  Fmt.pr "%a" (Fmt.array ~sep:Fmt.sp Fmt.string) (Sys.get_argv ()) ;
  Rpc_parallel.start_app command

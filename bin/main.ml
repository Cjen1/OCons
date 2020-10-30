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
      +> anon ("listen_address" %: int)
      +> anon ("election_timeout" %: int)
      +> anon ("tick_speed" %: float)
      +> flag "-s" ~doc:" Size of batches" (optional_with_default 1 int)
      +> flag "-d" ~doc:" Time before batch is dispatched in ms"
           (optional_with_default 100. float)
      +> log_param)
    (fun node_id node_list datadir listen_port election_timeout tick_speed
         batch_size dispatch_timeout () () ->
      let global_level = Async.Log.Global.level () in
      let global_output = Async.Log.Global.get_output () in
      List.iter [Infra.logger; Utils.logger; Odbutils.Owal.logger]
        ~f:(fun log ->
          Async.Log.set_level log global_level ;
          Async.Log.set_output log global_output ;
          );
      let tick_speed = Time.Span.of_sec tick_speed in
      let dispatch_timeout = Time.Span.of_ms dispatch_timeout in
      let%bind () =
        match%bind Sys.file_exists_exn datadir with
        | true ->
            return ()
        | false ->
            Unix.mkdir datadir
      in
      let%bind (_ : Infra.t) =
        Infra.create ~node_id ~node_list ~datadir ~listen_port ~election_timeout
          ~tick_speed ~batch_size ~dispatch_timeout
      in
      Deferred.never ())

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
  Fmt_tty.setup_std_outputs () ;
  Logs.(set_level (Some Debug)) ;
  Logs.set_reporter reporter ;
  Fmt.pr "%a" (Fmt.array ~sep:Fmt.sp Fmt.string) (Sys.get_argv ()) ;
  Command.run command

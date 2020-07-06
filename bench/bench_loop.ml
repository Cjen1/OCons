open Ocamlpaxos
open Lwt.Infix

let src = Logs.Src.create "Bench"

module Log = (val Logs.src_log src : Logs.LOG)

let node_address =
  ( Int64.of_int 1
  , Unix_capnp_messaging.Conn_manager.addr_of_string "127.0.0.1:5000"
    |> Result.get_ok )

let log_path, term_path = ("tmp.log", "tmp.term")

let time_it f =
  let start = Unix.gettimeofday () in
  f () >>= fun () -> Unix.gettimeofday () -. start |> Lwt.return

let throughput n ?(debug = false) () =
  ( match debug with
  | false ->
      Log.info (fun m -> m "Setting up throughput test") ;
      Infra.create ~listen_address:(snd node_address) ~node_list:[node_address]
        ~election_timeout:5 ~tick_time:0.5 ~log_path ~term_path
        (fst node_address)
      >>= fun node ->
      let close () = 
        Infra.close node >|= fun () ->
        Unix.unlink log_path;
        Unix.unlink term_path
      in
      Lwt.return close
  | true ->
      let node =
        Unix_capnp_messaging.Conn_manager.create
          ~listen_address:(snd node_address) ~node_id:Int64.zero
          (fun cmgr src msg ->
            let open Messaging.API.Reader in
            let msg = ServerMessage.of_message msg in
            match ServerMessage.get msg with
            | ClientRequest cmd ->
                Messaging.Send.clientResponse cmgr ~sem:`AtLeastOnce src
                  ~id:(Command.id_get cmd) ~result:Types.StateMachine.Failure
            | _ ->
                assert false)
      in
      let close () = Unix_capnp_messaging.Conn_manager.close node in
      Lwt.return close )
  >>= fun close ->
  Client.new_client [node_address] ()
  >>= fun client ->
  let test_data = Bytes.of_string "asdf" in
  Client.op_write client test_data test_data
  >>= fun _ ->
  let stream =
    List.init n (fun _ -> Bytes.(of_string "asdf", of_string "asdf"))
    |> Lwt_stream.of_list
  in
  let max_concurrency = 100 in
  let test () =
    Lwt_stream.iter_n ~max_concurrency
      (fun (key, value) ->
        Client.op_write client key value
        >|= function
        | Error (`Msg s) ->
            Log.err (fun m -> m "Failed with %s during run" s)
        | Ok _ ->
            ())
      stream
  in
  Log.info (fun m -> m "Starting throughput test") ;
  time_it test
  >>= fun time ->
  Log.info (fun m -> m "Closing managers") ;
  Lwt.join [close (); Client.close client]
  >>= fun () ->
  Log.info (fun m -> m "Finished throughput test!") ;
  Fmt.str "Took %f to do %d operations: %f ops/s" time n
    Core.Float.(of_int n / time)
  |> Lwt.return

let reporter =
  let open Core in
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

(*
let () = 
  let buffer = MProf_unix.mmap_buffer ~size:1000000 "trace.ctf" in
  let trace_config = MProf.Trace.Control.make buffer MProf_unix.timestamper in
  MProf.Trace.Control.start trace_config
   *)

let () =
  Logs.(set_level ~all:true (Some Debug)) ;
  (*
  List.iter
    (fun src -> Logs.Src.set_level src (Some Info))
    [Unix_capnp_messaging.Conn_manager.src; Unix_capnp_messaging.Sockets.src] ;
    *)
  Logs.set_reporter reporter ;
  let res = Lwt_main.run @@ throughput 1000 ~debug:false () in
  print_endline res

open Ocamlpaxos
open Lwt.Infix

let log_src = Logs.Src.create "Bench"

module Log = (val Logs.src_log log_src : Logs.LOG)

let node_address p =
  ( Int64.of_int 1
  , Unix_capnp_messaging.Conn_manager.addr_of_string (Fmt.str "127.0.0.1:%d" p)
    |> Result.get_ok )

let log_path, term_path = ("tmp.log", "tmp.term")

let remove path =
  if Sys.file_exists path then
    let path = Fpath.of_string path |> Result.get_ok in
    Bos.OS.Dir.delete ~must_exist:false ~recurse:true path |> Result.get_ok
  else ()

let cleanup _ = remove log_path ; remove term_path

let time_it f =
  let start = Unix.gettimeofday () in
  f () >>= fun () -> Unix.gettimeofday () -. start |> Lwt.return

let throughput n request_batching p =
  Log.info (fun m -> m "Setting up throughput test") ;
  Client.new_client [node_address p] ()
  >>= fun client ->
  let test_data = Bytes.of_string "asdf" in
  Client.op_write client test_data test_data
  >>= fun _ ->
  let stream =
    List.init n (fun _ -> Bytes.(of_string "asdf", of_string "asdf"))
    |> Lwt_stream.of_list
  in
  let result_q = Queue.create () in
  let test () =
    Lwt_stream.iter_n ~max_concurrency:request_batching
      (fun (key, value) ->
        let start = Unix.gettimeofday () in
        Client.op_write client key value
        >|= function
        | Error (`Msg s) ->
            Log.err (fun m -> m "Failed with %s during run" s)
        | Ok _ ->
            let time = Unix.gettimeofday () -. start in
            Queue.add time result_q)
      stream
  in
  Log.info (fun m -> m "Starting throughput test") ;
  time_it test
  >>= fun time ->
  Log.info (fun m -> m "Closing managers") ;
  Client.close client
  >>= fun () ->
  Log.info (fun m -> m "Finished throughput test!") ;
  let res_str = Base.Float.(of_int n / time) in
  Lwt.return
    (res_str, request_batching, Queue.fold (fun ls e -> e :: ls) [] result_q)

type test_res = {throughput: float; batch: int; latencies: float array}

let server (p, _request_batching, close_flag) =
  let run () =
    Log.info (fun m ->
        m "Starting server on %a" Unix_capnp_messaging.Conn_manager.pp_addr
          (snd @@ node_address p)) ;
    Infra.create
      ~listen_address:(snd @@ node_address p)
      ~node_list:[node_address p] ~election_timeout:5 ~tick_time:0.5 ~log_path
      ~term_path
      (fst @@ node_address p)
      ~request_batching:0.001
    >>= fun node ->
    close_flag
    >>= fun () ->
    Log.info (fun m -> m "Closing server") ;
    Infra.close node
  in
  Lwt_preemptive.run_in_main run

let run (n, batch, p) =
  let inner =
    Log.info (fun m -> m "Running test for batch %d" batch) ;
    let close_flag, close_fulfiller = Lwt.task () in
    let server_t = Lwt_preemptive.detach server (p, batch, close_flag) in
    throughput n batch p
    >>= fun res ->
    Lwt.wakeup close_fulfiller () ;
    server_t >>= fun () -> cleanup () ; Lwt.return res
  in
  Lwt_main.run inner

let tests () =
  let process (throughput, batch, latencies) =
    let latencies = latencies |> Array.of_list in
    {throughput; batch; latencies}
  in
  let batch_sizes =
    if true then
      [ (50000, 1, 5001)
      ; (50000, 10, 5002)
      ; (50000, 100, 5003)
      ; (50000, 1000, 5004)
      ; (50000, 10000, 5005) ]
    else [(1000, 1, 5001)]
  in
  List.map run batch_sizes |> List.map process |> Lwt.return

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

(* test_res Fmt.t*)
let pp_stats =
  let open Owl.Stats in
  let open Fmt in
  let fields =
    [ field "batch" (fun s -> s.batch) int
    ; field "throughput" (fun s -> s.throughput) float
    ; field "mean" (fun s -> mean s.latencies) float
    ; field "p50" (fun stats -> percentile stats.latencies 50.) float
    ; field "p75" (fun stats -> percentile stats.latencies 75.) float
    ; field "p99" (fun stats -> percentile stats.latencies 99.) float ]
  in
  record fields

let () =
  cleanup () ;
  Logs.(set_level ~all:true (Some Info)) ;
  List.iter
    (fun src -> Logs.Src.set_level src (Some Info))
    [Unix_capnp_messaging.Conn_manager.src; Unix_capnp_messaging.Sockets.src] ;
  Lwt_unix.on_signal Sys.sigterm (fun _ -> cleanup () ; exit 0) |> ignore ;
  Logs.set_reporter reporter ;
  let res = try Lwt_main.run (tests ()) with e -> cleanup () ; raise e in
  Fmt.pr "%a" (Fmt.list pp_stats) res

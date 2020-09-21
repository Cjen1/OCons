open Ocamlpaxos
open Lwt.Infix

let log_src = Logs.Src.create "Bench"

module Log = (val Logs.src_log log_src : Logs.LOG)

let node_address p =
  ( Int64.of_int 1
  , Unix_capnp_messaging.Conn_manager.addr_of_string (Fmt.str "127.0.0.1:%d" p)
    |> Result.get_ok )

let time_it f =
  let start = Unix.gettimeofday () in
  f () >>= fun () -> Unix.gettimeofday () -. start |> Lwt.return

let client_counter = ref 0
let throughput n max_concurrency p =
  Log.info (fun m -> m "Setting up throughput test") ;
  Client.new_client ~cid:(incr client_counter; Int64.of_int !client_counter) [node_address p] ()
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
    Lwt_stream.iter_n ~max_concurrency
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
  Log.info (fun m -> m "Closing client") ;
  Client.close client
  >>= fun () ->
  Log.info (fun m -> m "Finished throughput test!") ;
  let throughput = Base.Float.(of_int n / time) in
  Lwt.return
    (throughput, max_concurrency, Queue.fold (fun ls e -> e :: ls) [] result_q)

type test_res = {throughput: float; concurrency: int; latencies: float array}

let run (n, batch, p) =
  Log.info (fun m -> m "Running test for batch %d" batch) ;
  Lwt_main.run (throughput n batch p)

let tests () =
  let process (throughput, concurrency, latencies) =
    let latencies = latencies |> Array.of_list in
    {throughput; concurrency; latencies}
  in
  let batch_sizes =
    if true then
      [ (10000, 1, 5001)
      ; (10000, 10, 5001)
      ; (10000, 100, 5001)
      ; (10000, 1000, 5001)
      ; (10000, 10000, 5001) ]
    else [(1, 1, 5001); (1,1,5001)]
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
    [ field "concurrency" (fun s -> s.concurrency) int
    ; field "throughput" (fun s -> s.throughput) float
    ; field "mean" (fun s -> mean s.latencies) float
    ; field "p50" (fun stats -> percentile stats.latencies 50.) float
    ; field "p75" (fun stats -> percentile stats.latencies 75.) float
    ; field "p99" (fun stats -> percentile stats.latencies 99.) float ]
  in
  record fields

let () =
  Logs.(set_level ~all:true (Some Info)) ;
  List.iter
    (fun src -> Logs.Src.set_level src (Some Info))
    [Unix_capnp_messaging.Conn_manager.src; Unix_capnp_messaging.Sockets.src] ;
  Logs.set_reporter reporter ;
  let res = Lwt_main.run (tests ()) in
  Fmt.pr "%a" (Fmt.list pp_stats) res

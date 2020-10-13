open! Core
open! Async
module O = Ocamlpaxos

let log_src = Logs.Src.create "Bench"

module Log = (val Logs.src_log log_src : Logs.LOG)

type test_res =
  { throughput: float
  ; starts: float array
  ; ends: float array
  ; latencies: float array }
[@@deriving yojson]

(* test_res Fmt.t*)
let pp_stats =
  let open Owl.Stats in
  let open Fmt in
  let fields =
    [ field "throughput" (fun s -> s.throughput) float
    ; field "mean" (fun s -> mean s.latencies) float
    ; field "p50" (fun stats -> percentile stats.latencies 50.) float
    ; field "p75" (fun stats -> percentile stats.latencies 75.) float
    ; field "p99" (fun stats -> percentile stats.latencies 99.) float ]
  in
  record fields

let throughput n batch_size ps =
  Log.info (fun m -> m "Setting up throughput test\n") ;
  let node_list = List.map ps ~f:(fun port -> port, Fmt.str "127.0.0.1:%d" port) in
  let client = O.Client.new_client (List.map node_list ~f:snd) in
  let%bind servers = 
    Deferred.List.map node_list ~how:`Parallel 
      ~f:(fun (id, _addr) -> 
          let datadir = Fmt.str "%d-%d.data" (List.length ps) id in
          let%bind () = 
            match%bind Sys.file_exists datadir with
            | `Yes -> return () 
            | _ -> Unix.mkdir datadir
          in 
          O.Infra.create ~node_id:id ~node_list ~datadir:(Fmt.str "%d-%d.data" (List.length ps) id)
            ~listen_port:id ~election_timeout:5 ~tick_speed:(Time.Span.of_sec 1.)
            ~batch_size ~dispatch_timeout:(Time.Span.of_ms 10.)
        )
  in
  let test = Bytes.of_string "test" in
  let%bind _ = O.Client.op_write client test test in
  let%bind stream =
    Deferred.Queue.init ~how:`Parallel n ~f:(fun _ -> return ())
  in
  let result_q = Queue.create () in
  Log.info (fun m -> m "Starting throughput test\n") ;
  let%bind () = Deferred.Queue.iteri stream ~how:(`Max_concurrent_jobs 1000) ~f:(fun i () ->
      if i % 100 = 0 then print_char '.';
      let start =
        Time.now () |> Time.to_span_since_epoch |> Time.Span.to_sec
      in
      let%bind _ = O.Client.op_write client test test in
     let ed = Time.now () |> Time.to_span_since_epoch |> Time.Span.to_sec in
     Queue.enqueue result_q (start, ed) ;
     return ())
  in
  Log.info (fun m -> m "Finished throughput test!\n") ;
  let results = Queue.to_array result_q in
  let min_start = Array.map ~f:fst results |> Owl_stats.min in
  let max_end = Array.map ~f:snd results |> Owl_stats.max in
  let throughput = Float.(of_int n / (max_end - min_start)) in
  let starts, ends = Array.unzip results in
  let latencies = Array.map ~f:(fun (st, ed) -> ed -. st) results in
  let%bind () = Deferred.List.iter servers ~how:`Parallel ~f:O.Infra.close in
  {throughput; starts; ends; latencies} |> return

let reporter =
  let report src level ~over k msgf =
    let k _ = over () ; k () in
    let src = Logs.Src.name src in
    msgf
    @@ fun ?header ?tags:_ fmt ->
    Fmt.kpf k Fmt.stdout
      ("%a %a @[" ^^ fmt ^^ "@]@.")
      Fmt.(styled `Magenta string)
      (Printf.sprintf "%14s" src)
      Logs_fmt.pp_header (level, header)
  in
  {Logs.report}

let main n batch_size output portss =
  let portss = List.map portss ~f:[%of_sexp: int list] in
  let perform () =
    let iter ports =
      let jsonpath =
        match output with None -> Fmt.str "%d.json" (List.length ports) | Some s -> s
      in
      let%bind res = throughput n batch_size ports in
      Log.info (fun m -> m "%a\n" pp_stats res) ;
      let json = test_res_to_yojson res in
      Yojson.Safe.to_file jsonpath json ;
      return ()
    in
    Deferred.List.iter ~f:iter portss
  in
  Logs.(set_level (Some Info)) ;
  Logs.set_reporter reporter ; perform ()

let () =
  Command.async_spec ~summary:"Benchmark for write ahead log"
    Command.Spec.(
      empty
      +> flag "-s" ~doc:" Size of batches" (optional_with_default 1 int)
      +> flag "-n" ~doc:" Number of requests to send"
           (optional_with_default 10000 int)
      +> flag "-o" ~doc:" Output file" (optional string)
      +> flag "-p" ~doc:" ports list" (listed sexp)
    )
    (fun s n o ps () -> main n s o ps)
  |> Command.run

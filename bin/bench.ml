open! Core
open! Async
module O = Ocons_core

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

let run_ts ts =
  let printi i _ = if i % 100 = 0 then print_char '.' in
  let ts = List.sort ts ~compare:(fun (_, a) (_, b) -> Time.compare a b) in
  let%bind res =
    Deferred.List.foldi ts ~init:[] ~f:(fun i acc (f, start) ->
        let%bind () = at start in
        let p = f () in
        upon p (printi i) ;
        return (p :: acc) )
  in
  let%bind res = res |> List.rev |> Deferred.List.all in
  return res

let get_not_failure r =
  match%map r with
  | O.Types.Failure s ->
      raise @@ Failure ("Got failure from operation" ^ s)
  | _ ->
      r

let run_latencies throughput n ps =
  Log.info (fun m -> m "Setting up latency test\n") ;
  let node_list =
    List.map ps ~f:(fun port -> (port, Fmt.str "127.0.0.1:%d" port))
  in
  let client = O.Client.new_client (List.map node_list ~f:snd) in
  let test = Bytes.of_string "test" in
  let%bind _ = O.Client.op_write client ~k:test ~v:test |> get_not_failure in
  let period = Float.(1. / throughput) in
  let start = Time.now () in
  let start = Time.(add start Span.(of_ms 500.)) in
  let ts =
    List.init n ~f:(fun i ->
        let wait = Float.(of_int i * period) |> Time.Span.of_sec in
        let start = Time.add start wait in
        let f () =
          let st =
            Time_ns.now () |> Time_ns.to_span_since_epoch |> Time_ns.Span.to_sec
          in
          let%bind _ =
            O.Client.op_write client ~k:test ~v:test |> get_not_failure
          in
          let ed =
            Time_ns.now () |> Time_ns.to_span_since_epoch |> Time_ns.Span.to_sec
          in
          return (st, ed)
        in
        (f, start) )
  in
  let%bind res = run_ts ts in
  let results = Array.of_list res in
  let min_start = Array.map ~f:fst results |> Owl_stats.min in
  let max_end = Array.map ~f:snd results |> Owl_stats.max in
  let throughput = Float.(of_int n / (max_end - min_start)) in
  let starts, ends = Array.unzip results in
  let latencies = Array.map ~f:(fun (st, ed) -> ed -. st) results in
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

let main target_throughput n output portss =
  let portss = List.map portss ~f:[%of_sexp: int list] in
  let perform () =
    let iter ports =
      let jsonpath = match output with None -> "data.json" | Some s -> s in
      let%bind res = run_latencies target_throughput n ports in
      Log.info (fun m -> m "") ;
      Log.info (fun m -> m "%a\n" pp_stats res) ;
      let json = test_res_to_yojson res in
      Yojson.Safe.to_file jsonpath json ;
      return ()
    in
    Deferred.List.iter ~f:iter portss
  in
  Logs.(set_level (Some Info)) ;
  Logs.set_reporter reporter ; perform ()

let log_param =
  Log_extended.Command.(
    setup_via_params ~log_to_console_by_default:(Stderr Color)
      ~log_to_syslog_by_default:false ())

let () =
  Command.async_spec ~summary:"Benchmark for main.ml"
    Command.Spec.(
      empty
      +> flag "-n" ~doc:" Number of requests to send"
           (optional_with_default 10000 int)
      +> flag "-o" ~doc:" Output file" (optional string)
      +> flag "-p" ~doc:" ports list" (listed sexp)
      +> flag "-t" ~doc:" Throughput" (optional_with_default 100000. float)
      +> log_param)
    (fun n o ps t () () ->
      let global_level = Async.Log.Global.level () in
      let global_output = Async.Log.Global.get_output () in
      List.iter [O.Client.logger] ~f:(fun log ->
          Async.Log.set_level log global_level ;
          Async.Log.set_output log global_output ) ;
      main t n o ps )
  |> Command.run

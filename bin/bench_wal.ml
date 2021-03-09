open! Core
open! Async

let src = Logs.Src.create "Bench"

module Log = (val Logs.src_log src : Logs.LOG)

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

module T_p = struct
  type t = bytes list

  let init () = []

  type op = Write of bytes [@@deriving bin_io]

  let apply t (Write b) = b :: t
end

module T = Odbutils.Owal.Persistant (T_p)

let throughput file n write_size =
  Log.info (fun m -> m "Setting up throughput test\n") ;
  let%bind wal, _t =
    T.of_path file ~file_size:Int64.(of_int Int.(1024 * 1024 * 128))
  in
  let stream =
    List.init n ~f:(fun _ -> Bytes.init write_size ~f:(fun _ -> 'c'))
    |> Async.Stream.of_list
  in
  let result_q = Queue.create () in
  Log.info (fun m -> m "Starting throughput test\n") ;
  let i = ref 0 in
  let%bind () =
    Stream.iter'
      ~f:(fun v ->
        let () =
          incr i ;
          if !i % 100 = 0 then print_char '.'
        in
        let start =
          Time.now () |> Time.to_span_since_epoch |> Time.Span.to_sec
        in
        T.write wal (T_p.Write v) ;
        let%bind () = T.datasync wal in
        let ed = Time.now () |> Time.to_span_since_epoch |> Time.Span.to_sec in
        Queue.enqueue result_q (start, ed) ;
        return () )
      stream
  in
  Log.info (fun m -> m "Finished throughput test!\n") ;
  let results = Queue.to_array result_q in
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

let main n write_sizes log output =
  let perform () =
    let iter write_size =
      let log =
        let prefix = match log with None -> "" | Some v -> v ^ "/" in
        Fmt.str "%s%d.wal" prefix write_size
      in
      let jsonpath =
        match output with None -> Fmt.str "%d.json" write_size | Some s -> s
      in
      let%bind res = throughput log n write_size in
      Log.info (fun m -> m "%a\n" pp_stats res) ;
      let json = test_res_to_yojson res in
      Yojson.Safe.to_file jsonpath json ;
      return ()
    in
    Deferred.List.iter ~f:iter write_sizes
  in
  Logs.(set_level (Some Info)) ;
  Logs.set_reporter reporter ; perform ()

let () =
  Command.async_spec ~summary:"Benchmark for write ahead log"
    Command.Spec.(
      empty
      +> flag "-s" ~doc:" Size of buffers" (listed int)
      +> flag "-n" ~doc:" Number of requests to send"
           (optional_with_default 10000 int)
      +> flag "-l" ~doc:" Log location prefix" (optional string)
      +> flag "-o" ~doc:" Output file" (optional string))
    (fun write_sizes n l o () -> main n write_sizes l o)
  |> Command.run

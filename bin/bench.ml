open! Ocons_core
open! Ocons_core.Types
module O = Ocons_core
module Cli = Ocons_core.Client
module MT = Eio.Time.Mono
open Eio.Std

let pitcher ~sw mclock n rate cmgr (dispatch : Mtime.t array) :
    unit Eio.Promise.t =
  let t, u = Promise.create () in
  let period =
    Mtime.Span.to_float_ns Mtime.Span.s /. rate
    |> Mtime.Span.of_float_ns |> Option.get
  in
  let req_reporter = O.Utils.InternalReporter.rate_reporter 0 "req_dispatch" in
  let rec aux = function
    | i, _ when i >= n ->
        ()
    | i, prev ->
        let cmd =
          Command.
            { op= Write ("asdf", "asdf")
            ; id= i
            ; trace_start= Unix.gettimeofday () }
        in
        let target = Mtime.add_span prev period |> Option.get in
        if Mtime.is_later target ~than:(MT.now mclock) then
          MT.sleep_until mclock target ;
        ( try Cli.submit_request cmgr cmd
          with e when O.Utils.is_not_cancel e ->
            traceln "Failed to dispatch %a" Fmt.exn_backtrace
              (e, Printexc.get_raw_backtrace ()) ) ;
        req_reporter () ;
        Array.set dispatch i (Eio.Time.Mono.now mclock) ;
        aux (i + 1, target)
  in
  Fiber.fork ~sw (fun () ->
      ( try aux (0, MT.now mclock)
        with e ->
          traceln "Failed to dispatch %a" Fmt.exn_backtrace
            (e, Printexc.get_raw_backtrace ()) ) ;
      Promise.resolve u () ; traceln "Pitcher complete" ) ;
  t

let to_float_ms s =
  Mtime.Span.to_float_ns s /. Mtime.Span.to_float_ns Mtime.Span.ms

let to_float_s =
  let min = Mtime.min_stamp in
  fun s ->
    let s = Mtime.span min s in
    Mtime.Span.to_float_ns s /. Mtime.Span.to_float_ns Mtime.Span.s

let latency_reporter = O.Utils.InternalReporter.avg_reporter Fun.id "lat"

let catcher_iter mclock requests responses (_, (cid, _, _)) =
  if Array.get responses cid |> Option.is_none then (
    let t = Eio.Time.Mono.now mclock in
    let latency = Mtime.span t (Array.get requests cid) |> to_float_ms in
    if latency > 500. then Magic_trace.take_snapshot () ;
    Array.set responses cid (Some t) ;
    latency_reporter latency )

let pp_stats ppf s =
  let s =
    let ends = Array.map (fun (_, (_, v)) -> to_float_s v) s in
    let lowest = Array.fold_left Float.min Float.max_float ends in
    let highest = Array.fold_left Float.max Float.min_float ends in
    let duration = highest -. lowest in
    let throughput = Float.div (Array.length ends |> Float.of_int) duration in
    let latencies =
      Array.map (fun (_, (s, e)) -> Mtime.span s e |> to_float_ms) s
    in
    let tdigest =
      Array.fold_left
        (fun a v -> Tdigest.add ~data:v a)
        (Tdigest.create ()) latencies
    in
    (throughput, latencies, tdigest)
  in
  let mean s =
    Array.fold_left ( +. ) 0. s /. (Array.length s |> Float.of_int)
  in
  let pp_stats =
    let open Fmt in
    let fields =
      [ field "throughput"
          (fun (t, _, _) -> t)
          (fun ppf v -> Fmt.pf ppf "%.1f" v)
      ; field "mean" (fun (_, s, _) -> mean s) (fun ppf v -> Fmt.pf ppf "%.1f" v)
      ; field "l_p50"
          (fun (_, _, s) -> Tdigest.percentile s 0.5 |> snd)
          (option float)
      ; field "l_p75"
          (fun (_, _, s) -> Tdigest.percentile s 0.75 |> snd)
          (option float)
      ; field "l_p99"
          (fun (_, _, s) -> Tdigest.percentile s 0.99 |> snd)
          (option float)
      ; field "l_max"
          (fun (_, s, _) -> Array.fold_left max Float.min_float s)
          float
      ; field "number" (fun (_, s, _) -> Array.length s) int ]
    in
    record fields
  in
  Fmt.pf ppf "%a" pp_stats s

let run sockaddrs id n rate outfile debug =
  let dispatch = Array.init n (fun _ -> Mtime.of_uint64_ns Int64.zero) in
  let response = Array.init n (fun _ -> None) in
  let ( / ) = Eio.Path.( / ) in
  if debug then Ocons_conn_mgr.set_debug_flag () ;
  let main env =
    Switch.run
    @@ fun sw ->
    O.Utils.InternalReporter.run ~sw env#clock 2. ;
    let con_ress =
      sockaddrs
      |> List.mapi (fun idx addr ->
             ( idx
             , fun sw -> (Eio.Net.connect ~sw env#net addr :> Eio.Flow.two_way)
             ) )
    in
    traceln "Creating conns to: %a"
      Fmt.(braces @@ list ~sep:comma Eio.Net.Sockaddr.pp)
      sockaddrs ;
    let cmgr =
      Cli.create_cmgr
        ~kind:
          (Ocons_conn_mgr.Iter (catcher_iter env#mono_clock dispatch response))
        ~sw con_ress id
        (fun () -> Eio.Time.sleep env#clock 1.)
    in
    let complete = pitcher ~sw env#mono_clock n rate cmgr dispatch in
    Promise.await complete ;
    Eio.Time.sleep env#clock 1. ;
    traceln "Test complete" ;
    (* End of test *)
    let request_response_pairs =
      let open Iter.Infix in
      0 -- (n - 1)
      |> Iter.filter (fun i -> Array.get response i |> Option.is_some)
      |> Iter.map (fun i ->
             (i, (Array.get dispatch i, Array.get response i |> Option.get)) )
    in
    let responses =
      request_response_pairs |> Iter.to_seq_persistent |> Array.of_seq
    in
    traceln "Results: %a" pp_stats responses ;
    outfile
    |> Option.iter (fun path ->
           let path = Eio.Stdenv.cwd env / path in
           Eio.Path.with_open_out ~create:(`If_missing 0o777) path
           @@ fun out ->
           Eio.Buf_write.with_flow out
           @@ fun bw ->
           traceln "Saving" ;
           Eio.Buf_write.string bw "[" ;
           request_response_pairs
           |> Iter.map (fun (rid, (tx, rx)) () ->
                  Eio.Buf_write.string bw
                  @@ Fmt.str "{\"rid\": %d, \"tx\": %a, \"rx\": %a}" rid
                       Mtime.pp tx Mtime.pp rx )
           |> Iter.intersperse (fun () -> Eio.Buf_write.string bw ",\n")
           |> Iter.iter (fun f -> f ()) ;
           Eio.Buf_write.string bw "]" ;
           traceln "Saved" ) ;
    traceln "Closing everything" ;
    Eio.Time.sleep env#clock 1. ;
    Cli.Cmgr.close cmgr ;
    traceln "Closed everything, done bench"
  in
  Eio_unix.Ctf.with_tracing "trace.ctf"
  @@ fun () ->
  try Eio_main.run main
  with e -> Fmt.pr "%a" Fmt.exn_backtrace (e, Printexc.get_raw_backtrace ())

open Cmdliner

let ipv4 =
  let conv = Arg.(t4 ~sep:'.' int int int int) in
  let parse s =
    let ( let+ ) = Result.bind in
    let+ res = Arg.conv_parser conv s in
    let check v = v >= 0 && v < 256 in
    match res with
    | v0, v1, v2, v3 when check v0 && check v1 && check v2 && check v3 ->
        let raw = Bytes.create 4 in
        Bytes.set_uint8 raw 0 v0 ;
        Bytes.set_uint8 raw 1 v1 ;
        Bytes.set_uint8 raw 2 v2 ;
        Bytes.set_uint8 raw 3 v3 ;
        Ok (Eio.Net.Ipaddr.of_raw (Bytes.to_string raw))
    | v0, v1, v2, v3 ->
        Error
          (`Msg
            Fmt.(
              str "Invalid IP address: %a"
                (list ~sep:(const string ".") int)
                [v0; v1; v2; v3] ) )
  in
  Arg.conv ~docv:"IPv4" (parse, Eio.Net.Ipaddr.pp)

let sockv4 =
  let conv = Arg.(pair ~sep:':' ipv4 int) in
  let parse s =
    let ( let+ ) = Result.bind in
    let+ ip, port = Arg.conv_parser conv s in
    Ok (`Tcp (ip, port) : Eio.Net.Sockaddr.stream)
  in
  Arg.conv ~docv:"TCP" (parse, Eio.Net.Sockaddr.pp)

let cmd =
  let info = Cmd.info "bench" in
  let id_t =
    Arg.(
      required
      & pos 0 (some int) None (info ~docv:"ID" ~doc:"The id of the client" []) )
  in
  let sockaddrs_t =
    Arg.(
      required
      & pos 1
          (some @@ list sockv4)
          None
          (info ~docv:"SOCKADDRS"
             ~doc:
               "This is a comma separated list of ip addresses and ports eg: \
                \"192.168.0.1:5000,192.168.0.2:5000\""
             [] ) )
  in
  let n_t =
    Arg.(
      value
      & opt int 1000
          (info ~docv:"N" ~doc:"Number of requests" ["n"; "number-requests"]) )
  in
  let rate_t =
    Arg.(
      value
      & opt float 100. (info ~docv:"RATE" ~doc:"Rate of requests" ["r"; "rate"]) )
  in
  let file_t =
    Arg.(
      value
      & opt (some string) None
          (info ~docv:"PATH" ~doc:"Output the raw result to file" ["p"]) )
  in
  let debug_t =
    Arg.(value & flag (info ~docv:"DEBUG" ~doc:"Debug print flag" ["d"]))
  in
  Cmd.v info
    Term.(const run $ sockaddrs_t $ id_t $ n_t $ rate_t $ file_t $ debug_t)

let () = exit Cmd.(eval @@ cmd)

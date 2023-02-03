open! Ocons_core
open! Ocons_core.Types
module Cli = Ocons_core.Client
open Eio.Std

let pitcher ~sw clock n rate cmgr dispatch : unit Eio.Promise.t =
  let t, u = Promise.create () in
  let period = Float.div 1. rate in
  let rec aux = function
    | i, _ when i >= n ->
        ()
    | i, prev ->
        let cmd = Command.{op= Write ("asdf", "asdf"); id= i} in
        let target = Float.add prev period in
        Eio.Time.sleep_until clock target ;
        Cli.submit_request cmgr cmd ;
        Hashtbl.add dispatch i (Eio.Time.now clock) ;
        aux (i + 1, target)
  in
  Fiber.fork ~sw (fun () ->
      let start = Eio.Time.now clock in
      aux (0, start) ;
      Promise.resolve u () ) ;
  t

let catcher cmgr resp clock =
  while true do
    Fiber.check () ;
    let res = Cli.recv_resp ~force:false cmgr in
    res |> Iter.iter (fun (id, _) -> Hashtbl.add resp id (Eio.Time.now clock)) ;
    Fiber.yield ()
  done ;
  assert false

let pp_stats ppf s =
  let s =
    let ends = Array.map (fun (_, (_, v)) -> v) s in
    let lowest = Array.fold_left Float.min Float.max_float ends in
    let highest = Array.fold_left Float.max Float.min_float ends in
    let throughput =
      Float.div (Array.length ends |> Float.of_int) (Float.sub highest lowest)
    in
    let latencies = Array.map (fun (_, (s, e)) -> Float.sub e s) s in
    (throughput, latencies)
  in
  let pp_stats =
    let open Fmt in
    let open Owl.Stats in
    let fields =
      [ field "throughput" (fun (t, _) -> t) float
      ; field "mean" (fun (_, s) -> mean s) float
      ; field "l_p50" (fun (_, s) -> percentile s 50.) float
      ; field "l_p75" (fun (_, s) -> percentile s 75.) float
      ; field "l_p99" (fun (_, s) -> percentile s 99.) float
      ; field "number" (fun (_, s) -> Array.length s) int ]
    in
    record fields
  in
  Fmt.pf ppf "%a" pp_stats s

let run sockaddrs id n rate =
  let dispatch = Hashtbl.create n in
  let response = Hashtbl.create n in
  let main env =
    Switch.run
    @@ fun sw ->
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
    let cmgr = Cli.create_cmgr ~sw con_ress id (fun () -> Eio.Time.sleep env#clock 1.) in
    let complete = pitcher ~sw env#clock n rate cmgr dispatch in
    Fiber.fork_daemon ~sw (fun () -> catcher cmgr response env#clock) ;
    Promise.await complete ;
    Eio.Time.sleep env#clock 1.;
    Cli.Cmgr.close cmgr
  in
  Eio_main.run main ;
  Fmt.pr "Done bench\n" ;
  let request_response_pairs = Hashtbl.create n in
  let add_entries_iter id resp =
    let dis = Hashtbl.find dispatch id in
    Hashtbl.add request_response_pairs id (dis, resp)
  in
  Hashtbl.iter add_entries_iter response ;
  let responses = request_response_pairs |> Hashtbl.to_seq |> Array.of_seq in
  Fmt.pr "Results: %a" pp_stats responses

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
  Cmd.v info Term.(const run $ sockaddrs_t $ id_t $ n_t $ rate_t)

let () = exit Cmd.(eval @@ cmd)

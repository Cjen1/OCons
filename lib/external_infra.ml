open! Types
open Eio.Std
open! Utils

let request_yield_energy = 16

let response_flush_energy = 16

let result_yield_energy = 128

let dtraceln = Utils.dtraceln

type request = Line_prot.External_infra.request

type response = Line_prot.External_infra.response

type socket_responder = {sw: Switch.t; bw: Eio.Buf_write.t; mf: unit -> unit}

let to_float_ms span =
  let open Mtime.Span in
  to_float_ns span /. to_float_ns ms

type t =
  { conn_tbl: (client_id, socket_responder) Hashtbl.t
  ; req_tbl: (command_id, client_id) Hashtbl.t
  ; cmd_str: request Eio.Stream.t
  ; res_str: response Eio.Stream.t
  ; req_reporter: unit InternalReporter.reporter }

let accept_handler t sock addr =
  dtraceln "Accepted conn from: %a" Eio.Net.Sockaddr.pp addr ;
  Switch.run
  @@ fun sw ->
  let br = Eio.Buf_read.of_flow ~max_size:8192 sock in
  let cid = Eio.Buf_read.BE.uint64 br |> Int64.to_int in
  dtraceln "Setting up conns for %d" cid ;
  (* If an error occurs, remove the conn and then drain it
     This ensures that pending writes to the stream are flushed
     thus preventing deadlock
  *)
  Switch.on_release sw (fun () -> Hashtbl.remove t.conn_tbl cid) ;
  let request_fiber () =
    let maybe_yield = Utils.maybe_yield ~energy:request_yield_energy in
    while true do
      Fiber.check () ;
      dtraceln "Waiting for request from: %d" cid ;
      let r = Line_prot.External_infra.parse_request br in
      t.req_reporter () ;
      dtraceln "Got request from %d: %a" cid Command.pp r ;
      Hashtbl.add t.req_tbl r.id cid ;
      Eio.Stream.add t.cmd_str r ;
      maybe_yield ()
    done
  in
  let result_fiber () =
    Eio.Buf_write.with_flow sock
    @@ fun bw ->
    let mf =
      Utils.maybe_do ~energy:response_flush_energy ~f:(fun () ->
          Eio.Buf_write.flush bw )
    in
    let socket_responder = {sw; bw; mf} in
    Switch.on_release sw (fun () -> Hashtbl.remove t.conn_tbl cid) ;
    Hashtbl.replace t.conn_tbl cid socket_responder ;
    Fiber.await_cancel ()
  in
  try Fiber.both request_fiber result_fiber with
  | End_of_file | Eio.Exn.Io _ ->
      traceln "Connection closed"
  | e when is_not_cancel e ->
      traceln "Client handler failed with %a" Fmt.exn_backtrace
        (e, Printexc.get_raw_backtrace ())

let slow_result_check trace (clock : #Eio.Time.clock) reporter =
  let st = trace in
  if st != -1. then (
    let ed = Eio.Time.now clock in
    let diff = ed -. st in
    let delay_ms = diff *. 1000. in
    reporter delay_ms ;
    if delay_ms > 500. then (*Magic_trace.take_snapshot*) () )

let run (net : #Eio.Net.t) (clock : #Eio.Time.clock) port cmd_str res_str =
  Switch.run
  @@ fun sw ->
  let req_reporter = InternalReporter.rate_reporter 0 "cli_req" in
  let t =
    { conn_tbl= Hashtbl.create 16
    ; req_tbl= Hashtbl.create 4096
    ; cmd_str
    ; res_str
    ; req_reporter }
  in
  let accept_handler = accept_handler t in
  let addr = `Tcp (Eio.Net.Ipaddr.V4.any, port) in
  let sock = Eio.Net.listen ~backlog:4 ~sw net addr in
  let server_fiber () =
    Eio.Net.run_server
      ~on_error:(function
        | e when Utils.is_not_cancel e ->
            traceln "Client sock exception: %a" Fmt.exn_backtrace
              (e, Printexc.get_raw_backtrace ())
        | e ->
            raise e )
      sock accept_handler
  in
  let result_fiber () =
    let reporter =
      InternalReporter.avg_reporter Fun.id "internal->external latency"
    in
    let yielder = Utils.maybe_yield ~energy:result_yield_energy in
    (* Guaranteed to get at most one result per registered request *)
    while true do
      let cycle_timer = Mtime_clock.counter () in
      dtraceln "Waiting for response" ;
      let cid, res, trace = Eio.Stream.take res_str in
      slow_result_check trace clock reporter ;
      dtraceln "Got response for %d" cid ;
      let _try_send_response =
        let ( let* ) m f = Option.iter f m in
        let* conn_id = Hashtbl.find_opt t.req_tbl cid in
        let* {sw; bw; mf= maybe_flush} = Hashtbl.find_opt t.conn_tbl conn_id in
        dtraceln "Responding to %d for %d" conn_id cid ;
        try
          (* reply to client *)
          Line_prot.External_infra.serialise_response
            (cid, res, Eio.Time.now clock)
            bw ;
          maybe_flush ()
        with e when Utils.is_not_cancel e ->
          Eio.Fiber.fork ~sw (fun () -> Switch.fail sw e)
      in
      (* TODO check overhead for this*)
      Hashtbl.remove t.req_tbl cid ;
      yielder () ;
      let elapsed = Mtime_clock.count cycle_timer |> to_float_ms in
      if elapsed > 300. then Magic_trace.take_snapshot ()
    done
  in
  Fiber.both result_fiber server_fiber ;
  traceln "Closed external infra"

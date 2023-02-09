open! Types
open Eio.Std
open! Utils

let dtraceln = Utils.dtraceln

type request = Line_prot.External_infra.request

type response = Line_prot.External_infra.response

type t =
  { conn_tbl: (client_id, response Eio.Stream.t) Hashtbl.t
  ; req_tbl: (command_id, client_id) Hashtbl.t
  ; cmd_str: request Eio.Stream.t
  ; res_str: response Eio.Stream.t
  ; req_reporter: unit InternalReporter.reporter}

let rec drain str =
  match Eio.Stream.take_nonblocking str with Some _ -> drain str | None -> ()

let accept_handler t sock addr =
  dtraceln "Accepted conn from: %a" Eio.Net.Sockaddr.pp addr ;
  Switch.run
  @@ fun sw ->
  let br = Eio.Buf_read.of_flow ~max_size:8192 sock in
  let cid = Eio.Buf_read.BE.uint64 br |> Int64.to_int in
  dtraceln "Setting up conns for %d" cid ;
  let res_str = Eio.Stream.create 16 in
  (* If an error occurs, remove the conn and then drain it
     This ensures that pending writes to the stream are flushed
     thus preventing deadlock
  *)
  Switch.on_release sw (fun () ->
      Hashtbl.remove t.conn_tbl cid ;
      drain res_str ) ;
  Hashtbl.replace t.conn_tbl cid res_str ;
  let request_fiber () =
    while true do
      Fiber.check () ;
      dtraceln "Waiting for request from: %d" cid ;
      let r = Line_prot.External_infra.parse_request br in
      t.req_reporter () ;
      dtraceln "Got request from %d: %a" cid Command.pp r ;
      Hashtbl.add t.req_tbl r.id cid ;
      Eio.Stream.add t.cmd_str r
    done
  in
  let result_fiber () =
    Eio.Buf_write.with_flow sock
    @@ fun bw ->
    while true do
      Fiber.check () ;
      let res = Eio.Stream.take res_str in
      dtraceln "Got response for %d: %a" cid
        Fmt.(pair ~sep:comma int op_result_pp)
        res ;
      Line_prot.External_infra.serialise_response res bw ;
      dtraceln "Sent response for %d" cid
    done
  in
  try Fiber.both request_fiber result_fiber
  with 
  | End_of_file | Eio.Exn.Io _ -> traceln "Connection closed"
  | e when is_not_cancel e ->
      traceln "Client handler failed with %a" Fmt.exn_backtrace
        (e, Printexc.get_raw_backtrace ())

let run (net : #Eio.Net.t) port cmd_str res_str =
  Switch.run
  @@ fun sw ->
  let req_reporter =
    InternalReporter.rate_reporter 0 "cli_req"
  in
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
    let yielder = Utils.maybe_yield ~energy:128 in
    (* Guaranteed to get at most one result per registered request *)
    while true do
      dtraceln "Waiting for response" ;
      let ((cid, _) as res) = Eio.Stream.take res_str in
      dtraceln "Got response for %d" cid ;
      (let ( let* ) m f = Option.iter f m in
       let* conn_id = Hashtbl.find_opt t.req_tbl cid in
       let* conn = Hashtbl.find_opt t.conn_tbl conn_id in
       dtraceln "Passing response for %d to %d" cid conn_id ;
       Eio.Stream.add conn res ;
       yielder () ) ;
      Hashtbl.remove t.req_tbl cid
    done
  in
  Fiber.both result_fiber server_fiber;
  traceln "Closed external infra"

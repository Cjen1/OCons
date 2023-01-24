open! Types
open Eio.Std

let dtraceln = Utils.dtraceln

type request = Line_prot.External_infra.request

type response = Line_prot.External_infra.response

type t =
  { conn_tbl: (int, response Eio.Stream.t) Hashtbl.t
  ; req_tbl: (int, int) Hashtbl.t
  ; cmd_str: request Eio.Stream.t
  ; res_str: response Eio.Stream.t }

let rec drain str =
  match Eio.Stream.take_nonblocking str with Some _ -> drain str | None -> ()

let accept_handler t sock addr =
  dtraceln "Accepted conn from: %a" Eio.Net.Sockaddr.pp addr ;
  Switch.run
  @@ fun sw ->
  let br = Eio.Buf_read.of_flow ~max_size:8192 sock in
  let id = Eio.Buf_read.BE.uint64 br |> Int64.to_int in
  dtraceln "Setting up conns for %d" id;
  let res_str = Eio.Stream.create 16 in
  (* If an error occurs, remove the conn and then drain it
     This ensures that pending writes to the stream are flushed
     thus preventing deadlock
  *)
  Switch.on_release sw (fun () ->
      Hashtbl.remove t.conn_tbl id ;
      drain res_str ) ;
  Hashtbl.add t.conn_tbl id res_str ;
  (* request fiber *)
  Fiber.fork ~sw (fun () ->
      while true do
        dtraceln "Waiting for request from: %d" id ;
        let r = Line_prot.External_infra.parse_request br in
        dtraceln "Got request from %d: %a" id Command.pp r ;
        Eio.Stream.add t.cmd_str r
      done ) ;
  (* result fiber *)
  Fiber.fork ~sw (fun () ->
      Eio.Buf_write.with_flow sock
      @@ fun bw ->
      while true do
        let res = Eio.Stream.take res_str in
        dtraceln "Got response for %d: %a" id
          Fmt.(pair ~sep:comma int op_result_pp)
          res ;
        Line_prot.External_infra.serialise_response res bw
      done )

let run (net : #Eio.Net.t) port cmd_str res_str =
  Switch.run
  @@ fun sw ->
  let t =
    {conn_tbl= Hashtbl.create 8; req_tbl= Hashtbl.create 4096; cmd_str; res_str}
  in
  let accept_handler = accept_handler t in
  let addr = `Tcp (Eio.Net.Ipaddr.V4.any, port) in
  let sock = Eio.Net.listen ~backlog:4 ~sw net addr in
  (* Pass results back to the client *)
  Fiber.fork ~sw (fun () ->
      (* Guaranteed to get at most one result per registered request *)
      while true do
        let ((cid, _) as res) = Eio.Stream.take res_str in
        dtraceln "Got response for %d" cid ;
        (let ( let* ) m f = Option.iter f m in
         let* conn_id = Hashtbl.find_opt t.req_tbl cid in
         let* conn = Hashtbl.find_opt t.conn_tbl conn_id in
         dtraceln "Passing response for %d to %d" cid conn_id ;
         Eio.Stream.add conn res ) ;
        Hashtbl.remove t.req_tbl cid
      done ) ;
  while true do
    Eio.Net.accept_fork ~sw sock
      ~on_error:(fun e -> Fmt.pr "Client sock exception: %a\n" Fmt.exn e)
      accept_handler
  done

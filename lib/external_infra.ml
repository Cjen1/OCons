open! Types
open Eio.Std

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
  traceln "Accepted conn from: %a" Eio.Net.Sockaddr.pp addr ;
  Switch.run
  @@ fun sw ->
  let br = Eio.Buf_read.of_flow ~max_size:8192 sock in
  let id = Eio.Buf_read.BE.uint64 br |> Int64.to_int in
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
        let r = Line_prot.External_infra.parse_request br in
        Eio.Stream.add t.cmd_str r
      done ) ;
  (* result fiber *)
  Fiber.fork ~sw (fun () ->
      Eio.Buf_write.with_flow sock
      @@ fun bw ->
      while true do
        let res = Eio.Stream.take res_str in
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
        let conn_id = Hashtbl.find_opt t.req_tbl cid in
        let conn =
          Option.bind conn_id (fun conn_id ->
              Hashtbl.find_opt t.conn_tbl conn_id )
        in
        Option.fold ~none:() ~some:(fun conn -> Eio.Stream.add conn res) conn ;
        Hashtbl.remove t.req_tbl cid
      done ) ;
  while true do
    Eio.Net.accept_fork ~sw sock
      ~on_error:(fun e -> Fmt.pr "Client sock exception: %a\n" Fmt.exn e)
      accept_handler
  done

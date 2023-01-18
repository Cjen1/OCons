open! Types
open! Eio.Std
module Cmgr = Ocons_conn_mgr

type request = Line_prot.External_infra.request

type response = Line_prot.External_infra.response

type cmgr = response Cmgr.t

let ser_req req = Line_prot.External_infra.serialise_request req

let parse_resp = Line_prot.External_infra.parse_response

let resolver_with_handshake ~id (res : Cmgr.resolver) () =
  let f = res () in
  Eio.Buf_write.with_flow f (fun bw ->
      Eio.Buf_write.BE.uint64 bw (Int64.of_int id) ) ;
  f

(* Add the handshake *)
let create_cmgr ~sw resolvers id =
  let resolvers =
    resolvers |> List.map (fun (i, r) -> (i, resolver_with_handshake ~id r))
  in
  Cmgr.create ~sw resolvers parse_resp

let submit_request cmgr req = Cmgr.broadcast_blit cmgr (ser_req req)

let recv_resp cmgr = Cmgr.recv_any cmgr |> Iter.map snd

type request_state =
  {resolver: op_result Promise.u; retry: unit -> unit; mutable last_sent: float}

type t =
  { cmgr: cmgr
  ; clock: Eio.Time.clock
  ; request_state: (command_id, request_state) Hashtbl.t
  ; next_id: unit -> command_id }

let get_command_id id lid = (lid * 16) + id

let create_rpc ~sw env resolvers id retry_period =
  assert (id < 16) ;
  let next_cid =
    let internal = ref 0 in
    fun () ->
      let next = !internal in
      internal := next + 1 ;
      get_command_id id next
  in
  let t =
    { cmgr= create_cmgr ~sw resolvers id
    ; request_state= Hashtbl.create 1024
    ; clock= Eio.Stdenv.clock env
    ; next_id= next_cid }
  in
  (* retry any missing requests *)
  Eio.Fiber.fork_daemon ~sw (fun () ->
      while true do
        let now = Eio.Time.now (Eio.Stdenv.clock env) in
        t.request_state
        |> Hashtbl.iter (fun _ rstate ->
               if Float.(add rstate.last_sent retry_period > now) then (
                 rstate.last_sent <- now ;
                 rstate.retry () ) ) ;
        Eio.Time.sleep (Eio.Stdenv.clock env) (retry_period /. 2.)
      done ;
      assert false ) ;
  (* Resolve any incoming results *)
  Eio.Fiber.fork_daemon ~sw (fun () ->
      while true do
        let resps = recv_resp t.cmgr in
        resps
        |> Iter.iter (fun (id, res) ->
               match Hashtbl.find_opt t.request_state id with
               | None ->
                   ()
               | Some s ->
                   Promise.resolve s.resolver res ;
                   Hashtbl.remove t.request_state id )
      done ;
      assert false ) ;
  t

let send_request t op =
  let command = Command.{op; id= t.next_id ()} in
  let send () = submit_request t.cmgr command in
  let res_t, res_u = Promise.create () in
  let request_state =
    {resolver= res_u; retry= send; last_sent= Eio.Time.now t.clock}
  in
  Hashtbl.add t.request_state command.id request_state ;
  send () ;
  Promise.await res_t

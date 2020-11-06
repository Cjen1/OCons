open! Core
open! Async
open! Ppx_log_async
module P = Paxos_core
module O = Owal
module H = Hashtbl
module L = Types.Wal.Log
module T = Types.Wal.Term
open Types
open Types.MessageTypes
open! Utils

let debug_no_sync = false

let logger =
  let open Async_unix.Log in
  create ~level:`Info ~output:[] ~on_error:`Raise
    ~transform:(fun m -> Message.add_tags m [("src", "Infra")])
    ()

type t =
  { mutable core: P.t
  ; mutable server: (Socket.Address.Inet.t, int) Tcp.Server.t Ivar.t
  ; wal: Wal.t
  ; mutable last_applied: log_index
  ; conns: (int, Async_rpc_kernel.Persistent_connection.Rpc.t) H.t
  ; state_machine: state_machine
  ; client_ivars: (command_id, client_response Ivar.t list) H.t
  ; client_request_batcher: (client_request * client_response Ivar.t) Batcher.t
  ; client_results: (command_id, op_result) H.t }

let rec advance_state_machine t = function
  | commit_index when Int64.(t.last_applied < commit_index) ->
      let index = Int64.(t.last_applied + one) in
      t.last_applied <- index ;
      let entry = L.get_exn (P.get_log t.core) index in
      let result = update_state_machine t.state_machine entry.command in
      H.set t.client_results ~key:entry.command.id ~data:result ;
      let ivars = H.find_multi t.client_ivars entry.command.id in
      List.iter ivars ~f:(fun ivar ->
          [%log.debug
            logger "Resolving" ((entry.command.id, index) : Id.t * int64)] ;
          Ivar.fill ivar result) ;
      advance_state_machine t commit_index
  | _ ->
      ()

let get_ok v =
  match v with Error (`Msg s) -> raise @@ Invalid_argument s | Ok v -> v

let send : type query. t -> int -> query Rpc.One_way.t -> query -> unit =
 fun t target rpc msg ->
  let conn = H.find_exn t.conns target in
  let handler conn =
    match Rpc.One_way.dispatch' rpc conn msg with
    | Ok () ->
        ()
    | Error e ->
        Async_rpc_kernel.Rpc_error.raise e (Info.of_string "send request")
  in
  let connected = Async_rpc_kernel.Persistent_connection.Rpc.connected conn in
  match Deferred.peek connected with
  | Some conn ->
      handler conn
  | None ->
      upon connected handler

let do_pre t pre =
  List.iter pre ~f:(function
    | `PersistantChange (`Log op) ->
        Wal.write t.wal (Log op)
    | `PersistantChange (`Term op) ->
        Wal.write t.wal (Term op)
    | `SendRequestVote (id, rv) ->
        send t id Types.RPCs.request_vote rv
    | `SendAppendEntries (id, ae) ->
        send t id Types.RPCs.append_entries ae
    | `Unapplied _cmds ->
        ())

let do_post t post =
  List.iter post ~f:(function
    | `CommitIndexUpdate index ->
        advance_state_machine t index
    | `SendRequestVoteResponse (id, rvr) ->
        send t id Types.RPCs.request_vote_response rvr
    | `SendAppendEntriesResponse (id, aer) ->
        send t id Types.RPCs.append_entries_response aer)

let rec do_actions t ((pre, do_sync, post) : P.action_sequence) =
  do_pre t pre ;
  match do_sync && not debug_no_sync with
  | true ->
      let%map () = Wal.datasync t.wal in
      do_post t post
  | false ->
      do_post t post |> return

and handle_event t event =
  let core, actions = P.advance t.core event |> get_ok in
  t.core <- core ;
  do_actions t actions

let advance_wrapper t event =
  let core, actions = P.advance t.core event |> get_ok in
  t.core <- core ;
  actions

let handle_client_requests t
    (req_ivar_list : (command, Types.op_result Ivar.t) List.Assoc.t) =
  let pre, sync, post =
    advance_wrapper t (`Commands (List.map req_ivar_list ~f:fst))
  in
  let () =
    match List.find pre ~f:(function `Unapplied _ -> true | _ -> false) with
    | Some (`Unapplied cmds) ->
        let req_list = List.map req_ivar_list ~f:(fun (cr, i) -> (cr.id, i)) in
        let ivar_lookup =
          H.of_alist_exn ~growth_allowed:false (module Id) req_list
        in
        List.iter cmds ~f:(fun cmd ->
            let ivar = H.find_exn ivar_lookup cmd.id in
            Ivar.fill ivar Types.Failure)
    | _ ->
        ()
  in
  List.iter req_ivar_list ~f:(fun (cmd, ivar) ->
      if Ivar.is_empty ivar then
        H.add_multi t.client_ivars ~key:cmd.id ~data:ivar) ;
  do_actions t (pre,sync,post)

let server_impls =
  let dispatch_client_request t m i =
    Batcher.dispatch t.client_request_batcher (m, i)
  in
  [ Rpc.Rpc.implement RPCs.client_request (fun t cr ->
        [%log.debug logger "Received" (cr.id : Id.t)] ;
        match H.find t.client_results cr.id with
        | Some result ->
            return result
        | None ->
            Deferred.create (dispatch_client_request t cr))
  ; Rpc.One_way.implement RPCs.request_vote (fun t rv ->
        let res = `RRequestVote rv |> advance_wrapper t in
        don't_wait_for @@ do_actions t res)
  ; Rpc.One_way.implement RPCs.request_vote_response (fun t rvr ->
        let res = `RRequestVoteResponse rvr |> advance_wrapper t in
        don't_wait_for @@ do_actions t res)
  ; Rpc.One_way.implement RPCs.append_entries (fun t ae ->
        let res = `RAppendEntries ae |> advance_wrapper t in
        don't_wait_for @@ do_actions t res)
  ; Rpc.One_way.implement RPCs.append_entries_response (fun t aer ->
        let res = `RAppendEntiresResponse aer |> advance_wrapper t in
        don't_wait_for @@ do_actions t res) ]

let create ~node_id ~node_list ~datadir ~listen_port ~election_timeout
    ~tick_speed ~batch_size ~dispatch_timeout =
  [%log.debug
    logger "Input parameters"
      (node_id : int)
      (node_list : (int * string) list)
      (datadir : string)
      (listen_port : int)
      (election_timeout : int)
      (tick_speed : Time.Span.t)
      (batch_size : int)
      (dispatch_timeout : Time.Span.t)] ;
  let other_node_list =
    List.filter_map node_list ~f:(fun ((i, _) as x) ->
        if Int.(i <> node_id) then Some x else None)
  in
  let config =
    let num_nodes = List.length node_list in
    let phase1majority = (num_nodes / 2) + 1 in
    let phase2majority = (num_nodes / 2) + 1 in
    let other_nodes = other_node_list |> List.map ~f:fst in
    P.
      { phase1majority
      ; phase2majority
      ; num_nodes
      ; other_nodes
      ; node_id
      ; election_timeout }
  in
  let%bind wal, {term; log} = Wal.of_path datadir in
  let core = P.create_node config log term in
  let conns =
    node_list
    |> List.map ~f:(fun (id, addr) -> (id, connect_persist addr))
    |> H.of_alist_exn (module Int)
  in
  let state_machine = create_state_machine () in
  let t_ivar = Ivar.create () in
  let client_request_batcher =
    Batcher.create_counter
      ~f:(fun reqs ->
        match Ivar.peek t_ivar with
        | Some t ->
            handle_client_requests t reqs
        | None ->
            let%bind t = Ivar.read t_ivar in
            handle_client_requests t reqs)
      ~dispatch_timeout ~limit:batch_size
  in
  let server_ivar = Ivar.create () in
  let t =
    { core
    ; server= server_ivar
    ; wal
    ; last_applied= Int64.zero
    ; conns
    ; state_machine
    ; client_ivars= H.create (module Types.Id)
    ; client_request_batcher
    ; client_results= H.create (module Types.Id) }
  in
  Ivar.fill t_ivar t ;
  Async.every ~continue_on_error:true tick_speed (fun () ->
      handle_event t `Tick |> don't_wait_for) ;
  let implementations =
    Rpc.Implementations.create_exn ~implementations:server_impls
      ~on_unknown_rpc:`Continue
  in
  let on_handler_error =
    `Call (fun _ e -> [%log.error logger "Error while handling msg" (e : exn)])
  in
  let server =
    Tcp.Server.create (Tcp.Where_to_listen.of_port listen_port)
      ~on_handler_error (fun _addr reader writer ->
        Rpc.Connection.server_with_close reader writer ~implementations
          ~connection_state:(fun _ -> t)
          ~on_handshake_error:`Ignore)
  in
  upon server (fun server -> Ivar.fill server_ivar server) ;
  return t

let close t =
  let%bind server = Ivar.read t.server in
  let%bind () = Tcp.Server.close server in
  t.conns |> H.to_alist |> List.map ~f:snd
  |> Deferred.List.iter ~how:`Parallel ~f:(fun conn ->
         Async_rpc_kernel.Persistent_connection.Rpc.close conn)

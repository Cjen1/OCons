open! Core
open! Ppx_log_async
module P = Paxos_core
module O = Owal
module H = Hashtbl
module L = Types.Wal.Log
module T = Types.Wal.Term
module Q = Thread_safe_queue
open Types
open Types.MessageTypes
open! Utils

module Threaded = struct
  open Core
  module P = Paxos_core

  type 'a fulfiller = 'a -> unit

  open Types.MessageTypes

  type rx_qs =
    { close: bool
    ; tick: P.event Q.t
    ; client_reqs: (client_request * client_response fulfiller) list Q.t
    ; server_reqs: P.event Q.t
    ; server_resp: P.event Q.t }

  type rx = {q: rx_qs; cond: unit Async.Condition.t}

  type action = [P.pre_sync_action | P.post_sync_action]

  type tx = {q: action list Q.t; cond: unit Async.Condition.t}

  type state =
    { mutable core: P.t
    ; wal: Wal.t
    ; client_fulfillers: (command_id, client_response fulfiller) H.t
    ; client_results: (command_id, client_response) H.t
    ; mutable last_applied: int64
    ; state_machine: Types.state_machine }

  exception Close

  let rec advance_state_machine t = function
    | Some commit_index as idx when Int64.(t.last_applied < commit_index) ->
        let index = Int64.(t.last_applied + one) in
        t.last_applied <- index ;
        let entry = L.get_exn (P.get_log t.core) index in
        let result = update_state_machine t.state_machine entry.command in
        H.set t.client_results ~key:entry.command.id ~data:result ;
        ( match H.find t.client_fulfillers entry.command.id with
        | Some f ->
            f result ;
            H.remove t.client_fulfillers entry.command.id
        | None ->
            () ) ;
        advance_state_machine t idx
    | _ ->
        ()

  let handle_rx_q t ~(rx : rx) ~(tx : tx) =
    print_endline "Running rx_thread" ;
    [%log.debug logger "starting threaded handler"] ;
    let signal_tx () =
      Async.Thread_safe.run_in_async_exn
        (fun () -> Async.Condition.signal tx.cond)
        ()
    in
    let advance_wrapper ev =
      let core', action =
        match P.advance t.core ev with
        | Error (`Msg s) ->
            Fmt.failwith "Raised %s in advancing state machine" s
        | Ok v ->
            v
      in
      t.core <- core' ;
      action
    in
    let loop () =
      while true do
        let len_qs () =
          Q.length rx.q.tick + Q.length rx.q.client_reqs
          + Q.length rx.q.server_reqs + Q.length rx.q.server_resp
        in
        [%log.debug logger "Waiting on cond"] ;
        while len_qs () <= 0 || rx.q.close do
          Async.Thread_safe.run_in_async_wait_exn (fun () ->
              Async.Condition.wait rx.cond)
        done ;
        [%log.debug logger "Reading from queues"] ;
        let actions =
          match rx.q with
          | {close; _} when close ->
              raise Close
          | {tick= q; _} when Q.length q > 0 ->
              q |> Q.dequeue_exn |> advance_wrapper
          | {server_resp= q; _} when Q.length q > 0 ->
              q |> Q.dequeue_exn |> advance_wrapper
          | {server_reqs= q; _} when Q.length q > 0 ->
              q |> Q.dequeue_exn |> advance_wrapper
          | {client_reqs= q; _} when Q.length q > 0 ->
              let batch = Q.dequeue_exn q in
              List.iter batch ~f:(fun (c, f) ->
                  H.set t.client_fulfillers ~key:c.id ~data:f) ;
              advance_wrapper (`Commands (List.map ~f:fst batch))
          | _ ->
              Fmt.failwith "Nothing can be pulled from any queue"
        in
        (* Update on disk state *)
        List.iter actions.wal ~f:(Wal.write t.wal) ;
        Wal.flush t.wal ;
        (* Resolve unapplied commands *)
        List.iter actions.unapplied ~f:(fun id ->
            match H.find t.client_fulfillers id with
            | Some f ->
                f Types.Failure
            | None ->
                ()) ;
        (* Dispatch any actions which don't require a fsync *)
        Q.enqueue tx.q (actions.pre :> action list) ;
        if not @@ List.is_empty actions.pre then signal_tx () ;
        (* Do the fsync if required *)
        if actions.do_sync then Wal.datasync t.wal ;
        (* Do commit index update *)
        advance_state_machine t actions.commit_idx ;
        (* Dispatch remaining operations*)
        Q.enqueue tx.q (actions.post :> action list) ;
        if not @@ List.is_empty actions.post then signal_tx ()
      done
    in
    try loop () with Close -> ()
end

open! Async

let logger =
  let open Async_unix.Log in
  create ~level:`Info ~output:[] ~on_error:`Raise
    ~transform:(fun m -> Message.add_tags m [("src", "Infra")])
    ()

type t =
  { client_batch_size: int
  ; client_batch_timeout: Time.Span.t
  ; mutable current_batch:
      (client_request * client_response Threaded.fulfiller) list
  ; mutable last_batch_dispatched: Time.t
  ; mutable current_batch_size: int
  ; rx: Threaded.rx
  ; tx: Threaded.tx
  ; mutable server: (Socket.Address.Inet.t, int) Tcp.Server.t Ivar.t
  ; mutable last_applied: log_index
  ; conns: (int, Async_rpc_kernel.Persistent_connection.Rpc.t) H.t
  ; client_ivars: (command, client_response Ivar.t list) H.t
  ; client_results: (command_id, op_result) H.t }

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

let do_action t : Threaded.action -> unit = function
  | `SendRequestVote (id, rv) ->
      send t id Types.RPCs.request_vote rv
  | `SendAppendEntries (id, ae) ->
      send t id Types.RPCs.append_entries ae
  | `SendRequestVoteResponse (id, rvr) ->
      send t id Types.RPCs.request_vote_response rvr
  | `SendAppendEntriesResponse (id, aer) ->
      send t id Types.RPCs.append_entries_response aer

let do_actions t =
  let rec loop () =
    let rec try_receive () =
      if Q.length t.tx.q <= 0 then
        let%bind () = Condition.wait t.tx.cond in
        try_receive ()
      else Q.dequeue_exn t.tx.q |> return
    in
    let%bind actions = try_receive () in
    List.iter actions ~f:(do_action t) ;
    loop ()
  in
  loop ()

let enqueue q cond ev = Q.enqueue q ev ; Condition.signal cond ()

let batch_ready_to_dispatch t =
  t.current_batch_size > t.client_batch_size
  || Time.(add t.last_batch_dispatched t.client_batch_timeout < now ())

let maybe_dispatch_client t =
  if batch_ready_to_dispatch t then (
    [%log.debug
      logger "Dispatching request"
        (t.current_batch_size : int)
        ~time_til_batch_timeout:
          ( Time.diff (Time.now ())
              (Time.add t.last_batch_dispatched t.client_batch_timeout)
            : Time.Span.t )] ;
    enqueue t.rx.q.client_reqs t.rx.cond t.current_batch ;
    t.current_batch <- [] ;
    t.current_batch_size <- 0 ;
    t.last_batch_dispatched <- Time.now () )

let server_impls =
  [ Rpc.Rpc.implement RPCs.client_request (fun t cr ->
        [%log.debug logger "Received" (cr.id : Id.t)] ;
        match H.find t.client_results cr.id with
        | Some result ->
            return result
        | None ->
            let p, f = Thread_safe.deferred () in
            t.current_batch <- (cr, f) :: t.current_batch ;
            t.current_batch_size <- t.current_batch_size + 1 ;
            maybe_dispatch_client t ;
            upon p (fun res -> H.set t.client_results ~key:cr.id ~data:res) ;
            p)
  ; Rpc.One_way.implement RPCs.request_vote (fun t rv ->
        enqueue t.rx.q.server_reqs t.rx.cond (`RRequestVote rv))
  ; Rpc.One_way.implement RPCs.request_vote_response (fun t rvr ->
        enqueue t.rx.q.server_resp t.rx.cond (`RRequestVoteResponse rvr))
  ; Rpc.One_way.implement RPCs.append_entries (fun t ae ->
        enqueue t.rx.q.server_reqs t.rx.cond (`RAppendEntries ae))
  ; Rpc.One_way.implement RPCs.append_entries_response (fun t aer ->
        enqueue t.rx.q.server_resp t.rx.cond (`RAppendEntiresResponse aer)) ]

let tick t () =
  enqueue t.rx.q.tick t.rx.cond `Tick ;
  maybe_dispatch_client t

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
  let conns =
    node_list
    |> List.map ~f:(fun (id, addr) -> (id, connect_persist addr))
    |> H.of_alist_exn (module Int)
  in
  let server_ivar = Ivar.create () in
  let rx =
    let open Threaded in
    let rx_qs =
      { close= false
      ; tick= Q.create ()
      ; client_reqs= Q.create ()
      ; server_reqs= Q.create ()
      ; server_resp= Q.create () }
    in
    ({q= rx_qs; cond= Condition.create ()} : rx)
  in
  let tx = ({q= Q.create (); cond= Condition.create ()} : Threaded.tx) in
  let t =
    { rx
    ; client_batch_timeout= dispatch_timeout
    ; current_batch= []
    ; current_batch_size= 0
    ; last_batch_dispatched= Time.now ()
    ; client_batch_size= batch_size
    ; tx
    ; server= server_ivar
    ; last_applied= Int64.zero
    ; conns
    ; client_ivars= H.create (module Types.Command)
    ; client_results= H.create (module Types.Id) }
  in
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
  (* Core state machine thread *)
  let%bind wal, {term; log} = Wal.of_path_async datadir in
  let core = P.create_node config log term in
  let state_machine = Types.create_state_machine () in
  let sm_state =
    Threaded.
      { core
      ; wal
      ; state_machine
      ; client_fulfillers= H.create (module Id)
      ; client_results= H.create (module Id)
      ; last_applied= Int64.zero }
  in
  let _sm_thread : Thread.t =
    Thread.create ~on_uncaught_exn:`Kill_whole_process
      (fun () -> Threaded.handle_rx_q sm_state ~tx:t.tx ~rx:t.rx)
      ()
  in
  Async.every ~continue_on_error:true tick_speed (tick t) ;
  do_actions t |> don't_wait_for ;
  return t

let close t =
  let%bind server = Ivar.read t.server in
  let%bind () = Tcp.Server.close server in
  t.conns |> H.to_alist |> List.map ~f:snd
  |> Deferred.List.iter ~how:`Parallel ~f:(fun conn ->
         Async_rpc_kernel.Persistent_connection.Rpc.close conn)

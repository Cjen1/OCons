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
open Core_profiler.Std_offline

let debug_no_sync = false

let logger =
  let open Async_unix.Log in
  create ~level:`Info ~output:[] ~on_error:`Raise
    ~transform:(fun m -> Message.add_tags m [("src", "Infra")])
    ()

type ev_q =
  { client_reqs: (client_request * Types.op_result Ivar.t) Queue.t
  ; server_reqs: P.event Queue.t
  ; server_resp: P.event Queue.t }

type t =
  { mutable core: P.t
  ; ev_q: ev_q
  ; event_queue_bvar: (unit, read_write) Bvar.t
  ; client_batch_size: int
  ; mutable server: (Socket.Address.Inet.t, int) Tcp.Server.t Ivar.t
  ; wal: Wal.t
  ; mutable last_applied: log_index
  ; conns: (int, Async_rpc_kernel.Persistent_connection.Rpc.t) H.t
  ; state_machine: state_machine
  ; client_ivars: (command, client_response Ivar.t list) H.t
  ; client_results: (command_id, op_result) H.t }

let rec advance_state_machine t = function
  | commit_index when Int64.(t.last_applied < commit_index) ->
      [%log.debug
        logger "Advancing state machine"
          ~from:(t.last_applied : log_index)
          ~to_:(commit_index : log_index)] ;
      let index = Int64.(t.last_applied + one) in
      t.last_applied <- index ;
      let entry = L.get_exn (P.get_log t.core) index in
      let result = update_state_machine t.state_machine entry.command in
      H.set t.client_results ~key:entry.command.id ~data:result ;
      let ivars = H.find_multi t.client_ivars entry.command in
      List.iter ivars ~f:(fun ivar ->
          [%log.debug
            logger "Resolving" ((entry.command.id, index) : Id.t * int64)] ;
          Ivar.fill_if_empty ivar result) ;
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

let probe = Delta_timer.create ~name:"datasync"

let do_actions t ((pre, do_sync, post) : P.action_sequence) =
  do_pre t pre ;
  match do_sync && not debug_no_sync with
  | true ->
      let st = Delta_timer.stateless_start probe in
      let%map () = Wal.datasync t.wal in
      Delta_timer.stateless_stop probe st ;
      do_post t post
  | false ->
      do_post t post |> return

let advance_wrapper t event =
  let core, actions = P.advance t.core event |> get_ok in
  t.core <- core ;
  actions

let enqueue t q v =
  Queue.enqueue q v ;
  Bvar.broadcast t.event_queue_bvar ()

let server_impls =
  [ Rpc.Rpc.implement RPCs.client_request (fun t cr ->
        [%log.debug logger "Received" (cr.id : Id.t)] ;
        match H.find t.client_results cr.id with
        | Some result ->
            return result
        | None ->
            Deferred.create (fun i -> enqueue t t.ev_q.client_reqs (cr, i)))
  ; Rpc.One_way.implement RPCs.request_vote (fun t rv ->
        enqueue t t.ev_q.server_reqs (`RRequestVote rv))
  ; Rpc.One_way.implement RPCs.request_vote_response (fun t rvr ->
        enqueue t t.ev_q.server_resp (`RRequestVoteResponse rvr))
  ; Rpc.One_way.implement RPCs.append_entries (fun t ae ->
        enqueue t t.ev_q.server_reqs (`RAppendEntries ae))
  ; Rpc.One_way.implement RPCs.append_entries_response (fun t aer ->
        enqueue t t.ev_q.server_resp (`RAppendEntiresResponse aer)) ]

let deque_n q n =
  let rec loop (acc, i) n =
    match n with
    | 0 ->
        (acc, i)
    | _ when Queue.length q = 0 ->
        (acc, i)
    | n ->
        loop (Queue.dequeue_exn q :: acc, i + 1) (n - 1)
  in
  let xs, l = loop ([], 0) n in
  (List.rev xs, l)

let%expect_test "dequeue_n" =
  let q = Queue.of_list [1; 2; 3; 4] in
  deque_n q 3 |> [%sexp_of: int list * int] |> Sexp.to_string_hum
  |> print_endline ;
  [%expect {| (1 2 3) |}]

let batch_probe = Probe.create ~name:"Batch_size" ~units:Profiler_units.Int

let handle_ev_q t =
  let batch_counter = ref 0 in
  let batching_freq = 2 in
  let rec loop () =
    let run_queues () =
      match () with
      | () when Queue.length t.ev_q.server_resp > 0 ->
          let event = Queue.dequeue_exn t.ev_q.server_resp in
          event |> advance_wrapper t |> do_actions t
      | () when Queue.length t.ev_q.server_reqs > 0 ->
          let event = Queue.dequeue_exn t.ev_q.server_reqs in
          event |> advance_wrapper t |> do_actions t
      | ()
        when Queue.length t.ev_q.client_reqs >= t.client_batch_size
             || !batch_counter >= batching_freq ->
          if !batch_counter >= batching_freq then batch_counter := 0 ;
          let batch, batch_size =
            deque_n t.ev_q.client_reqs t.client_batch_size
          in
          Probe.record batch_probe batch_size ;
          [%log.debug logger (batch_size : int)] ;
          let htbl = H.of_alist_exn (module Types.Command) batch in
          let ((pre, _, _) as actions) =
            advance_wrapper t (`Commands (List.map batch ~f:fst))
          in
          let () =
            match
              List.find_map pre ~f:(function
                | `Unapplied _ as v ->
                    Some v
                | _ ->
                    None)
            with
            | Some (`Unapplied cmds) ->
                List.iter cmds ~f:(fun cmd ->
                    let ivar = H.find_exn htbl cmd in
                    Ivar.fill ivar Types.Failure)
            | None ->
                ()
          in
          List.iter batch ~f:(fun (cmd, ivar) ->
              if Ivar.is_empty ivar then
                H.add_multi t.client_ivars ~key:cmd ~data:ivar) ;
          do_actions t actions
      | () ->
          incr batch_counter ; return ()
    in
    let%bind () =
      match
        Queue.length t.ev_q.server_resp
        + Queue.length t.ev_q.server_reqs
        + Queue.length t.ev_q.client_reqs
      with
      | 0 ->
          Bvar.wait t.event_queue_bvar
      | _ ->
          return ()
    in
    let%bind () = run_queues () in
    let%bind () =
      Scheduler.yield_until_no_jobs_remain ~may_return_immediately:true ()
    in
    loop ()
  in
  loop ()

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
  let server_ivar = Ivar.create () in
  let t =
    { core
    ; ev_q=
        { client_reqs= Queue.create ()
        ; server_reqs= Queue.create ()
        ; server_resp= Queue.create () }
    ; event_queue_bvar= Bvar.create ()
    ; client_batch_size= batch_size
    ; server= server_ivar
    ; wal
    ; last_applied= Int64.zero
    ; conns
    ; state_machine
    ; client_ivars= H.create (module Types.Command)
    ; client_results= H.create (module Types.Id) }
  in
  Ivar.fill t_ivar t ;
  don't_wait_for (handle_ev_q t) ;
  Async.every ~continue_on_error:true tick_speed (fun () ->
      advance_wrapper t `Tick |> do_actions t |> don't_wait_for) ;
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

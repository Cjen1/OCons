open! Core
open! Async
open! Ppx_log_async
module O = Owal
module H = Hashtbl
module CH = Client_handler
open Types
open Utils
open Core_profiler_disabled.Std

let logger =
  let open Async_unix.Log in
  create ~level:`Info ~output:[] ~on_error:`Raise
    ~transform:(fun m -> Message.add_tags m [("src", "Infra")])
    ()

type infra_config =
  { node_id: node_id
  ; node_list: (node_id * string) list
  ; datadir: string
  ; external_port: int
  ; internal_port: int
  ; tick_speed: Time.Span.t
  ; batch_size: int
  ; batch_timeout: Time.Span.t }
[@@deriving sexp_of]

module Make (C : Consensus_intf.S) = struct
  module MS = Mutable_store.Make (C.Store)

  let message_rpc =
    Async.Rpc.One_way.create ~name:"conc_msg" ~version:0 ~bin_msg:C.bin_message

  type t =
    { mutable core: C.t
    ; event_queue: C.event rd_wr_pipe
    ; tick_queue: [`Tick] rd_wr_pipe
    ; datasync_queue: log_index rd_wr_pipe
    ; external_server: CH.Connection.t
    ; internal_server: (Socket.Address.Inet.t, term) Tcp.Server.t
    ; store: MS.t
    ; mutable last_applied: log_index
    ; conns: (int, Async_rpc_kernel.Persistent_connection.Rpc.t) H.t
    ; state_machine: state_machine
    ; command_results: (Types.command_id, Types.op_result) H.t }

  let batch_available_watch t =
    CH.Connection.run_exn t.external_server
      ~f:CH.functions.batch_available_watch ~arg:()

  let p_cr_len = Probe.create ~name:"cr_len" ~units:Profiler_units.Int

  let get_request_batch t =
    let%bind batch, cr_len =
      CH.Connection.run_exn t.external_server ~f:CH.functions.get_batch ~arg:()
    in
    Probe.record p_cr_len cr_len ;
    return batch

  let return_result t id res =
    CH.Connection.run_exn t.external_server ~f:CH.functions.return_result
      ~arg:(id, res)
    |> don't_wait_for

  let rec advance_state_machine t = function
    | commit_index when Int64.(t.last_applied < commit_index) ->
        [%log.debug
          logger "Advancing state machine"
            ~from:(t.last_applied : log_index)
            ~to_:(commit_index : log_index)] ;
        let index = Int64.(t.last_applied + one) in
        t.last_applied <- index ;
        (* This can't throw an error since there are no holes in the log (<= commit index) *)
        let entry = MS.get_index t.store index |> Result.ok_exn in
        let result = update_state_machine t.state_machine entry.command in
        H.set t.command_results ~key:entry.command.id ~data:result ;
        return_result t entry.command.id (Ok result) ;
        advance_state_machine t commit_index
    | _ ->
        ()

  let get_ok v =
    match v with Error (`Msg s) -> raise @@ Invalid_argument s | Ok v -> v

  let do_action t (act : C.action) =
    match act with
    | `Unapplied cmds ->
        List.iter cmds ~f:(fun cmd -> return_result t cmd.id (Error `Unapplied))
    | `CommitIndexUpdate index ->
        advance_state_machine t index
    | `Send (dst, msg) -> (
        let conn = H.find_exn t.conns dst in
        match
          Async_rpc_kernel.Persistent_connection.Rpc.current_connection conn
        with
        | None ->
            [%log.debug logger "Connection not connected, skipping" (dst : int)]
        | Some conn -> (
          match Rpc.One_way.dispatch' message_rpc conn msg with
          | Ok () ->
              [%log.debug logger "Dispatched successfully" (dst : int)]
          | Error e ->
              Async_rpc_kernel.Rpc_error.raise e (Info.of_string "send request")
          ) )

  let datasync_time = Delta_timer.create ~name:"ds_time"

  let datasync =
    let sequencer = Throttle.Sequencer.create () in
    fun t ->
      let st = Delta_timer.stateless_start datasync_time in
      let%bind () =
        Throttle.enqueue sequencer (fun () ->
            let%bind sync_index = MS.datasync t.store in
            Pipe.write_without_pushback t.datasync_queue.wr sync_index ;
            Deferred.unit )
      in
      Delta_timer.stateless_stop datasync_time st ;
      Deferred.unit

  let do_advance t event =
    let core, actions = C.advance t.core event |> get_ok in
    let core, store = C.pop_store core in
    t.core <- core ;
    let%bind () =
      match MS.update t.store store with
      | `SyncPossible when not actions.nonblock_sync ->
          datasync t
      | `SyncPossible ->
          datasync t |> Deferred.don't_wait_for ;
          Deferred.unit
      | `NoSync ->
          Deferred.unit
    in
    List.iter actions.acts ~f:(do_action t) ;
    Deferred.unit

  let server_impl =
    Rpc.Implementations.create_exn ~on_unknown_rpc:`Raise
      ~implementations:
        [ Rpc.One_way.implement message_rpc (fun q msg ->
              Pipe.write_without_pushback q (`Recv msg) ) ]

  let record_queue_lengths =
    let ds = Probe.create ~name:"ds" ~units:Profiler_units.Int in
    let tk = Probe.create ~name:"tick" ~units:Profiler_units.Int in
    let ev = Probe.create ~name:"ev" ~units:Profiler_units.Int in
    fun t ->
      Probe.record ds (Pipe.length t.datasync_queue.rd) ;
      Probe.record tk (Pipe.length t.tick_queue.rd) ;
      Probe.record ev (Pipe.length t.event_queue.rd)

  let handle_ev_q t =
    let request_batch_pipe =
      Pipe.unfold ~init:() ~f:(fun () ->
          [%log.debug logger "Waiting for batch available"] ;
          let%bind () = batch_available_watch t in
          [%log.debug logger "Batch available"] ;
          let%bind batch = get_request_batch t in
          [%log.debug logger "Got batch"] ;
          return (Some (batch, ())) )
    in
    let construct_choices () :
        [`Event of C.event | `Eof of string] Deferred.Choice.t list =
      record_queue_lengths t ;
      let datasync_queue =
        let read =
          Pipe.read_choice_single_consumer_exn t.datasync_queue.rd [%here]
        in
        Deferred.Choice.map read ~f:(function
          | `Eof ->
              `Eof "datasync"
          | `Ok idx ->
              `Event (`Syncd idx) )
      in
      let tickc =
        let read =
          Pipe.read_choice_single_consumer_exn t.tick_queue.rd [%here]
        in
        Deferred.Choice.map read ~f:(function
          | `Eof ->
              `Eof "tick"
          | `Ok `Tick ->
              `Event `Tick )
      in
      let evc =
        let read =
          Pipe.read_choice_single_consumer_exn t.event_queue.rd [%here]
        in
        Deferred.Choice.map read ~f:(function
          | `Eof ->
              `Eof "event"
          | `Ok v ->
              `Event v )
      in
      let batchc =
        let read =
          Pipe.read_choice_single_consumer_exn request_batch_pipe [%here]
        in
        Deferred.Choice.map read ~f:(function
          | `Eof ->
              `Eof "client-batching"
          | `Ok v ->
              `Event (`Commands v) )
      in
      [datasync_queue; tickc; evc; batchc]
    in
    let rec loop () =
      let run_queues () =
        match%bind choose @@ construct_choices () with
        | `Event (`Commands b) ->
            [%log.debug logger "got command batch"] ;
            let fast_return (cmd : command) =
              match H.find t.command_results cmd.id with
              | Some res ->
                  [%log.debug
                    logger
                      "Client retried request which has already been responded \
                       to"] ;
                  CH.Connection.run_exn t.external_server
                    ~f:CH.functions.return_result ~arg:(cmd.id, Ok res)
                  |> don't_wait_for ;
                  false
              | None ->
                  true
            in
            let b = List.filter b ~f:fast_return in
            do_advance t (`Commands b)
        | `Event e ->
            [%log.debug logger "got event"] ; do_advance t e
        | `Eof queue ->
            [%log.debug logger "Queue unexpectedly closed"] ;
            let message =
              [%message "A queue unexpectedly closed" (queue : string)]
              |> Sexp.to_string_hum
            in
            [%log.error logger message] ;
            raise (Invalid_argument message)
      in
      let%bind () = run_queues () in
      loop ()
    in
    loop ()

  let create infra_config conc_config =
    [%log.debug
      logger "configs" (infra_config : infra_config) (conc_config : C.config)] ;
    let%bind store = MS.of_path infra_config.datadir in
    let core = C.create_node conc_config (MS.get_state store) in
    let conns =
      infra_config.node_list
      |> List.map ~f:(fun (id, addr) -> (id, connect_persist addr))
      |> H.of_alist_exn (module Int)
    in
    let _conn_state =
      H.iteri conns ~f:(fun ~key:id ~data:conn ->
          upon (Async_rpc_kernel.Persistent_connection.Rpc.connected conn)
            (fun _ -> [%log.info logger "Connected to other node" (id : int)]) )
    in
    let state_machine = create_state_machine () in
    let event_queue =
      let rd, wr = Pipe.create () in
      {rd; wr}
    in
    let tick_queue =
      let rd, wr = Pipe.create () in
      {rd; wr}
    in
    let datasync_queue =
      let rd, wr = Pipe.create () in
      {rd; wr}
    in
    let%bind external_server =
      CH.spawn_client_handler ~external_port:infra_config.external_port
        ~batch_size:infra_config.batch_size
        ~batch_timeout:infra_config.batch_timeout
    in
    let%bind internal_server =
      Tcp.Server.create
        (Tcp.Where_to_listen.of_port infra_config.internal_port)
        ~on_handler_error:
          (`Call
            (fun _ e -> [%log.error logger "Error while handling msg" (e : exn)])
            )
        (fun _addr reader writer ->
          Rpc.Connection.server_with_close reader writer
            ~implementations:server_impl
            ~connection_state:(fun _ -> event_queue.wr)
            ~on_handshake_error:`Ignore )
    in
    let t =
      { core
      ; event_queue
      ; tick_queue
      ; datasync_queue
      ; external_server
      ; internal_server
      ; store
      ; last_applied= Int64.zero
      ; conns
      ; state_machine
      ; command_results= H.create (module Types.Id) }
    in
    don't_wait_for (handle_ev_q t) ;
    Async.every ~continue_on_error:true infra_config.tick_speed (fun () ->
        Pipe.write_without_pushback t.tick_queue.wr `Tick ) ;
    return t

  let close t =
    let%bind () = CH.Connection.close t.external_server
    and () = Tcp.Server.close t.internal_server in
    t.conns |> H.to_alist
    |> Deferred.List.iter ~how:`Parallel ~f:(fun (_, conn) ->
           Async_rpc_kernel.Persistent_connection.Rpc.close conn )
end

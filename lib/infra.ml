open! Core
open! Async
open! Ppx_log_async
module P = Paxos_core
module O = Owal
module H = Hashtbl
module IS = Types.IStorage
module MS = Types.MutableStorage (IS)
module CH = Client_handler
open Types
open Utils

let logger =
  let open Async_unix.Log in
  create ~level:`Info ~output:[] ~on_error:`Raise
    ~transform:(fun m -> Message.add_tags m [("src", "Infra")])
    ()

type t =
  { mutable core: P.t
  ; event_queue: P.event rd_wr_pipe
  ; tick_queue: [`Tick] rd_wr_pipe
  ; datasync_queue: log_index rd_wr_pipe
  ; external_server: CH.Connection.t
  ; internal_server: (Socket.Address.Inet.t, term) Tcp.Server.t
  ; store: MS.t
  ; mutable last_applied: log_index
  ; conns: (int, Async_rpc_kernel.Persistent_connection.Rpc.t) H.t
  ; state_machine: state_machine }

let get_request_batch t =
  CH.Connection.run_exn t.external_server ~f:CH.functions.get_batch ~arg:()

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
      let entry = IS.get_index (MS.get_state t.store) index |> Result.ok_exn in
      let result = update_state_machine t.state_machine entry.command in
      return_result t entry.command.id (Ok result) ;
      advance_state_machine t commit_index
  | _ ->
      ()

let get_ok v =
  match v with Error (`Msg s) -> raise @@ Invalid_argument s | Ok v -> v

let send : type query. t -> int -> query Rpc.One_way.t -> query -> unit =
 fun t target rpc msg ->
  let conn = H.find_exn t.conns target in
  match Async_rpc_kernel.Persistent_connection.Rpc.current_connection conn with
  | None -> ()
  | Some conn -> 
    match Rpc.One_way.dispatch' rpc conn msg with
    | Ok () -> ()
    | Error e ->
        Async_rpc_kernel.Rpc_error.raise e (Info.of_string "send request")

let do_action t (act : P.action) =
  match act with
  | `SendRequestVote (id, rv) ->
      send t id Types.RPCs.request_vote rv
  | `SendRequestVoteResponse (id, rvr) ->
      send t id Types.RPCs.request_vote_response rvr
  | `SendAppendEntries (id, ae) ->
      send t id Types.RPCs.append_entries ae
  | `SendAppendEntriesResponse (id, aer) ->
      send t id Types.RPCs.append_entries_response aer
  | `Unapplied cmds ->
      List.iter cmds ~f:(fun cmd -> return_result t cmd.id (Error `Unapplied))
  | `CommitIndexUpdate index ->
      advance_state_machine t index

let datasync =
  let sequencer = Throttle.Sequencer.create () in
  fun t ->
    Throttle.enqueue sequencer (fun () ->
        let%bind sync_index = MS.datasync t.store in
        Pipe.write_without_pushback t.datasync_queue.wr sync_index ;
        Deferred.unit )

let do_advance t event =
  let core, actions = P.advance t.core event |> get_ok in
  let core, store = P.pop_store core in
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

let server_impls =
  Rpc.Implementations.create_exn ~on_unknown_rpc:`Raise
    ~implementations:
      [ Rpc.One_way.implement RPCs.request_vote (fun q rv ->
            Pipe.write_without_pushback q (`RRequestVote rv) )
      ; Rpc.One_way.implement RPCs.request_vote_response (fun q rvr ->
            Pipe.write_without_pushback q (`RRequestVoteResponse rvr) )
      ; Rpc.One_way.implement RPCs.append_entries (fun q ae ->
            Pipe.write_without_pushback q (`RAppendEntries ae) )
      ; Rpc.One_way.implement RPCs.append_entries_response (fun q aer ->
            Pipe.write_without_pushback q (`RAppendEntiresResponse aer) ) ]

let handle_ev_q t _window_size =
  let request_batch_pipe =
    Pipe.unfold ~init:() ~f:(fun () ->
        let%bind batch = get_request_batch t in
        return (Some (batch, ())) )
  in
  let construct_choices () :
      [`Event of P.event | `Eof of string] Deferred.Choice.t list =
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
      let read = Pipe.read_choice_single_consumer_exn t.tick_queue.rd [%here] in
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
      | `Event e ->
          do_advance t e
      | `Eof queue ->
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

let create ~node_id ~node_list ~datadir ~external_port ~internal_port
    ~election_timeout ~tick_speed ~batch_size ~batch_timeout =
  [%log.debug
    logger "Input parameters"
      (node_id : int)
      (node_list : (int * string) list)
      (datadir : string)
      (external_port : int)
      (internal_port : int)
      (election_timeout : int)
      (tick_speed : Time.Span.t)
      (batch_size : int)
      (batch_timeout : Time.Span.t)] ;
  let other_node_list =
    List.filter_map node_list ~f:(fun ((i, _) as x) ->
        if Int.(i <> node_id) then Some x else None )
  in
  let config =
    let num_nodes = List.length node_list in
    let phase1quorum = (num_nodes / 2) + 1 in
    let phase2quorum = (num_nodes / 2) + 1 in
    let other_nodes = other_node_list |> List.map ~f:fst in
    P.
      { phase1quorum
      ; phase2quorum
      ; num_nodes
      ; other_nodes
      ; node_id
      ; election_timeout }
  in
  let%bind store = MS.of_path datadir in
  let core = P.create_node config (MS.get_state store) in
  let conns =
    node_list
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
    CH.spawn_client_handler ~external_port ~batch_size ~batch_timeout
  in
  let%bind internal_server =
    Tcp.Server.create
      (Tcp.Where_to_listen.of_port internal_port)
      ~on_handler_error:
        (`Call
          (fun _ e -> [%log.error logger "Error while handling msg" (e : exn)])
          )
      (fun _addr reader writer ->
        Rpc.Connection.server_with_close reader writer
          ~implementations:server_impls
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
    ; state_machine }
  in
  let log_window = 1 * batch_size in
  don't_wait_for (handle_ev_q t log_window) ;
  Async.every ~continue_on_error:true tick_speed (fun () ->
      Pipe.write_without_pushback t.tick_queue.wr `Tick ) ;
  return t

let close t =
  let%bind () = CH.Connection.close t.external_server
  and () = Tcp.Server.close t.internal_server in
  t.conns |> H.to_alist
  |> Deferred.List.iter ~how:`Parallel ~f:(fun (_, conn) ->
         Async_rpc_kernel.Persistent_connection.Rpc.close conn )

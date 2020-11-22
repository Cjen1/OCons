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

type 'a rd_wr_pipe = {rd: 'a Pipe.Reader.t; wr: 'a Pipe.Writer.t}

module ClientHandler = struct
  open Rpc_parallel

  module T = struct
    (* Basic idea is that the main process contacts this one explicitly for a batch
       We read that batch off of the cr_pipe.
       When a request arrives we put it in that pipe.
    *)
    type request_batch = client_request list [@@deriving bin_io]

    type return_result_t = command_id * client_response [@@deriving bin_io]

    type 'a functions =
      { get_batch: ('a, unit, request_batch) Function.t
      ; return_result: ('a, return_result_t, unit) Function.t }

    module Worker_state = struct
      type init_arg =
        {external_port: int; batch_size: int; batch_timeout: Time.Span.t}
      [@@deriving bin_io]

      type t =
        { client_ivars: (command_id, client_response Ivar.t list) H.t
        ; client_results: (command_id, op_result) H.t
        ; cr_pipe: client_request rd_wr_pipe
        ; batch_size: int
        ; batch_timeout: Time.Span.t }
    end

    module Connection_state = struct
      type init_arg = unit [@@deriving bin_io]

      type t = unit
    end

    module Functions
        (C : Rpc_parallel.Creator
               with type worker_state := Worker_state.t
                and type connection_state := Connection_state.t) =
    struct
      open Worker_state

      let client_handler_impl =
        [ Rpc.Rpc.implement RPCs.client_request (fun t cr ->
              [%log.debug logger "Received" (cr.id : Id.t)] ;
              match H.find t.client_results cr.id with
              | Some result ->
                  return (Ok result)
              | None ->
                  Deferred.create (fun i ->
                      H.add_multi t.client_ivars ~key:cr.id ~data:i ;
                      Pipe.write_without_pushback t.cr_pipe.wr cr)) ]

      let init_worker_state {external_port; batch_size; batch_timeout} =
        let rd, wr = Pipe.create () in
        let t =
          { client_ivars= H.create (module Id)
          ; client_results= H.create (module Id)
          ; cr_pipe= {rd; wr}
          ; batch_size
          ; batch_timeout }
        in
        let implementations =
          Rpc.Implementations.create_exn ~implementations:client_handler_impl
            ~on_unknown_rpc:`Continue
        in
        let on_handler_error =
          `Call
            (fun _ e ->
              [%log.error logger "Error while handling msg" (e : exn)])
        in
        let%bind (_ : (Socket.Address.Inet.t, int) Tcp.Server.t) =
          Tcp.Server.create (Tcp.Where_to_listen.of_port external_port)
            ~on_handler_error (fun _addr reader writer ->
              Rpc.Connection.server_with_close reader writer ~implementations
                ~connection_state:(fun _ -> t)
                ~on_handshake_error:`Ignore)
        in
        return t

      let init_connection_state ~connection:_ ~worker_state:_ () = Deferred.unit

      let get_batch =
        C.create_rpc ~name:"get_batch"
          ~f:(fun ~worker_state:t ~conn_state:() () ->
            match%bind Pipe.read t.cr_pipe.rd with
            | `Eof ->
                assert false
            | `Ok fst ->
                let batch = ref [fst] in
                let rec loop rem =
                  match%bind Pipe.read_exactly t.cr_pipe.rd ~num_values:rem with
                  | `Eof ->
                      assert false
                  | `Exactly q ->
                      Queue.iter q ~f:(fun v -> batch := v :: !batch) ;
                      Deferred.unit
                  | `Fewer q ->
                      Queue.iter q ~f:(fun v -> batch := v :: !batch) ;
                      loop (rem - Queue.length q)
                in
                let batch_gather = loop t.batch_size in
                let batch_timout = after t.batch_timeout in
                let%bind () =
                  choose [choice batch_gather ignore; choice batch_timout ignore]
                in
                return !batch)
          ~bin_input:bin_unit ~bin_output:bin_request_batch ()

      let return_result =
        C.create_one_way ~name:"return_results"
          ~f:(fun ~worker_state:t ~conn_state:() (cmd_id, result) ->
            ( match result with
            | Error _ ->
                ()
            | Ok result ->
                H.set t.client_results ~key:cmd_id ~data:result ) ;
            let ivars = H.find_multi t.client_ivars cmd_id in
            List.iter ivars ~f:(fun i -> Ivar.fill i result) ;
            H.remove_multi t.client_ivars cmd_id)
          ~bin_input:bin_return_result_t ()

      let functions = {get_batch; return_result}
    end
  end

  module M = Rpc_parallel.Make (T)
  include M
  module Shutdown_on = M.Shutdown_on (Monad.Ident)
end

module CH = ClientHandler

let spawn_client_handler ~external_port ~batch_size ~batch_timeout =
  let args = CH.T.Worker_state.{external_port; batch_size; batch_timeout} in
  CH.spawn_exn ~shutdown_on:CH.Shutdown_on.Connection_closed
    ~redirect_stdout:`Dev_null ~redirect_stderr:`Dev_null args
    ~on_failure:Error.raise ~connection_state_init_arg:()

type t =
  { mutable core: P.t
  ; event_queue: P.event rd_wr_pipe
  ; external_server: ClientHandler.Connection.t
  ; internal_server: (Socket.Address.Inet.t, term) Tcp.Server.t
  ; wal: Wal.t
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
      let entry = L.get_exn (P.get_log t.core) index in
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

let do_actions t ((pre, do_sync, post) : P.action_sequence) =
  do_pre t pre ;
  match do_sync && not debug_no_sync with
  | true ->
      let%map () = Wal.datasync t.wal in
      do_post t post
  | false ->
      do_post t post |> return

let advance_wrapper t event =
  let core, actions = P.advance t.core event |> get_ok in
  t.core <- core ;
  actions

let server_impls =
  Rpc.Implementations.create_exn ~on_unknown_rpc:`Continue
    ~implementations:
      [ Rpc.One_way.implement RPCs.request_vote (fun q rv ->
            Pipe.write_without_pushback q (`RRequestVote rv))
      ; Rpc.One_way.implement RPCs.request_vote_response (fun q rvr ->
            Pipe.write_without_pushback q (`RRequestVoteResponse rvr))
      ; Rpc.One_way.implement RPCs.append_entries (fun q ae ->
            Pipe.write_without_pushback q (`RAppendEntries ae))
      ; Rpc.One_way.implement RPCs.append_entries_response (fun q aer ->
            Pipe.write_without_pushback q (`RAppendEntiresResponse aer)) ]

let handle_ev_q t =
  let request_batch_pipe =
    Pipe.unfold ~init:() ~f:(fun () ->
        let%bind batch = get_request_batch t in
        return (Some (batch, ())))
  in
  let construct_choices () =
    let evq =
      let open Deferred.Choice in
      let read =
        Pipe.read_choice_single_consumer_exn t.event_queue.rd [%here]
      in
      map read ~f:(function `Eof -> assert false | `Ok v -> `Event v)
    in
    let batchq =
      let open Deferred.Choice in
      let read =
        Pipe.read_choice_single_consumer_exn request_batch_pipe [%here]
      in
      map read ~f:(function `Eof -> assert false | `Ok v -> `Batch v)
    in
    [evq; batchq]
  in
  let rec loop () =
    let run_queues () =
      match%bind choose @@ construct_choices () with
      | `Event e ->
          e |> advance_wrapper t |> do_actions t
      | `Batch batch ->
          let batch_size = List.length batch in
          [%log.debug logger (batch_size : int)] ;
          let ((pre, _, _) as actions) = advance_wrapper t (`Commands batch) in
          let unapplied =
            List.filter_map pre ~f:(function
              | `Unapplied _ as v ->
                  Some v
              | _ ->
                  None)
          in
          List.iter unapplied ~f:(fun (`Unapplied cmds) ->
              List.iter cmds ~f:(fun cmd ->
                  return_result t cmd.id (Error `Unapplied))) ;
          do_actions t actions
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
  let event_queue =
    let rd, wr = Pipe.create () in
    {rd; wr}
  in
  let%bind external_server =
    spawn_client_handler ~external_port ~batch_size ~batch_timeout
  in
  let%bind internal_server =
    Tcp.Server.create
      (Tcp.Where_to_listen.of_port external_port)
      ~on_handler_error:
        (`Call
          (fun _ e -> [%log.error logger "Error while handling msg" (e : exn)]))
      (fun _addr reader writer ->
        Rpc.Connection.server_with_close reader writer
          ~implementations:server_impls
          ~connection_state:(fun _ -> event_queue.wr)
          ~on_handshake_error:`Ignore)
  in
  let t =
    { core
    ; event_queue
    ; external_server
    ; internal_server
    ; wal
    ; last_applied= Int64.zero
    ; conns
    ; state_machine }
  in
  don't_wait_for (handle_ev_q t) ;
  Async.every ~continue_on_error:true tick_speed (fun () ->
      advance_wrapper t `Tick |> do_actions t |> don't_wait_for) ;
  return t

let close t =
  let%bind () = ClientHandler.Connection.close t.external_server
  and () = Tcp.Server.close t.internal_server in
  t.conns |> H.to_alist
  |> Deferred.List.iter ~how:`Parallel ~f:(fun (_, conn) ->
         Async_rpc_kernel.Persistent_connection.Rpc.close conn)

open! Core
open! Async
module P = Paxos_core
module O = Odbutils.Owal
module H = Hashtbl
module L = Types.Log
module T = Types.Term
open Types
open Types.MessageTypes
open! Utils

let src = Logs.Src.create "Infra" ~doc:"Infrastructure module"

module Log = (val Logs.src_log src : Logs.LOG)

type actions = P.action

type wal = {t: T.Wal.t; l: L.Wal.t}

type t =
  { mutable core: P.t
  ; mutable server: (Socket.Address.Inet.t, int) Tcp.Server.t Ivar.t
  ; wal: wal
  ; mutable last_applied: log_index
  ; conns: (int, Async_rpc_kernel.Persistent_connection.Rpc.t) H.t
  ; state_machine: state_machine
  ; client_ivars: (command_id, client_response Ivar.t list) H.t
  ; client_request_batcher: (client_request * client_response Ivar.t) Batcher.t
  ; client_results: (command_id, op_result) H.t }

let sync t =
  let l_p = L.Wal.datasync t.wal.l in
  let t_p = T.Wal.datasync t.wal.t in
  Deferred.all_unit [l_p; t_p]

let rec advance_state_machine t = function
  | commit_index when Int64.(t.last_applied < commit_index) ->
      let index = Int64.(t.last_applied + one) in
      t.last_applied <- index ;
      let entry = L.get_exn (P.get_log t.core) index in
      let result = update_state_machine t.state_machine entry.command in
      H.set t.client_results ~key:entry.command.id ~data:result ;
      let ivars = H.find_multi t.client_ivars entry.command.id in
      List.iter ivars ~f:(fun ivar ->
          Log.debug (fun m ->
              m "Resolving (cid=%s, idx=%a"
                (Id.to_string entry.command.id)
                Fmt.int64 index) ;
          Ivar.fill ivar result) ;
      advance_state_machine t commit_index
  | _ ->
      ()

(* For the implementations in order to have side effects other than just the simple response (see updating the term etc) 
   need to filter out the rpc response *)
let find_delete xs ~f =
  let rec fold xs acc =
    match xs with
    | [] ->
        (None, acc)
    | x :: xs -> (
      match f x with
      | Some v ->
          (Some v, List.rev_append acc xs)
      | None ->
          fold xs (x :: acc) )
  in
  fold xs []

let find_delete_exn xs ~f =
  match find_delete xs ~f with
  | Some v, xs ->
      (v, xs)
  | None, _ ->
      raise @@ Invalid_argument "find_delete: no matching element in list"

let get_ok v =
  match v with Error (`Msg s) -> raise @@ Invalid_argument s | Ok v -> v

let rec do_actions t (actions : actions list) =
  let actions = List.sort actions ~compare:P.compare_apply_order in
  Deferred.List.iter actions ~how:`Sequential ~f:(function
    | `PersistantChange (`Log op) ->
        L.Wal.write t.wal.l op |> return
    | `PersistantChange (`Term op) ->
        T.Wal.write t.wal.t op |> return
    | `Sync ->
        sync t
    | `CommitIndexUpdate index ->
        advance_state_machine t index |> return
    | `SendRequestVote (id, rv) ->
        send t id Types.RPCs.request_vote rv (fun rvr ->
            `RRequestVoteResponse (id, rvr)) ;
        return ()
    | `SendAppendEntries (id, ae) ->
        send t id Types.RPCs.append_entries ae (fun aer ->
            `RAppendEntiresResponse (id, aer)) ;
        return ()
    | `SendRequestVoteResponse _ | `SendAppendEntriesResponse _ ->
        raise @@ Invalid_argument "Cannot send response what is not the source"
    | `Unapplied _cmds ->
        raise @@ Invalid_argument "Cannot apply unapplied args")

and send :
    type query response.
       t
    -> int
    -> (query, response) Rpc.Rpc.t
    -> query
    -> (response -> P.event)
    -> unit =
 fun t target rpc msg to_event ->
  let p =
    let conn = H.find_exn t.conns target in
    let%bind conn = Async_rpc_kernel.Persistent_connection.Rpc.connected conn in
    match%bind Rpc.Rpc.dispatch' rpc conn msg with
    | Ok v ->
        to_event v |> handle_event t
    | Error _ ->
        (* TODO good errors here *)
        assert false
  in
  don't_wait_for p

and handle_event t event =
  let core, actions = P.advance t.core event |> get_ok in
  t.core <- core ;
  do_actions t actions

let advance_wrapper t event =
  let core, actions = P.advance t.core event |> get_ok in
  t.core <- core ;
  actions

let server_impls =
  [ Rpc.Rpc.implement RPCs.request_vote (fun t rv ->
        let actions = `RRequestVote (-1, rv) |> advance_wrapper t in
        let resp, actions =
          find_delete_exn actions ~f:(function
            | `SendRequestVoteResponse (i, rvr) when i = -1 ->
                Some rvr
            | _ ->
                None)
        in
        let%bind () = do_actions t actions in
        return resp)
  ; Rpc.Rpc.implement RPCs.append_entries (fun t ae ->
        let actions = `RAppendEntries (-1, ae) |> advance_wrapper t in
        let resp, actions =
          find_delete_exn actions ~f:(function
            | `SendAppendEntriesResponse (i, aer) when i = -1 ->
                Some aer
            | _ ->
                None)
        in
        let%bind () = do_actions t actions in
        return resp)
  ; Rpc.Rpc.implement RPCs.client_request (fun t cr ->
        match H.find t.client_results cr.id with
        | Some result ->
            return result
        | None ->
            let resp_ivar = Ivar.create () in
            let%bind () =
              Batcher.dispatch t.client_request_batcher (cr, resp_ivar)
            in
            Ivar.read resp_ivar) ]

let handle_client_requests t
    (req_ivar_list : (command, Types.op_result Ivar.t) List.Assoc.t) =
  let actions = advance_wrapper t (`Commands (List.map req_ivar_list ~f:fst)) in
  let actions =
    match
      find_delete actions ~f:(function
        | `Unapplied _ as v ->
            Some v
        | _ ->
            None)
    with
    | None, actions ->
        actions
    | Some (`Unapplied cmds), actions ->
        List.iter cmds ~f:(fun cmd ->
            let ivar =
              List.Assoc.find_exn req_ivar_list cmd ~equal:(fun a b ->
                  Id.(a.id = b.id))
            in
            Ivar.fill ivar Types.Failure) ;
        actions
  in
  List.iter req_ivar_list ~f:(fun (cmd, ivar) ->
      if Ivar.is_empty ivar then
        H.add_multi t.client_ivars ~key:cmd.id ~data:ivar) ;
  do_actions t actions

let create ~node_id ~node_list ~datadir ~listen_port ~election_timeout
    ~tick_speed ~batch_size ~dispatch_timeout =
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
  let%bind lwal, log = L.Wal.of_path (Fmt.str "%s/log.wal" datadir) in
  let%bind twal, term = T.Wal.of_path (Fmt.str "%s/term.wal" datadir) in
  let core = P.create_node config log term in
  let wal = {t= twal; l= lwal} in
  let conns =
    node_list
    |> List.map ~f:(fun (id, addr) -> (id, connect_persist addr))
    |> H.of_alist_exn (module Int)
  in
  let state_machine = create_state_machine () in
  let t_ivar = Ivar.create () in
  let client_request_batcher =
    Batcher.create
      ~f:(fun reqs ->
          let%bind t = Ivar.read t_ivar in
          handle_client_requests t reqs )
      ~dispatch_timeout
      ~limit:(fun ls -> List.length ls > batch_size)
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
  Ivar.fill t_ivar t;
  Async.every ~continue_on_error:true tick_speed (fun () ->
      handle_event t `Tick |> don't_wait_for) ;
  let implementations =
    Rpc.Implementations.create_exn ~implementations:server_impls
      ~on_unknown_rpc:`Continue
  in
  let server =
    Tcp.Server.create (Tcp.Where_to_listen.of_port listen_port)
      ~on_handler_error:`Ignore (fun _addr reader writer ->
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

open Base
open Types
open Lwt.Infix
module P = Paxos_core
module L = Log
module T = Term
module U = Unix_capnp_messaging
module O = Owal
module M = Messaging

let src = Logs.Src.create "Infra" ~doc:"Infrastructure module"

module Log = (val Logs.src_log src : Logs.LOG)

let ( >>>= ) = Lwt_result.bind

type t =
  { mutable core: P.t
  ; mutable last_applied: log_index
  ; mutable ongoing_log_addition: bool
  ; seen_client_requests: (command_id, command * client_id option) Hashtbl.t
  ; client_results: (command_id, op_result) Hashtbl.t
  ; state_machine: StateMachine.t
  ; cmgr: U.Conn_manager.t }

let rec advance_state_machine t = function
  | commit_index when Int64.(t.last_applied < commit_index) -> (
      let index = Int64.(succ t.last_applied) in
      let entry = L.get_exn t.core.log index in
      match Hashtbl.find t.seen_client_requests entry.command_id with
      | None ->
          Log.err (fun m -> m "Have to get client command from other replicas") ;
          List.map t.core.config.other_nodes ~f:(fun id ->
              M.Send.requestsAfter t.cmgr id ~index)
      | Some (cmd, id_o) -> (
          let result = StateMachine.update t.state_machine cmd in
          Hashtbl.set t.client_results ~key:cmd.id ~data:result ;
          t.last_applied <- index ;
          match id_o with
          | Some cid ->
              Log.err (fun m ->
                  m "Sending response for index %a cid %a to %a" Fmt.int64 index
                    Fmt.int64 cmd.id Fmt.int64 cid) ;
              [M.Send.clientResponse t.cmgr cid ~id:cmd.id ~result]
          | None ->
              [] ) )
  | _ ->
      []

let perform_action t : P.action -> (unit, exn) Lwt_result.t list =
  let open M.Send in
  function
  | `SendRequestVote (node_id, msg) ->
      Log.debug (fun m -> m "Sending request vote") ;
      [requestVote t.cmgr node_id ~term:msg.term ~leaderCommit:msg.leader_commit]
  | `SendRequestVoteResponse (node_id, msg) ->
      Log.debug (fun m -> m "Sending request vote response") ;
      [ requestVoteResp t.cmgr node_id ~term:msg.term
          ~voteGranted:msg.vote_granted ~entries:msg.entries ]
  | `SendAppendEntries (node_id, msg) ->
      Log.debug (fun m -> m "Sending append entries") ;
      [ appendEntries t.cmgr node_id ~term:msg.term
          ~prevLogIndex:msg.prev_log_index ~prevLogTerm:msg.prev_log_term
          ~entries:msg.entries ~leaderCommit:msg.leader_commit ]
  | `SendAppendEntriesResponse (node_id, msg) ->
      Log.debug (fun m -> m "Sending append entries response") ;
      [appendEntriesResp t.cmgr node_id ~term:msg.term ~success:msg.success]
  | `CommitIndexUpdate commit_index ->
      Log.debug (fun m ->
          m "Advancing state machine to commit index %a" Fmt.int64 commit_index) ;
      advance_state_machine t commit_index

let do_actions t actions =
  let actions = List.map ~f:(perform_action t) actions in
  let actions = List.fold_left actions ~init:[] ~f:(fun acc v -> v @ acc) in
  List.fold_left actions ~init:(Lwt.return_ok ()) ~f:(fun prev v ->
      prev >>>= fun () -> v)

let handle_advance t event =
  (* Due to global ocaml lock any changes within handle will be serialisable *)
  let t_core', actions = P.advance t.core event in
  t.core <- t_core' ;
  if Poly.(event = `LogAddition) then t.ongoing_log_addition <- false ;
  let sync =
    if
      List.exists
        ~f:(function
          | `SendRequestVote _
          | `SendRequestVoteResponse _
          | `SendAppendEntries _
          | `SendAppendEntriesResponse _ ->
              true
          | _ ->
              false)
        actions
    then
      let t_p = T.sync t.core.current_term |> Lwt_result.get_exn in
      let l_p = L.sync t.core.log |> Lwt_result.get_exn in
      Lwt.join [t_p; l_p]
    else Lwt.return_unit
  in
  sync >>= fun () -> do_actions t actions

let handle_message t_p _cmgr src msg =
  t_p
  >>= fun t ->
  let open Messaging.API.Reader in
  let open ServerMessage in
  match msg |> Capnp.BytesMessage.Message.readonly |> of_message |> get with
  | RequestVote msg ->
      let open RequestVote in
      let term = term_get msg in
      let leader_commit = leader_commit_get msg in
      let event = `RRequestVote (src, Types.{term; leader_commit}) in
      handle_advance t event
  | RequestVoteResp msg ->
      let open RequestVoteResp in
      let vote_granted = vote_granted_get msg in
      let term = term_get msg in
      let entries =
        entries_get_list msg |> List.map ~f:Messaging.log_entry_from_capnp
      in
      let start_index = start_index_get msg in
      let event =
        `RRequestVoteResponse
          (src, Types.{vote_granted; term; entries; start_index})
      in
      handle_advance t event
  | AppendEntries msg ->
      let open AppendEntries in
      let term = term_get msg in
      let prev_log_index = prev_log_index_get msg in
      let prev_log_term = prev_log_term_get msg in
      let entries =
        entries_get_list msg |> List.map ~f:Messaging.log_entry_from_capnp
      in
      let leader_commit = leader_commit_get msg in
      let event =
        `RAppendEntries
          ( src
          , Types.{term; prev_log_index; prev_log_term; entries; leader_commit}
          )
      in
      handle_advance t event
  | AppendEntriesResp msg ->
      let open AppendEntriesResp in
      let term = term_get msg in
      let success =
        match get msg with
        | Success mi ->
            Ok mi
        | Failure pli ->
            Error pli
        | Undefined i ->
            raise
            @@ Invalid_argument
                 (Fmt.str "Success undefined %d in AppendEntriesResp" i)
      in
      let event = `RAppendEntiresResponse (src, Types.{term; success}) in
      handle_advance t event
  | ClientRequest msg ->
      (* Dispatch a thread to add new entries to the log at some future point
         This should ensure that the system gets minimally overloaded
      *)
      let command = Messaging.command_from_capnp msg in
      if Hashtbl.mem t.client_results command.id then
        M.Send.clientResponse ~sem:`AtLeastOnce t.cmgr src ~id:command.id
          ~result:(Hashtbl.find_exn t.client_results command.id)
      else (
        Base.Hashtbl.update t.seen_client_requests command.id ~f:(function
          | Some v ->
              v
          | None when L.id_in_log t.core.log command.id ->
              (command, Some src)
          | None ->
              (* Dispatch `LogAddition as a side effect *)
              ( if not t.ongoing_log_addition then
                let async () =
                  Lwt.catch
                    (fun () -> handle_advance t `LogAddition)
                    Lwt.return_error
                  >|= function
                  | Ok () ->
                      ()
                  | Error exn ->
                      Log.err (fun m ->
                          m "Got %a while handling log addition" Fmt.exn exn)
                in
                Lwt.async async ) ;
              (command, Some src)) ;
        Lwt.return_ok () )
  | ClientResponse _msg ->
      raise (Invalid_argument "ClientResponse message")
  | RequestsAfter index ->
      let relevant_entries = L.entries_after_inc t.core.log index in
      let fold acc v =
        let open Continue_or_stop in
        match Hashtbl.find t.seen_client_requests v.command_id with
        | Some (cmd, _) ->
            Continue (cmd :: acc)
        | None ->
            Stop acc
      in
      let commands =
        List.fold_until relevant_entries ~finish:(fun x -> x) ~init:[] ~f:fold
      in
      Messaging.Send.requestUpdate t.cmgr src ~commands
  | RequestsUpdate commands ->
      let iter command =
        let command = M.command_from_capnp command in
        Hashtbl.update t.seen_client_requests command.id ~f:(function
          | Some v ->
              v
          | None ->
              (command, None))
      in
      Capnp.Array.iter commands ~f:iter ;
      do_actions t [`CommitIndexUpdate t.core.commit_index]
  | Undefined i ->
      raise (Invalid_argument (Fmt.str "Undefined message of %d" i))

let ticker t s () =
  let rec loop () =
    Lwt_unix.sleep s >>= fun () -> handle_advance t `Tick >>>= fun () -> loop ()
  in
  loop () |> Lwt_result.get_exn

let create ~listen_address ~node_list ?(election_timeout = 5) ?(tick_time = 0.1)
    ?(retry_connection_timeout = 0.1) ?(log_path = "./log")
    ?(term_path = "./term") node_id =
  let other_node_list =
    List.filter_map node_list ~f:(fun ((i, _) as x) ->
        if Int64.(i <> node_id) then Some x else None)
  in
  let t_p, t_f = Lwt.task () in
  let cmgr =
    U.Conn_manager.create ~retry_connection_timeout ~listen_address ~node_id
      (handle_message t_p)
  in
  let ps =
    match other_node_list with
    | [] ->
        [Lwt.return_unit]
    | _ ->
        List.map other_node_list ~f:(fun (id, addr) ->
            U.Conn_manager.add_outgoing cmgr id addr (`Persistant addr))
  in
  Lwt.choose ps
  >>= fun () ->
  (* Wait for at least one connection to be made *)
  let log_p = L.of_file log_path in
  let term_p = T.of_file term_path in
  Lwt.both log_p term_p
  >>= fun (log, term) ->
  let config =
    let num_nodes = List.length node_list in
    let phase1majority = num_nodes / 2 in
    let phase2majority = num_nodes / 2 in
    let other_nodes = other_node_list |> List.map ~f:fst in
    P.
      { phase1majority
      ; phase2majority
      ; num_nodes
      ; other_nodes
      ; node_id
      ; election_timeout }
  in
  let state_machine = StateMachine.create () in
  let last_applied = Int64.zero in
  let t =
    { core= P.create_node config log term
    ; last_applied
    ; state_machine
    ; seen_client_requests= Hashtbl.create (module Int64)
    ; client_results = Hashtbl.create (module Int64)
    ; ongoing_log_addition= false
    ; cmgr }
  in
  Lwt.wakeup t_f t ;
  Lwt.async (ticker t tick_time) ;
  Lwt.wait () |> fst

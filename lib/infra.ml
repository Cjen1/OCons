open Base
module P = Paxos_core
module S = Types.StateMachine
module L = Log
module T = Term
module UC = Unix_capnp_messaging.Conn_manager
module O = Owal
module MS = Messaging.Serialise
module H = Hashtbl
open Types
open Lwt.Infix

let ( >>>= ) = Lwt_result.bind

open Utils

let src = Logs.Src.create "Infra" ~doc:"Infrastructure module"

module Log = (val Logs.src_log src : Logs.LOG)

type state_machine_actions =
  [ `RequestsAfter of node_id * log_index
  | `ClientResponse of S.op_result * command_id * client_id ]

type actions = [P.action | state_machine_actions]

type t =
  { mutable core: P.t
  ; mutable last_applied: log_index
  ; state_machine: S.t
  ; seen_client_requests: (command_id, command * client_id option) H.t
  ; mutable unapplied_client_requests: command_id list * int
  ; client_results: (command_id, op_result) H.t
  ; switch: Lwt_switch.t
  ; event_batcher: P.event Batcher.t
  ; client_request_batcher: (client_id * command) Batcher.t }

let sync t =
  let l_p = L.sync t.core.log |> Lwt_result.get_exn in
  let t_p = T.sync t.core.current_term |> Lwt_result.get_exn in
  Lwt.join [t_p; l_p]

let rec advance_state_machine t = function
  | commit_index when Int64.(t.last_applied < commit_index) ->
      let res =
        let index = Int64.(succ t.last_applied) in
        let entry = L.get_exn t.core.log index in
        match H.find t.seen_client_requests entry.command_id with
        | None ->
            Log.err (fun m ->
                m "Have to get client command from other replicas") ;
            List.map t.core.config.other_nodes ~f:(fun id ->
                `RequestsAfter (id, index))
        | Some (cmd, id_o) -> (
            let result = StateMachine.update t.state_machine cmd in
            H.set t.client_results ~key:cmd.id ~data:result ;
            t.last_applied <- index ;
            match id_o with
            | Some cid ->
                Log.debug (fun m ->
                    m "Sending response for index %a cid %a to %a" Fmt.int64
                      index Fmt.int64 cmd.id Fmt.int64 cid) ;
                [`ClientResponse (result, cmd.id, cid)]
            | None ->
                [] )
      in
      res @ advance_state_machine t commit_index
  | _ ->
      []

let rec perform_action t cmgr : actions -> unit = function
  | `SendRequestVote (node_id, msg) ->
      Log.debug (fun m -> m "Sending request vote") ;
      MS.requestVote ~term:msg.term ~leaderCommit:msg.leader_commit
      |> UC.send cmgr node_id
      |> handle_failure_result "send_request_vote"
  | `SendRequestVoteResponse (node_id, msg) ->
      Log.debug (fun m -> m "Sending request vote response") ;
      let msg =
        MS.requestVoteResp ~term:msg.term ~voteGranted:msg.vote_granted
          ~entries:msg.entries ~startIndex:msg.start_index
      in
      sync t
      >>= (fun () -> UC.send cmgr node_id msg)
      |> handle_failure_result "send_request_vote_response"
  | `SendAppendEntries (node_id, msg) ->
      Log.debug (fun m -> m "Sending append entries") ;
      MS.appendEntries ~term:msg.term ~prevLogIndex:msg.prev_log_index
        ~prevLogTerm:msg.prev_log_term ~entries:msg.entries
        ~leaderCommit:msg.leader_commit
      |> UC.send cmgr node_id
      |> handle_failure_result "send_append_entries"
  | `SendAppendEntriesResponse (node_id, msg) ->
      Log.debug (fun m -> m "Sending append entries response") ;
      let msg = MS.appendEntriesResp ~term:msg.term ~success:msg.success in
      sync t
      >>= (fun () -> UC.send cmgr node_id msg)
      |> handle_failure_result "send_append_entries_response"
  | `RequestsAfter (node_id, index) ->
      Log.debug (fun m -> m "Requesting entries after %a" Fmt.int64 index) ;
      MS.requestsAfter ~index |> UC.send cmgr node_id
      |> handle_failure_result "send_requests_after"
  | `ClientResponse (result, cmd_id, cid) ->
      Log.debug (fun m ->
          m "Responding to %a for req %a" Fmt.int64 cid Fmt.int64 cmd_id) ;
      let msg = MS.clientResponse ~id:cmd_id ~result in
      sync t
      >>= (fun () -> UC.send cmgr cid msg)
      |> handle_failure_result "send_client_response"
  | `CommitIndexUpdate commit_index ->
      Log.debug (fun m ->
          m "Advancing state machine to commit index %a" Fmt.int64 commit_index) ;
      let actions = advance_state_machine t commit_index in
      List.iter ~f:(perform_action t cmgr) actions

let handle_advance_batch t cmgr events =
  List.iter events ~f:(fun event ->
      (* Due to global ocaml lock any changes within advance will be serialisable *)
      let t_core', actions = P.advance t.core event in
      t.core <- t_core' ;
      Log.debug (fun m -> m "Doing actions: %a" (Fmt.list P.pp_action) actions) ;
      List.iter ~f:(perform_action t cmgr) actions) ;
  Lwt.pause ()

let handle_client_request_batch t cmgr batch =
  let fold log_additions (src, command) =
    let open StateMachine in
    match H.find t.client_results command.id with
    | Some result ->
        Log.debug (fun m ->
            m "Got client request %a from %a but already has a result."
              Fmt.int64 command.id Fmt.int64 src) ;
        MS.clientResponse ~id:command.id ~result
        |> UC.send ~semantics:`AtLeastOnce cmgr src
        |> handle_failure_result "client_request_is_leader_result_exists." ;
        log_additions
    | None when L.id_in_log t.core.log command.id ->
        Log.debug (fun m -> m "Command already in log, but uncommitted.") ;
        H.set t.seen_client_requests ~key:command.id ~data:(command, Some src) ;
        log_additions
    | None when P.is_leader t.core ->
        Log.debug (fun m -> m "Command not in log, adding.") ;
        H.set t.seen_client_requests ~key:command.id ~data:(command, Some src) ;
        command.id :: log_additions
    | None ->
        Log.debug (fun m -> m "No result found for %a" Fmt.int64 command.id) ;
        H.set t.seen_client_requests ~key:command.id ~data:(command, Some src) ;
        log_additions
  in
  match List.fold batch ~init:[] ~f:fold with
  | [] ->
      ()
  | ids ->
      `LogAddition ids |> Batcher.auto_dispatch t.event_batcher

let handle_message t cmgr src msg =
  let () =
    let open Messaging.API.Reader in
    let open ServerMessage in
    match msg |> Capnp.BytesMessage.Message.readonly |> of_message |> get with
    | RequestVote msg ->
        Log.debug (fun m -> m "Got request vote from %a" Fmt.int64 src) ;
        let open RequestVote in
        let term = term_get msg in
        let leader_commit = leader_commit_get msg in
        `RRequestVote (src, Types.{term; leader_commit})
        |> Batcher.auto_dispatch t.event_batcher
    | RequestVoteResp msg ->
        Log.debug (fun m -> m "Got request vote response from %a" Fmt.int64 src) ;
        let open RequestVoteResp in
        let vote_granted = vote_granted_get msg in
        let term = term_get msg in
        let entries =
          entries_get_list msg |> List.map ~f:Messaging.log_entry_from_capnp
        in
        let start_index = start_index_get msg in
        `RRequestVoteResponse
          (src, Types.{vote_granted; term; entries; start_index})
        |> Batcher.auto_dispatch t.event_batcher
    | AppendEntries msg ->
        Log.debug (fun m -> m "Got append entries from %a" Fmt.int64 src) ;
        let open AppendEntries in
        let term = term_get msg in
        let prev_log_index = prev_log_index_get msg in
        let prev_log_term = prev_log_term_get msg in
        let entries =
          entries_get_list msg |> List.map ~f:Messaging.log_entry_from_capnp
        in
        let leader_commit = leader_commit_get msg in
        `RAppendEntries
          ( src
          , Types.{term; prev_log_index; prev_log_term; entries; leader_commit}
          )
        |> Batcher.auto_dispatch t.event_batcher
    | AppendEntriesResp msg ->
        Log.debug (fun m ->
            m "Got append entries response from %a" Fmt.int64 src) ;
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
        `RAppendEntiresResponse (src, Types.{term; success})
        |> Batcher.auto_dispatch t.event_batcher
    | ClientRequest msg ->
        Log.debug (fun m -> m "Got client request from %a" Fmt.int64 src) ;
        (src, Messaging.command_from_capnp msg)
        |> Batcher.auto_dispatch t.client_request_batcher
    | ClientResponse _msg ->
        raise (Invalid_argument "ClientResponse message")
    | RequestsAfter index ->
        let relevant_entries = L.entries_after_inc t.core.log index in
        let fold acc v =
          let open Continue_or_stop in
          match H.find t.seen_client_requests v.command_id with
          | Some (cmd, _) ->
              Continue (cmd :: acc)
          | None ->
              Stop acc
        in
        let commands =
          List.fold_until relevant_entries ~finish:(fun x -> x) ~init:[] ~f:fold
        in
        MS.requestUpdate ~commands |> UC.send cmgr src
        |> handle_failure_result "requests_after"
    | RequestsUpdate commands ->
        let iter command =
          let command = Messaging.command_from_capnp command in
          H.update t.seen_client_requests command.id ~f:(function
            | Some v ->
                v
            | None ->
                (command, None))
        in
        Capnp.Array.iter commands ~f:iter ;
        perform_action t cmgr (`CommitIndexUpdate t.core.commit_index)
    | Undefined i ->
        raise (Invalid_argument (Fmt.str "Undefined message of %d" i))
  in
  Lwt.return_ok ()

let ticker t cmgr s =
  let rec loop () =
    if Lwt_switch.is_on t.switch then
      Lwt_unix.sleep s
      >>= fun () -> handle_advance_batch t cmgr [`Tick] >>= fun () -> loop ()
    else Lwt.return_ok ()
  in
  loop () |> Lwt_result.get_exn

let create ~listen_address ~node_list ?(election_timeout = 5) ?(tick_time = 0.1)
    ?(retry_connection_timeout = 0.1) ?(log_path = "./log")
    ?(term_path = "./term") ?(event_batching = 1) ?(request_batching = 1)
    node_id =
  let other_node_list =
    List.filter_map node_list ~f:(fun ((i, _) as x) ->
        if Int64.(i <> node_id) then Some x else None)
  in
  (* Wait for at least one connection to be made *)
  let log_p = L.of_file log_path in
  let term_p = T.of_file term_path in
  Lwt.both log_p term_p
  >>= fun (log, term) ->
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
  let state_machine = StateMachine.create () in
  let last_applied = Int64.zero in
  let switch = Lwt_switch.create () in
  let ref_t = ref (fun _ -> assert false) in
  let ref_cmgr = ref (fun _ -> assert false) in
  let event_batcher =
    Batcher.create ~label:1 event_batching (fun batch ->
        handle_advance_batch (!ref_t ()) (!ref_cmgr ()) batch)
  in
  let handle_client_request_wrapper t c b =
    handle_client_request_batch (!t ()) (!c ()) b ;
    Lwt.return_unit
  in
  let client_request_batcher =
    Batcher.create ~label:2 request_batching
      (handle_client_request_wrapper ref_t ref_cmgr)
  in
  let rec t =
    { core= P.create_node config log term
    ; last_applied
    ; state_machine
    ; seen_client_requests= Hashtbl.create (module Int64)
    ; unapplied_client_requests= ([], 0)
    ; client_results= Hashtbl.create (module Int64)
    ; switch
    ; event_batcher
    ; client_request_batcher }
  in
  let cmgr =
    UC.create ~retry_connection_timeout ~listen_address ~node_id
      (handle_message t)
  in
  (ref_t := fun () -> t) ;
  (ref_cmgr := fun () -> cmgr) ;
  Lwt_switch.add_hook (Some switch) (fun () -> UC.close cmgr) ;
  let ps =
    match other_node_list with
    | [] ->
        [Lwt.return_unit]
    | _ ->
        List.map other_node_list ~f:(fun (id, addr) ->
            UC.add_outgoing cmgr id addr (`Persistant addr))
  in
  Lwt.choose ps
  >>= fun () ->
  Lwt.async (fun () -> ticker t cmgr tick_time) ;
  Lwt.return t

let close t =
  Lwt_switch.turn_off t.switch
  >>= fun () -> Lwt.join [T.close t.core.current_term; L.close t.core.log]

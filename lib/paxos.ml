open Base
open Types
open Messaging

let paxos = Logs.Src.create "Paxos" ~doc:"Paxos module"

module PL = (val Logs.src_log paxos : Logs.LOG)

type node_state =
  | Follower of {mutable last_recv_from_leader: time}
  | Candidate of {mutable entries: partial_log; mutable votes: string list}
  | Leader of
      { mutable nextIndex: (node_id, log_index) Lookup.t
      ; mutable matchIndex: (node_id, log_index) Lookup.t
      ; matchIndex_cond: unit Lwt_condition.t
      ; mutable heartbeat_last_sent: (node_id, time) Lookup.t }

type node =
  { (* --- Persistent State --------- *)
    mutable currentTerm: term
  ; mutable log: log (* --- Volatile State ----------- *)
  ; mutable commitIndex: log_index
  ; mutable lastApplied: log_index
  ; mutable node_state: node_state (* --- Implementation details --- *)
  ; state_machine: StateMachine.t (* - wait variables --- *)
  ; mutable client_request_result_w:
      (command_id, StateMachine.op_result Lwt.t) Lookup.t
  ; mutable client_request_result_f:
      (command_id, StateMachine.op_result Lwt.u) Lookup.t
        (* - condition vars --- *)
  ; commitIndex_cond: unit Lwt_condition.t
  ; log_cond: unit Lwt_condition.t (* - subsystem -------- *)
  ; msg_layer: Msg_layer.t
  ; config: config }

type 'a value_type =
  | CurrentTerm : term value_type
  | Log : (log * Log.op list) value_type
  | CommitIndex : log_index value_type
  | LastApplied : log_index value_type
  | NextIndex : (node_id, log_index) Lookup.t value_type
  | MatchIndex : (node_id, log_index) Lookup.t value_type

let value_type_to_string : type a. a value_type -> string = function
  | CurrentTerm ->
      "CurrentTerm"
  | Log ->
      "Log"
  | CommitIndex ->
      "CommitIndex"
  | LastApplied ->
      "LastApplied"
  | NextIndex ->
      "NextIndex"
  | MatchIndex ->
      "MatchIndex"

let update : type a. node -> a value_type -> a -> unit Lwt.t =
 fun t name value ->
  PL.debug (fun m -> m "Update: %s" (value_type_to_string name)) ;
  match name with
  | CurrentTerm ->
      t.currentTerm <- value ;
      Config.write_to_term t.config value
  | Log ->
      let log, ops = value in
      t.log <- log ;
      Lwt_condition.broadcast t.log_cond () ;
      Config.write_all_to_log t.config ops
  | CommitIndex ->
      t.commitIndex <- value ;
      Lwt_condition.broadcast t.commitIndex_cond () ;
      Lwt.return_unit
  | LastApplied ->
      t.lastApplied <- value ;
      Lwt.return_unit
  | NextIndex ->
      (match t.node_state with Leader s -> s.nextIndex <- value | _ -> ()) ;
      Lwt.return_unit
  | MatchIndex ->
      ( match t.node_state with
      | Leader s ->
          s.matchIndex <- value ;
          Lwt_condition.broadcast s.matchIndex_cond ()
      | _ ->
          () ) ;
      Lwt.return_unit

let merge_entries new_entries curr_entries =
  PL.debug (fun m -> m "merge_entries") ;
  let rec loop : log_entry list * log_entry list -> log_entry list = function
    | [], ys ->
        ys
    | xs, [] ->
        xs
    | new_entry :: new_entries, curr_entry :: curr_entries ->
        if new_entry.term > curr_entry.term then
          new_entry :: loop (new_entries, curr_entries)
        else curr_entry :: loop (new_entries, curr_entries)
  in
  loop (new_entries, curr_entries)

let send_AppendEntries t follower_id =
  PL.debug (fun m -> m "send_AppendEntries: call entry") ;
  match t.node_state with
  | Leader s -> (
      let ( let* ) r f = Result.bind r ~f in
      let result : (unit, exn) Result.t =
        let* nextIndex = Lookup.get s.nextIndex follower_id in
        if Log.get_max_index t.log >= nextIndex then (
          let prevLogIndex = nextIndex - 1 in
          let* prev_entry = Log.get t.log prevLogIndex in
          let prevLogTerm = prev_entry.term in
          let entries = Log.entries_after t.log nextIndex in
          if
            List.length entries > 0
            || Float.(
                 Lookup.get_exn s.heartbeat_last_sent follower_id
                 < t.config.idle_timeout)
            (* Something to send or hasn't sent anything in sufficient time *)
          then
            PL.debug (fun m ->
                m "send_AppendEntries: sending to %d" follower_id) ;
          { term= t.currentTerm
          ; prevLogIndex
          ; prevLogTerm
          ; entries
          ; leaderCommit= t.commitIndex }
          |> Msg_layer.send t.msg_layer ~msg_filter:append_entries_request
               ~dest:(Config.get_addr_from_id_exn t.config follower_id) ;
          Ok () )
        else Ok ()
      in
      match result with
      | Error exn ->
          raise exn
      | Ok _ ->
          s.heartbeat_last_sent <-
            Lookup.set s.heartbeat_last_sent ~key:follower_id
              ~data:(time_now ()) )
  | _ ->
      ()

(*---- Condition loops and state transitions -------*)

let commitIndex_cond_check t =
  let rec loop () =
    PL.debug (fun m -> m "commitIndex_cond_check: waiting") ;
    let%lwt () = Lwt_condition.wait t.commitIndex_cond in
    PL.debug (fun m -> m "commitIndex_cond_check: running") ;
    let rec update_SM () =
      match t.commitIndex > t.lastApplied with
      | false ->
          PL.debug (fun m -> m "commitIndex_cond_check: Updating SM") ;
          let%lwt () = update t LastApplied (t.lastApplied + 1) in
          let command = (Log.get_exn t.log t.lastApplied).command in
          (* lastApplied must exist by match statement *)
          let result = StateMachine.update t.state_machine command in
          let () =
            match Lookup.get t.client_request_result_f command.id with
            | Error _ ->
                ()
            | Ok fulfiller ->
                PL.debug (fun m ->
                    m "commitIndex_cond_check: Alerting fulfiller") ;
                Lwt.wakeup_later fulfiller result ;
                Lookup.remove t.client_request_result_f command.id
          in
          update_SM ()
      | true ->
          Lwt.return_unit
    in
    let%lwt () = update_SM () in
    loop ()
  in
  loop ()

let become_candidate t =
  PL.debug (fun m -> m "become_candidate") ;
  t.node_state <- Candidate {entries= []; votes= []} ;
  let updated_term : term =
    let id_in_current_epoch =
      let open Int in
      t.currentTerm - (t.currentTerm % t.config.num_nodes) + t.config.node_id
    in
    if id_in_current_epoch < t.currentTerm then
      id_in_current_epoch + t.config.num_nodes
    else id_in_current_epoch
  in
  PL.debug (fun m -> m "become_candidate: new term = %d" updated_term) ;
  let%lwt () = update t CurrentTerm updated_term in
  List.iter t.config.followers ~f:(fun (_id, addr) ->
      {term= t.currentTerm; leaderCommit= t.commitIndex}
      |> Msg_layer.send t.msg_layer ~msg_filter:request_vote_request ~dest:addr) ;
  Lwt.return_unit

let update_last_recv t =
  match t.node_state with
  | Follower s ->
      s.last_recv_from_leader <- time_now ()
  | _ ->
      ()

let election_timeout_expired_check t =
  let rec loop () =
    PL.debug (fun m -> m "election_timeout_expired_check: sleeping") ;
    let%lwt () = Lwt_unix.sleep t.config.election_timeout in
    PL.debug (fun m -> m "election_timeout_expired_check: ended sleep") ;
    match t.node_state with
    | Follower s ->
        if
          Float.(
            time_now () -. s.last_recv_from_leader > t.config.election_timeout)
        then (
          PL.debug (fun m ->
              m "election_timeout_expired_check: becomming candidate") ;
          (* Election timeout expired => leader most likely dead *)
          become_candidate t )
        else (
          (* If not becomming candidate then continue looping *)
          PL.debug (fun m ->
              m "election_timeout_expired_check: %f remaining"
                (time_now () -. s.last_recv_from_leader)) ;
          loop () )
    | _ ->
        Lwt.return_unit
  in
  loop

let become_follower t =
  PL.debug (fun m -> m "become_follower: call entry") ;
  t.node_state <- Follower {last_recv_from_leader= 0.0} ;
  Lwt.async (election_timeout_expired_check t)

let rec leader_heartbeat_check t () =
  PL.debug (fun m -> m "leader_heartbeat_check: call entry") ;
  match t.node_state with
  | Leader _ ->
      List.iter t.config.followers ~f:(fun (id, _) -> send_AppendEntries t id) ;
      PL.debug (fun m -> m "leader_heartbeat_check: sleeping") ;
      let%lwt () = Lwt_unix.sleep (t.config.election_timeout /. 2.1) in
      PL.debug (fun m -> m "leader_heartbeat_check: end sleep") ;
      leader_heartbeat_check t ()
  | _ ->
      PL.debug (fun m -> m "leader_heartbeat_check: call exit") ;
      Lwt.return_unit

let log_cond_check t =
  let rec loop () =
    PL.debug (fun m -> m "log_cond_check: waiting") ;
    let%lwt () = Lwt_condition.wait t.log_cond in
    PL.debug (fun m -> m "log_cond_check: got cond") ;
    match t.node_state with
    | Leader _ ->
        List.iter t.config.followers ~f:(fun (id, _addr) ->
            send_AppendEntries t id) ;
        loop ()
    | _ ->
        PL.debug (fun m -> m "log_cond_check: call exit") ;
        Lwt.return_unit
  in
  loop

let matchIndex_cond_check t =
  let rec loop () =
    match t.node_state with
    | Leader s ->
        PL.debug (fun m -> m "matchIndex_cond_check: waiting") ;
        let%lwt () = Lwt_condition.wait s.matchIndex_cond in
        PL.debug (fun m -> m "matchIndex_cond_check: got cond") ;
        let n =
          Lookup.fold s.matchIndex ~init:[] ~f:(fun data acc -> data :: acc)
          |> List.sort ~compare:(fun x y -> -Int.compare x y)
          |> fun ls -> List.nth_exn ls t.config.majority
          (* element exists bc defined as such *)
        in
        PL.debug (fun m -> m "matchIndex_cond_check: new commitIndex = %d" n) ;
        let%lwt () = update t CommitIndex n in
        loop ()
    | _ ->
        PL.debug (fun m -> m "matchIndex_cond_check: call exit") ;
        Lwt.return_unit
  in
  loop

let become_leader t =
  PL.debug (fun m -> m "become_leader:") ;
  let nextIndex =
    List.fold t.config.followers
      ~init:(Lookup.create (module Int))
      ~f:(fun acc (id, _addr) ->
        Lookup.set acc ~key:id ~data:(t.commitIndex + 1))
  in
  let matchIndex =
    List.fold t.config.followers
      ~init:(Lookup.create (module Int))
      ~f:(fun acc (id, _addr) -> Lookup.set acc ~key:id ~data:0)
  in
  let heartbeat_last_sent =
    List.fold t.config.followers
      ~init:(Lookup.create (module Int))
      ~f:(fun acc (id, _addr) -> Lookup.set acc ~key:id ~data:0.0)
  in
  t.node_state <-
    Leader
      { nextIndex
      ; matchIndex
      ; matchIndex_cond= Lwt_condition.create ()
      ; heartbeat_last_sent } ;
  (* Ensure no election timeouts *)
  Lwt.async (leader_heartbeat_check t) ;
  (* Ensure followers are kept up to date *)
  Lwt.async (log_cond_check t) ;
  (* Ensure commitIndex is correct *)
  Lwt.async (matchIndex_cond_check t)

let preempted_check t term =
  if term > t.currentTerm then (
    PL.debug (fun m -> m "preempted_check: preempted by term %d" term) ;
    let%lwt () = update t CurrentTerm term in
    Lwt.return @@ become_follower t )
  else Lwt.return_unit

(*---- Core Paxos RPCs -----------------------------*)

let recv_RequestVote_req t src (msg : request_vote_request) =
  PL.debug (fun m -> m "recv_RequestVote_req: received request from %s" src) ;
  let%lwt () = preempted_check t msg.term in
  let voteGranted = msg.term < t.currentTerm in
  if voteGranted then (
    PL.debug (fun m -> m "recv_RequestVote_req: vote granted") ;
    update_last_recv t ) ;
  let entries = Log.entries_after t.log msg.leaderCommit in
  {term= t.currentTerm; voteGranted; entries}
  |> Msg_layer.send t.msg_layer ~msg_filter:request_vote_response ~dest:src ;
  Lwt.return_unit

let recv_RequestVote_rep t src (msg : request_vote_response) =
  PL.debug (fun m ->
      m "recv_RequestVote_rep: received response from %s for term %d" src
        msg.term) ;
  let%lwt () = preempted_check t msg.term in
  match t.node_state with
  | Candidate r when Int.(msg.term = t.currentTerm) ->
      if List.mem r.votes src ~equal:String.equal then (
        PL.debug (fun m ->
            m "recv_RequestVote_rep: Not received a vote from node %s" src) ;
        (* If vote unreceived previously *)
        r.votes <- src :: r.votes ;
        r.entries <- merge_entries msg.entries r.entries ;
        if List.length r.votes > t.config.majority then (
          PL.debug (fun m -> m "recv_RequestVote_rep: elected Leader") ;
          (* elected leader *)
          let entries_to_add =
            List.map r.entries ~f:(fun entry ->
                {term= t.currentTerm; command= entry.command; index= entry.index})
          in
          let%lwt () =
            (* ops will be in newest -> oldest ordering *)
            let log, ops =
              List.fold_left entries_to_add ~init:(t.log, [])
                ~f:(fun (log, ops_acc) entry ->
                  let log, ops = Log.set log ~index:entry.index ~value:entry in
                  (log, ops @ ops_acc))
            in
            PL.debug (fun m ->
                m "recv_RequestVote_rep: updating log with new ops") ;
            update t Log (log, List.rev ops)
          in
          become_leader t ; Lwt.return_unit )
        else Lwt.return_unit )
      else (
        PL.debug (fun m -> m "recv_RequestVote_rep: already received response") ;
        Lwt.return_unit )
  | _
  (* Follower: If receiving then previous request vote request failed
   * thus don't do anything TODO check? *)
  (* Leader: Either msg.term < t.currentTerm => don't care,
   * or msg.term = t.currentTerm => don't care *)
  (* Candidate receiving RequestVote_rep for term < t.currentTerm 
   * => old response => ignore *) ->
      PL.debug (fun m -> m "recv_RequestVote_rep: not correct node_state") ;
      Lwt.return_unit

let recv_AppendEntries_req t src (msg : append_entries_request) =
  PL.debug (fun m -> m "recv_AppendEntries_req: received msg from %s" src) ;
  let%lwt () = preempted_check t msg.term in
  match (msg.term < t.currentTerm, Log.get t.log msg.prevLogIndex) with
  | false, Ok entry when entry.term = msg.prevLogTerm ->
      PL.debug (fun m -> m "recv_AppendEntries_req: received valid") ;
      update_last_recv t ;
      PL.debug (fun m ->
          m "recv_AppendEntries_req: log removing conflicts and adding entries") ;
      let%lwt () =
        update t Log @@ Log.add_entries_remove_conflicts t.log msg.entries
      in
      let%lwt () =
        if msg.leaderCommit > t.commitIndex then (
          PL.debug (fun m ->
              m
                "recv_AppendEntries_req: Updating commit index to that of \
                 leader") ;
          (* if a last entry exists, take min, otherwise leaderCommit *)
          match List.last msg.entries with
          | Some last ->
              update t CommitIndex @@ min last.index msg.leaderCommit
          | None ->
              update t CommitIndex msg.leaderCommit )
        else Lwt.return_unit
      in
      {term= t.currentTerm; success= true; matchIndex= Log.get_max_index t.log}
      |> Msg_layer.send t.msg_layer ~msg_filter:append_entries_response
           ~dest:src ;
      Lwt.return_unit
  | _ ->
      PL.debug (fun m -> m "recv_AppendEntries_req: responding negative") ;
      (* term < currentTerm or log inconsistent *)
      {term= t.currentTerm; success= false; matchIndex= Log.get_max_index t.log}
      |> Msg_layer.send t.msg_layer ~msg_filter:append_entries_response
           ~dest:src ;
      Lwt.return_unit

let recv_AppendEntries_rep t source (msg : append_entries_response) =
  PL.debug (fun m -> m "recv_AppendEntries_rep: received msg from %s" source) ;
  let%lwt () = preempted_check t msg.term in
  match t.node_state with
  | Leader s ->
      let follower_id = Config.get_id_from_addr_exn t.config source in
      let%lwt () =
        if msg.success then (
          PL.debug (fun m -> m "recv_AppendEntries_rep: msg was a success") ;
          if msg.matchIndex >= Lookup.get_exn s.matchIndex follower_id then (
            PL.debug (fun m ->
                m "recv_AppendEntries_rep: not old msg => updating state") ;
            let%lwt () =
              update t NextIndex
              @@ Lookup.set s.nextIndex ~key:follower_id
                   ~data:(msg.matchIndex + 1)
            in
            update t MatchIndex
            @@ Lookup.set s.matchIndex ~key:follower_id ~data:msg.matchIndex )
          else Lwt.return_unit (* old appendEntries response*) )
        else (
          PL.debug (fun m ->
              m "recv_AppendEntries_rep: need to retry AppendEntires") ;
          (* need to retry *)
          let%lwt () =
            update t NextIndex
            @@ Lookup.set s.nextIndex ~key:follower_id
                 ~data:(Lookup.get_exn s.nextIndex follower_id - 1)
          in
          Lwt.return @@ send_AppendEntries t follower_id )
      in
      Lwt.return_unit
  | _ ->
      Lwt.return_unit

let recv_client_request t (msg : client_request) =
  PL.debug (fun m -> m "recv_client_request: received client_request") ;
  match (t.node_state, Lookup.get t.client_request_result_w msg.command.id) with
  | Leader _s, Error _ ->
      PL.debug (fun m -> m "recv_client_request: Leader and unfulfilled") ;
      let index = Log.get_max_index t.log + 1 in
      let%lwt () =
        update t Log
        @@ Log.set t.log ~index
             ~value:{command= msg.command; term= t.currentTerm; index}
      in
      let waiter, fulfiller = Lwt.task () in
      (* Set up responses *)
      PL.debug (fun m -> m "recv_client_request: setting up fulfillers") ;
      t.client_request_result_w <-
        Lookup.set t.client_request_result_w ~key:msg.command.id ~data:waiter ;
      t.client_request_result_f <-
        Lookup.set t.client_request_result_f ~key:msg.command.id ~data:fulfiller ;
      (* Wait for result from system *)
      PL.debug (fun m -> m "recv_client_request: waiting for result") ;
      let%lwt result = waiter in
      PL.debug (fun m -> m "recv_client_request: got result") ;
      Lwt.return_some {result}
  | _, Ok res ->
      PL.debug (fun m -> m "recv_client_request: Already submitted") ;
      let%lwt result = res in
      PL.debug (fun m -> m "recv_client_request: fulfilled") ;
      Lwt.return_some {result}
  | _ ->
      Lwt.return_none

(*---- Creation etc --------------------------------*)

let create_and_start ~data_path ~node_list ~node_addr ~client_port
    ~election_timeout =
  let open Config in
  let log_path = data_path ^ ".log" in
  let term_path = data_path ^ ".term" in
  let%lwt log = Log.get_log_from_file log_path in
  let log_fd = Unix.openfile log_path [Unix.O_RDWR] 0o640 in
  let log_file =
    {fd= log_fd; channel= Lwt_io.of_unix_fd ~mode:Lwt_io.output log_fd}
  in
  let term_fd = Unix.openfile term_path [Unix.O_RDWR] 0o640 in
  let term_file =
    {fd= term_fd; channel= Lwt_io.of_unix_fd ~mode:Lwt_io.output term_fd}
  in
  let sm = StateMachine.create () in
  let%lwt msg_layer, m_ps =
    Msg_layer.create
      ~node_list:(node_list |> List.map ~f:(fun (addr, uri, _id) -> (addr, uri)))
      ~id:node_addr
  in
  let node_state = Follower {last_recv_from_leader= 0.} in
  let followers =
    node_list |> List.map ~f:(fun (addr, _uri, id) -> (id, addr))
  in
  let node_id =
    followers |> List.Assoc.inverse
    |> fun ls -> List.Assoc.find_exn ls node_addr ~equal:String.equal
  in
  let%lwt currentTerm = get_term_from_file term_path in
  let currentTerm = Int.max currentTerm node_id in
  let config =
    { majority= Int.((List.length node_list / 2) + 1)
    ; followers
    ; election_timeout
    ; idle_timeout= Float.(election_timeout / 2.)
    ; log_file
    ; term_file
    ; num_nodes= List.length node_list
    ; node_id }
  in
  let t =
    { currentTerm
    ; log
    ; commitIndex= 0
    ; lastApplied= 0
    ; node_state
    ; state_machine= sm
    ; client_request_result_w= Lookup.create (module Int)
    ; client_request_result_f= Lookup.create (module Int)
    ; commitIndex_cond= Lwt_condition.create ()
    ; log_cond= Lwt_condition.create ()
    ; msg_layer
    ; config }
  in
  Msg_layer.attach_watch_src msg_layer ~msg_filter:request_vote_request
    ~callback:(recv_RequestVote_req t) ;
  Msg_layer.attach_watch_src msg_layer ~msg_filter:request_vote_response
    ~callback:(recv_RequestVote_rep t) ;
  Msg_layer.attach_watch_src msg_layer ~msg_filter:append_entries_request
    ~callback:(recv_AppendEntries_req t) ;
  Msg_layer.attach_watch_src msg_layer ~msg_filter:append_entries_response
    ~callback:(recv_AppendEntries_rep t) ;
  let client_promise =
    Msg_layer.client_socket msg_layer ~callback:(recv_client_request t)
      ~port:client_port
  in
  let%lwt () = become_candidate t in
  Lwt.join [client_promise; m_ps; commitIndex_cond_check t]

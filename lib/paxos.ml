open Base
open Types
open Messaging
open Utils
open Lwt.Infix
open Unix_capnp_messaging
open Owal

let ( >><= ) = Result.( >>= )
let ( >>>= ) = Lwt_result.bind

let failif s x =
  match x with
  | Ok v ->
      Lwt.return v
  | Error exn ->
      Fmt.failwith "Failed during %s with %a" s Fmt.exn exn

let paxos = Logs.Src.create "Paxos" ~doc:"Paxos module"

module L = (val Logs.src_log paxos : Logs.LOG)

module PaxosTypes = struct
  type election_callback = (node_id, log_entry list) List.Assoc.t -> unit

  type req_vote_quorum = (node_id, log_entry list) Quorum.t

  type config =
    { quorum_gen: election_callback -> req_vote_quorum
    ; majority: int
    ; node_list: node_id list
    ; other_node_list: node_id list
    ; num_nodes: int
    ; cmgr: Conn_manager.t
    ; election_timeout: float
    ; idle_timeout: float
    ; node_id: node_id }

  module Log : sig
    type t

    val sync : t -> (unit, exn) Lwt_result.t

    type entry_list = log_entry list

    val of_file : node_addr -> t Lwt.t

    val get : t -> log_index -> (log_entry, exn) Result.t

    val get_exn : t -> log_index -> log_entry

    val get_term : t -> log_index -> (term, exn) Result.t

    val get_term_exn : t -> log_index -> term

    val set : t -> index:log_index -> value:log_entry -> t

    val remove : t -> log_index -> t

    val get_max_index : t -> log_index

    val entries_after_inc : t -> index:log_index -> entry_list

    val to_string : t -> string

    val add_entries_remove_conflicts : t -> entry_list -> t

    val append : t -> command -> term -> t
  end = struct
    (* Common operations:
        - Get max_index
        - Indices after
        - Get specific index (close to end generally)
        - Append to end of log
    *)
    module P_t = struct
      type t = (log_index, log_entry) Lookup.t

      let init () = Lookup.create (module Int)

      type op =
        | Set of log_index * log_entry [@key 1]
        | Remove of log_index [@key 2]
      [@@deriving protobuf]

      let encode_blit op =
        let res = Protobuf.Encoder.encode_exn op_to_protobuf op in
        let p_len = Bytes.length res in
        let blit buf ~offset =
          Bytes.blit ~src:res ~src_pos:0 ~dst:buf ~dst_pos:offset ~len:p_len
        in
        (p_len, blit)

      let decode buf ~offset =
        let decode_buf =
          Bytes.sub buf ~pos:offset ~len:(Bytes.length buf - offset)
        in
        Protobuf.Decoder.decode_exn op_from_protobuf decode_buf

      let apply t op =
        match op with
        | Set (index, value) ->
            Lookup.set t ~key:index ~data:value
        | Remove index ->
            Lookup.remove t index
    end

    include P_t
    module P = Persistant (P_t)
    include P

    type t = P.t

    type entry_list = log_entry list (* Lowest index first *)

    let get (t : t) = Lookup.get t.t

    let get_exn t = Lookup.get_exn t.t

    let get_term t index =
      match index with
      | 0 ->
          Ok 0
      | _ ->
          get t index >><= fun entry -> Ok entry.term

    let get_term_exn t index =
      match get_term t index with Ok v -> v | Error exn -> raise exn

    let set t ~index ~value =
      Logs.debug (fun m -> m "Setting %d to %s" index (string_of_entry value)) ;
      change t (Set (index, value)) ;
      t

    let remove t i = change t (Remove i) ; t

    let get_max_index (t : t) =
      Lookup.fold t.t ~init:0 ~f:(fun v acc -> Int.max v.index acc)

    (*
    (* Entries after i inclusive *)
    let entries_after_inc log ~index : entry_list =
      let index = if index = 0 then 1 else 0 in
      let rec loop i acc =
        match get log index with
        | Ok v ->
            loop (i + 1) (v :: acc)
        | Error _ ->
            acc
      in
      List.rev @@ loop index []
        *)

    let entries_after_inc t ~index : entry_list =
      let res =
        Hashtbl.fold t.t ~init:[] ~f:(fun ~key ~data acc ->
            if key >= index then data :: acc else acc)
      in
      List.sort res ~compare:(fun a b -> Int.compare a.index b.index)

    let to_string t =
      let entries = entries_after_inc t ~index:1 in
      string_of_entries entries

    let add_entries_remove_conflicts t (entries : entry_list) =
      let rec delete_geq t i =
        match get t i with
        | Ok v ->
            assert (Int.(v.index = i)) ;
            change t (Remove i) ;
            (delete_geq [@tailcall]) t (i + 1)
        | Error _ ->
            t
      in
      List.fold_left entries ~init:t ~f:(fun t e ->
          match get t e.index with
          | Ok {term; command; _}
            when Int.(term = e.term)
                 && StateMachine.command_equal command e.command ->
              (* no log inconsistency *)
              t
          | Error _ ->
              (* No entry at index *)
              change t (Set (e.index, e)) ;
              t
          | Ok _ ->
              ((* inconsitent log *)
               let t = delete_geq t e.index in
               change t (Set (e.index, e))) ;
              t)

    let append t command term =
      let max_index = get_max_index t in
      let index = max_index + 1 in
      let entry = {command; term; index} in
      change t (Set (index, entry)) ;
      t
  end

  type log = Log.t

  module Term_t = struct
    type t = term

    let init () = 0

    type op = int

    let encode_blit v =
      let p_len = 8 in
      let v = Int64.of_int v in
      let blit buf ~offset = EndianBytes.LittleEndian.set_int64 buf offset v in
      (p_len, blit)

    let decode v ~offset =
      EndianBytes.LittleEndian.get_int64 v offset |> Int64.to_int_exn

    let apply _t op = op
  end

  module Term = Persistant (Term_t)

  type host = node_id

  type node_state =
    | Follower of {mutable last_recv_from_leader: time}
    | Candidate of {term: term; quorum: req_vote_quorum}
    | Leader of
        { mutable nextIndex: (node_id, log_index) Lookup.t
        ; mutable matchIndex: (node_id, log_index) Lookup.t
        ; matchIndex_cond: unit Lwt_condition.t
        ; mutable heartbeat_last_sent: (node_id, time) Lookup.t
        ; term: term }

  type t =
    { mutable node_state: node_state
    ; mutable currentTerm: Term.t
    ; mutable log: log
    ; mutable state_machine: StateMachine.t
    ; mutable commitIndex: log_index
    ; mutable lastApplied: log_index
    ; config: config
    ; commitIndex_cond: unit Lwt_condition.t
    ; log_cond: unit Lwt_condition.t
    ; mutable client_result_forwarders: (command_id, host) Lookup.t
    ; mutable client_resolvers: (command_id, StateMachine.op_result option) Hashtbl.t
    ; mutable is_leader: unit Lwt_condition.t }

  let update_current_term t = function
    | v when Int.(v = t.currentTerm.t) ->
        Lwt.return_unit
    | v ->
        L.debug (fun m -> m "Updating currentTerm to %d" v) ;
        (* This ordering should preserve correctness of execution *)
        Term.change t.currentTerm v ;
        L.debug (fun m -> m "Syncing term") ;
        Term.sync t.currentTerm >>= failif "Sync"
        >>= fun () ->
        L.debug (fun m -> m "Sync'd term") ;
        Lwt.return_unit

  let update_log t log =
    t.log <- log ;
    Lwt_condition.broadcast t.log_cond ()

  let update_commit_index t v =
    L.debug (fun m -> m "Updating commitIndex to %d" v) ;
    t.commitIndex <- v ;
    Lwt_condition.broadcast t.commitIndex_cond ()

  let update_last_applied t v =
    L.debug (fun m -> m "Updating lastApplied to %d" v) ;
    t.lastApplied <- v

  let update_next_index t node ni =
    L.debug (fun m -> m "Updating NextIndex for %a to %d" Fmt.int64 node ni) ;
    match t.node_state with
    | Leader s ->
        s.nextIndex <- Lookup.set s.nextIndex ~key:node ~data:ni
    (* This will cause many updates to be sent out, otherwise is capped
       if Log.get_max_index t.log >= ni then
         Lwt_condition.broadcast t.log_cond ()
    *)
    | _ ->
        ()

  let update_match_index t node data =
    L.debug (fun m -> m "Updating MatchIndex for %a to %d" Fmt.int64 node data) ;
    match t.node_state with
    | Leader s ->
        s.matchIndex <- Lookup.set s.matchIndex ~key:node ~data ;
        update_next_index t node (data + 1) ;
        Lwt_condition.broadcast s.matchIndex_cond ()
    | _ ->
        ()

  let sync_term_log t =
    let pl = Log.sync t.log >>= failif "sync log" in
    let pt = Term.sync t.currentTerm >>= failif "sync term" in
    Lwt.join [pl; pt]

  let send_append_entries t ~leader_term ~host =
    (* Sync log to disk before sending anything *)
    match t.node_state with
    | Leader {nextIndex; term; _} when term = leader_term -> (
        let entries_start_index = Lookup.get_exn nextIndex host in
        let entries = Log.entries_after_inc t.log ~index:entries_start_index in
        let prevLogIndex = entries_start_index - 1 in
        let prevLogTerm = Log.get_term_exn t.log prevLogIndex in
        let leaderCommit = t.commitIndex in
        L.debug (fun m ->
            m "append_entries: sending request to %a: (%d %d %d %d)" Fmt.int64
              host term prevLogIndex prevLogTerm leaderCommit) ;
        sync_term_log t
        >>= fun () ->
        Send.appendEntries t.config.cmgr host ~term ~prevLogIndex ~prevLogTerm
          ~entries ~leaderCommit
        >|= function
        | Ok () ->
            ()
        | Error exn ->
            L.err (fun m ->
                m "Failed to send append_entries with %a" Fmt.exn exn) )
    | _ ->
        L.err (fun m ->
            m "Failed to send append entries because not the leader") ;
        Lwt.return_unit
end

open PaxosTypes

module rec Transition : sig
  val follower : t -> unit

  val candidate : t -> unit Lwt.t

  val leader : t -> term -> log_entry list list -> unit Lwt.t
end = struct
  let follower t =
    L.info (fun m -> m "Becomming a follower") ;
    t.node_state <- Follower {last_recv_from_leader= time_now ()} ;
    Lwt.async (Condition_checks.election_timeout_expired t)

  let leader t term entries =
    match t.node_state with
    | Candidate {term= cterm; quorum= _} when Int.(cterm = term) ->
        L.info (fun m -> m "Elected leader of term %d" t.currentTerm.t) ;
        (*L.debug (fun m -> m "Entries: %s" (string_of_entries_list entries)) ;*)
        let merge xs ys =
          let rec loop = function
            | [], ys ->
                ys
            | xs, [] ->
                xs
            | x :: xs, y :: ys ->
                assert (Int.(x.index = y.index)) ;
                (if x.term > y.term then x else y) :: loop (xs, ys)
          in
          loop (xs, ys)
        in
        let entries_to_add =
          entries
          |> List.fold ~init:[] ~f:merge
          |> List.map ~f:(fun entry ->
                 { term= t.currentTerm.t
                 ; command= entry.command
                 ; index= entry.index })
          |> List.rev
        in
        (* Update log *)
        let log =
          List.fold_left entries_to_add ~init:t.log ~f:(fun log entry ->
              let log = Log.set log ~index:entry.index ~value:entry in
              log)
        in
        L.debug (fun m -> m "Adding ops to log") ;
        update_log t log ;
        let nextIndex, matchIndex, heartbeat_last_sent =
          List.fold t.config.node_list
            ~init:
              ( Lookup.create (module Int64)
              , Lookup.create (module Int64)
              , Lookup.create (module Int64) )
            ~f:(fun (nI, mI, hb) id ->
              ( Lookup.set nI ~key:id ~data:(t.commitIndex + 1)
              , Lookup.set mI ~key:id ~data:0
              , Lookup.set hb ~key:id ~data:0. ))
        in
        t.node_state <-
          Leader
            { nextIndex
            ; matchIndex
            ; heartbeat_last_sent
            ; matchIndex_cond= Lwt_condition.create ()
            ; term } ;
        (* Alert unsubmitted client requests *)
        Lwt_condition.broadcast t.is_leader () ;
        (* Ensure no election timeouts *)
        Lwt.async (Condition_checks.leader_heartbeat t) ;
        (* Ensure followers are kept up to date *)
        Lwt.async (Condition_checks.log t term) ;
        (* Ensure commitIndex is correct *)
        Lwt.async (Condition_checks.matchIndex t) ;
        Lwt.return_unit
    | _ ->
        Lwt.return_unit

  let rec candidate t =
    let updated_term : term =
      let id_in_current_epoch =
        Int.(
          t.currentTerm.t
          - (t.currentTerm.t % t.config.num_nodes)
          + (t.config.node_id |> Int64.to_int_exn))
      in
      if id_in_current_epoch <= t.currentTerm.t then
        id_in_current_epoch + t.config.num_nodes
      else id_in_current_epoch
    in
    L.info (fun m -> m "Becomming candidate for term %d" updated_term) ;
    update_current_term t updated_term
    >>= fun () ->
    let term = t.currentTerm.t in
    (* When elected is called *)
    let election_callback res =
      let res = List.map res ~f:snd in
      Lwt.async (fun () -> leader t updated_term res)
    in
    let quorum = t.config.quorum_gen election_callback in
    t.node_state <- Candidate {term; quorum} ;
    let commitIndex = t.commitIndex in
    List.iter t.config.other_node_list ~f:(fun url ->
        Lwt.async (fun () ->
            Messaging.Send.requestVote t.config.cmgr url ~term:updated_term
              ~leaderCommit:commitIndex
            |> Lwt_result.get_exn)) ;
    let redo_election_if_unelected () =
      Lwt_unix.sleep t.config.election_timeout
      >>= fun () ->
      match t.node_state with
      | Candidate {term= term'; _} when term' = term ->
          L.err (fun m -> m "Not elected within timeout, retrying") ;
          candidate t
      | _ ->
          Lwt.return_unit
    in
    Lwt.async redo_election_if_unelected ;
    quorum.add (t.config.node_id, []) ;
    Lwt.return_unit
end

and Condition_checks : sig
  val commitIndex : t -> unit -> unit Lwt.t

  val log : t -> term -> unit -> unit Lwt.t

  val election_timeout_expired : t -> unit -> unit Lwt.t

  val leader_heartbeat : t -> unit -> unit Lwt.t

  val matchIndex : t -> unit -> unit Lwt.t
end = struct
  let rec update_SM t () =
    if t.commitIndex > t.lastApplied then (
      L.debug (fun m -> m "commitIndex_cond_check: Updating SM") ;
      update_last_applied t (t.lastApplied + 1) ;
      let entry = Log.get_exn t.log t.lastApplied in
      let command = entry.command in
      (* lastApplied must exist by match statement *)
      let result = StateMachine.update t.state_machine command in
      let result_handler () =
        match Lookup.get t.client_result_forwarders command.id with
        | Ok src -> (
            t.client_result_forwarders <-
              Lookup.remove t.client_result_forwarders command.id ;
            L.debug (fun m ->
                m "Got answer for %a at index %d sending to %a" Fmt.int64 command.id
                  entry.index Fmt.int64 src) ;
            Send.clientResponse ~sem:`AtLeastOnce t.config.cmgr src
              ~id:command.id ~result
            >>= function
            | Ok () ->
                Lwt.return_unit
            | Error exn ->
                L.err (fun m ->
                    m "Failed to reply to client with %a" Fmt.exn exn) ;
                Lwt.return_unit )
        | Error _ ->
            L.debug (fun m ->
                m "No forwarder for %a at index %d" Fmt.int64 command.id entry.index) ;
            Lwt.return_unit
      in
      Lwt.async result_handler ;
      (update_SM [@tailcall]) t () )
    else Lwt.return_unit

  let commitIndex t =
    let rec loop () =
      L.debug (fun m -> m "commitIndex_cond_check: waiting") ;
      Lwt_condition.wait t.commitIndex_cond
      >>= fun () ->
      L.debug (fun m -> m "commitIndex_cond_check: running") ;
      update_SM t () >>= fun () -> (loop [@tailcall]) ()
    in
    loop

  let log t leader_term () =
    let rec loop () =
      match t.node_state with
      | Leader {nextIndex; term; _} when Int.(term = leader_term) ->
          Lwt_condition.wait t.log_cond
          >>= fun () ->
          let max_log_index = Log.get_max_index t.log in
          update_match_index t t.config.node_id max_log_index ;
          let send_fn host =
            let id = host in
            let next_index = Lookup.get_exn nextIndex id in
            if max_log_index >= next_index then
              Lwt.async (fun () -> send_append_entries t ~leader_term ~host)
          in
          List.iter t.config.other_node_list ~f:send_fn ;
          (loop [@tailcall]) ()
      | _ ->
          Lwt.return_unit
    in
    loop ()

  let rec leader_heartbeat t () =
    match t.node_state with
    | Leader {heartbeat_last_sent; _} ->
        List.iter t.config.other_node_list ~f:(fun host ->
            match Lookup.get_exn heartbeat_last_sent host with
            | lastSent when Float.(lastSent < t.config.idle_timeout) ->
                L.debug (fun m -> m "managed lookup, timed out") ;
                Lwt.async (fun () ->
                    send_append_entries t ~leader_term:t.currentTerm.t ~host)
            | _ ->
                L.debug (fun m ->
                    m "managed lookup, don't need to send keepalive") ;
                ()) ;
        L.debug (fun m -> m "leader_heartbeat_check: sleeping") ;
        Lwt_unix.sleep (t.config.election_timeout /. 2.1)
        >>= fun () ->
        L.debug (fun m -> m "leader_heartbeat_check: end sleep") ;
        leader_heartbeat t ()
    | _ ->
        L.debug (fun m -> m "leader_heartbeat_check: call exit") ;
        Lwt.return_unit

  let election_timeout_expired t =
    let rec loop () =
      L.debug (fun m -> m "election_timeout_expired_check: sleeping") ;
      Lwt_unix.sleep t.config.election_timeout
      >>= fun () ->
      L.debug (fun m -> m "election_timeout_expired_check: ended sleep") ;
      match t.node_state with
      | Follower s ->
          if
            Float.(
              time_now () -. s.last_recv_from_leader > t.config.election_timeout)
          then (
            L.err (fun m ->
                m "election_timeout_expired_check: becomming candidate") ;
            (* Election timeout expired => leader most likely dead *)
            Transition.candidate t )
          else
            (* If not becomming candidate then continue looping *)
            (loop [@tailcall]) ()
      | _ ->
          Lwt.return_unit
    in
    loop

  let matchIndex t =
    let rec loop () =
      match t.node_state with
      | Leader {matchIndex_cond; matchIndex; _} ->
          L.debug (fun m -> m "matchIndex_cond_check: waiting") ;
          Lwt_condition.wait matchIndex_cond
          >>= fun () ->
          let n =
            Lookup.fold matchIndex ~init:[] ~f:(fun data acc -> data :: acc)
            |> List.sort ~compare:(fun x y -> -Int.compare x y)
            |> fun ls -> List.nth_exn ls (t.config.majority - 1)
            (* element exists bc defined as such *)
          in
          L.debug (fun m -> m "matchIndex_cond_check: new commitIndex = %d" n) ;
          update_commit_index t n ;
          (loop [@tailcall]) ()
      | _ ->
          L.debug (fun m -> m "matchIndex_cond_check: call exit") ;
          Lwt.return_unit
    in
    loop
end

module CoreRpcServer = struct
  type nonrec t = t

  let update_last_recv t =
    match t.node_state with
    | Follower s ->
        s.last_recv_from_leader <- time_now ()
    | _ ->
        ()

  let preempted_check t term =
    if term > t.currentTerm.t then (
      L.debug (fun m -> m "preempted_check: preempted by term %d" term) ;
      update_current_term t term
      >>= fun () -> Transition.follower t ; Lwt.return_unit )
    else Lwt.return_unit

  let with_msg f msg =
    f msg
    >>= fun () ->
    Messaging.API.MessageWrapper.Message.release msg ;
    Lwt.return_unit

  open Messaging.API.Reader

  let handle_request_vote t src msg =
    let term = RequestVote.term_get_int_exn msg in
    L.info (fun m -> m "RequestVote from %a for %d" Fmt.int64 src term) ;
    if term < t.currentTerm.t then (
      L.debug (fun m ->
          m "request_vote: vote not granted to %a for term %d" Fmt.int64 src
            term) ;
      sync_term_log t
      >>= fun () ->
      Send.requestVoteResp t.config.cmgr src ~term:t.currentTerm.t
        ~voteGranted:false ~entries:[] )
    else
      preempted_check t term
      >>= fun () ->
      L.debug (fun m ->
          m "request_vote: vote granted to %a for term %d" Fmt.int64 src term) ;
      update_last_recv t ;
      let leaderCommit = RequestVote.leader_commit_get_int_exn msg in
      let entries = Log.entries_after_inc t.log ~index:leaderCommit in
      sync_term_log t
      >>= fun () ->
      Send.requestVoteResp t.config.cmgr src ~term:t.currentTerm.t
        ~voteGranted:true ~entries

  let handle_request_vote_resp t src msg =
    match t.node_state with
    | Candidate {term; quorum}
      when RequestVoteResp.vote_granted_get msg
           && RequestVoteResp.term_get_int_exn msg = term ->
        L.debug (fun m -> m "Got vote granted from %a" Fmt.int64 src) ;
        let entries =
          RequestVoteResp.entries_get_list msg
          |> List.map ~f:Messaging.log_entry_from_capnp
        in
        quorum.add (src, entries) ;
        Lwt.return_ok ()
    | _ ->
        Lwt.return_ok ()

  let handle_append_entries t src msg : (unit, exn) Lwt_result.t =
    sync_term_log t
    >>= fun () ->
    let term = AppendEntries.term_get_int_exn msg in
    let prevLogIndex = AppendEntries.prev_log_index_get_int_exn msg in
    let prevLogTerm = AppendEntries.prev_log_term_get_int_exn msg in
    let leaderCommit = AppendEntries.leader_commit_get_int_exn msg in
    L.info (fun m ->
        m "append_entries: received request from %a: (%d %d %d %d)" Fmt.int64
          src term prevLogIndex prevLogTerm leaderCommit) ;
    match (term < t.currentTerm.t, Log.get_term t.log prevLogIndex) with
    | false, Ok logterm when logterm = prevLogTerm ->
        preempted_check t term
        >>= fun () ->
        L.debug (fun m -> m "append_entries: received valid") ;
        update_last_recv t ;
        let entries =
          AppendEntries.entries_get_list msg
          |> List.map ~f:Messaging.log_entry_from_capnp
        in
        let log' = Log.add_entries_remove_conflicts t.log entries in
        update_log t log' ;
        let last_index =
          match List.last entries with
          | Some last ->
              last.index
          | None ->
              prevLogIndex
        in
        if leaderCommit > t.commitIndex then (
          let v' = min last_index leaderCommit in
          L.debug (fun m -> m "append_entries: %d is newer" v') ;
          update_commit_index t v' ) ;
        Send.appendEntriesResp t.config.cmgr src ~term:t.currentTerm.t
          ~success:true ~matchIndex:last_index
    | false, Ok term ->
        (* Log inconsistency *)
        L.debug (fun m ->
            m "append_entries: log inconsistency (expected term, got): %d %d"
              term prevLogTerm) ;
        Send.appendEntriesResp t.config.cmgr src ~term:t.currentTerm.t
          ~success:false ~matchIndex:prevLogIndex
    | true, _ ->
        L.debug (fun m ->
            m "append_entries: responding negative due to preemption in entry") ;
        preempted_check t term
        >>= fun () ->
        Send.appendEntriesResp t.config.cmgr src ~term:t.currentTerm.t
          ~success:false ~matchIndex:prevLogIndex
    | false, Error _ ->
        L.debug (fun m ->
            m "append_entries: responding negative due to preemption") ;
        preempted_check t term
        >>= fun () ->
        Send.appendEntriesResp t.config.cmgr src ~term:t.currentTerm.t
          ~success:false ~matchIndex:prevLogIndex

  let handle_append_entries_resp t src msg : (unit, exn) Lwt_result.t =
    let ae_term = AppendEntriesResp.term_get_int_exn msg in
    match t.node_state with
    | Leader s when ae_term = s.term && AppendEntriesResp.success_get msg ->
        let matchIndex = AppendEntriesResp.match_index_get_int_exn msg in
        update_match_index t src matchIndex ;
        (*if Log.get_max_index t.log >= nextIndex
          then send_append_entries t ~leader_term:s.term ~host:src
          else*)
        Lwt.return_ok ()
    | Leader s when ae_term = s.term && not (AppendEntriesResp.success_get msg)
      ->
        let ni = Lookup.get_exn s.nextIndex src in
        update_next_index t src (ni - 1) ;
        send_append_entries t ~leader_term:s.term ~host:src >>= Lwt.return_ok
    | _ ->
        preempted_check t ae_term >>= Lwt.return_ok

  (* If leader add to log, otherwise do nothing. *)
  let handle_client_request t host msg : (unit, exn) Lwt_result.t =
    let command = Messaging.command_from_capnp msg in
    L.debug (fun m -> m "got client request %a from %a" Fmt.int64 (Command.id_get msg) Fmt.int64 host) ;
    let debug = false in
    match debug with
    | true -> (
        Send.clientResponse ~sem:`AtLeastOnce t.config.cmgr host ~id:command.id
          ~result:StateMachine.Failure
        >>>= fun () ->
        L.debug (fun m -> m "replied to %a" Fmt.int64 host) |> Lwt.return_ok
      )
    | false ->
      match
        (t.node_state, Hashtbl.find t.client_resolvers command.id)
      with
      | Leader s, None ->
        t.client_result_forwarders <-
          Lookup.set t.client_result_forwarders ~key:command.id ~data:host ;
        Hashtbl.set t.client_resolvers ~key:command.id ~data:None;
        let log' = Log.append t.log command s.term in
        update_log t log' ;
        L.debug (fun m -> m "Added request to log, finishing") |> Lwt.return_ok
                                                                    (*
      | _, Some (Some res) -> 
        L.debug (fun m -> m "Entry %a already resolved sending resp" Fmt.int64 command.id);
        Send.clientResponse t.config.cmgr host ~id:command.id ~result:res
                                                                       *)
      | _, None ->
        L.debug (fun m -> m "Not leader") |> Lwt.return_ok
      | _, Some (_) ->
        L.debug (fun m -> m "Entry %a already in log" Fmt.int64 command.id) |> Lwt.return_ok

  let handle_client_response _t _host _msg =
    L.err (fun m -> m "Got client_response...") ;
    Lwt.return_error (Invalid_argument "Should not have got a client_response")

  let handle_message t_p _cmgr src msg =
    t_p
    >>= fun t ->
    let open ServerMessage in
    match msg |> Capnp.BytesMessage.Message.readonly |> of_message |> get with
    | RequestVote msg ->
        handle_request_vote t src msg
    | RequestVoteResp msg ->
        handle_request_vote_resp t src msg
    | AppendEntries msg ->
        handle_append_entries t src msg
    | AppendEntriesResp msg ->
        handle_append_entries_resp t src msg
    | ClientRequest msg ->
        handle_client_request t src msg
    | ClientResponse _msg ->
        Lwt.return_error (Invalid_argument "ClientResponse message")
    | Undefined i ->
        Lwt.return_error
          (Invalid_argument (Fmt.str "Undefined message of %d" i))
end

let create ~listen_address ~node_list ?(election_timeout = 0.5)
    ?(idle_timeout = 0.1) ?(retry_connection_timeout = 0.1)
    ?(log_path = "./log") ?(term_path = "./term") node_id =
  let other_node_list =
    List.filter_map node_list ~f:(fun ((i, _) as x) ->
        if Int64.(i <> node_id) then Some x else None)
  in
  let t_p, t_f = Lwt.task () in
  let cmgr =
    Conn_manager.create ~retry_connection_timeout ~listen_address ~node_id
      CoreRpcServer.(handle_message t_p)
  in
  let ps =
    match other_node_list with
    | [] ->
        [Lwt.return_unit]
    | _ ->
        List.map other_node_list ~f:(fun (id, addr) ->
            Conn_manager.add_outgoing cmgr id addr (`Persistant addr))
  in
  Lwt.choose ps
  >>= fun () ->
  (* Wait for at least one connection to be made *)
  let majority = (List.length node_list / 2) + 1 in
  let quorum_gen callback =
    Quorum.make_quorum ~threshold:majority ~equal:Int64.equal ~f:callback
  in
  let config =
    { quorum_gen
    ; majority
    ; node_list= List.map node_list ~f:(fun (x, _) -> x)
    ; other_node_list= List.map ~f:fst other_node_list
    ; num_nodes= List.length node_list
    ; cmgr
    ; election_timeout
    ; idle_timeout
    ; node_id }
  in
  let log_p = Log.of_file log_path in
  let term_p = Term.of_file term_path in
  Lwt.both log_p term_p
  >>= fun (log, currentTerm) ->
  let t =
    { currentTerm
    ; log
    ; commitIndex= 0
    ; lastApplied= 0
    ; node_state= Follower {last_recv_from_leader= time_now ()}
    ; state_machine= StateMachine.create ()
    ; config
    ; commitIndex_cond= Lwt_condition.create ()
    ; log_cond= Lwt_condition.create ()
    ; client_result_forwarders= Lookup.create (module Int64)
    ; client_resolvers = Hashtbl.create (module Int64)
    ; is_leader= Lwt_condition.create () }
  in
  Lwt.wakeup t_f t ;
  Lwt.async (Condition_checks.commitIndex t) ;
  Lwt.async (fun () -> Transition.candidate t) ;
  Lwt.wait () |> fst

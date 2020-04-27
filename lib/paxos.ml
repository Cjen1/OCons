open Base
open Types
open Messaging

let paxos = Logs.Src.create "Paxos" ~doc:"Paxos module"

module PL = (val Logs.src_log paxos : Logs.LOG)

module PaxosTypes = struct
  module Config = struct
    type file = {channel: Lwt_io.output_channel; fd: Unix.file_descr}

    let create_file path =
      let fd = Unix.openfile path [Unix.O_RDWR] 0o640 in
      {fd; channel= Lwt_io.of_unix_fd ~mode:Lwt_io.output fd}

    type t =
      { quorum: unit -> log_entry list Quorum.t
      ; majority: int
      ; node_list: (node_id, Messaging.Send.service) List.Assoc.t
      ; num_nodes: int
      ; election_timeout: float
      ; idle_timeout: float
      ; node_id: node_id }

    module PE = Protobuf.Encoder
  end

  type config = Config.t

  module Log : sig
    type t

    val sync : t -> t Lwt.t

    type entry_list = log_entry list

    val of_file : node_addr -> t Lwt.t

    val get : t -> log_index -> (log_entry, exn) Result.t

    val get_exn : t -> log_index -> log_entry

    val get_term : t -> log_index -> term

    val set : t -> index:log_index -> value:log_entry -> t

    val remove : t -> log_index -> t

    val get_max_index : t -> log_index

    val entries_after_inc : t -> index:log_index -> entry_list

    val to_string : t -> string

    val add_entries_remove_conflicts : t -> entry_list -> t

    val append : t -> log_entry -> t
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
          0
      | _ ->
          let entry = get_exn t index in
          entry.term

    let set t ~index ~value =
      Logs.debug (fun m -> m "Setting %d to %s" index (string_of_entry value)) ;
      change t (Set (index, value))

    let remove t i = change t (Remove i)

    let get_max_index (t : t) =
      Lookup.fold t.t ~init:0 ~f:(fun v acc -> Int.max v.index acc)

    (*
  (* Entries after i inclusive *)
  let entries_after_inc log ~index : entry_list =
    let index = if index = 0 then 1 else 0 in (* Avoids printing errors *)
    let rec loop i acc =
      match get log index with
      | Ok v ->
          loop (i + 1) (v :: acc)
      | Error _ ->
          acc
    in
    List.rev @@ loop (index) []
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
            let t = change t (Remove i) in
            delete_geq t (i + 1)
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
              change t (Set (e.index, e))
          | Ok _ ->
              (* inconsitent log *)
              let t = delete_geq t e.index in
              change t (Set (e.index, e)))

    let append t v =
      let max_index = get_max_index t in
      change t (Set (max_index + 1, v))
  end

  type log = Log.t

  module Term_t = struct
    type t = term

    let init () = 0

    type op = int [@@deriving protobuf]

    let apply _t op = op
  end

  module Term = Persistant (Term_t)

  type node_state =
    | Follower of {mutable last_recv_from_leader: time}
    | Candidate of term
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
    ; config: Config.t
    ; commitIndex_cond: unit Lwt_condition.t
    ; log_cond: unit Lwt_condition.t
    ; mutable client_request_results:
        ( command_id
        , StateMachine.op_result Lwt.t * StateMachine.op_result Lwt.u )
        Lookup.t
    ; mutable is_leader: unit Lwt_condition.t }

  type value_type =
    | CurrentTerm of term
    | Log of log
    | CommitIndex of log_index
    | LastApplied of log_index
    | NextIndex of (node_id * log_index)
    | MatchIndex of (node_id * log_index)

  let pp_value_type ppf = function
    | CurrentTerm _ ->
        Stdlib.Format.fprintf ppf "CurrentTerm"
    | Log _ ->
        Stdlib.Format.fprintf ppf "Log"
    | CommitIndex _ ->
        Stdlib.Format.fprintf ppf "CommitIndex"
    | LastApplied _ ->
        Stdlib.Format.fprintf ppf "LastApplied"
    | NextIndex _ ->
        Stdlib.Format.fprintf ppf "NextIndex"
    | MatchIndex _ ->
        Stdlib.Format.fprintf ppf "MatchIndex"

  let update t v =
    match v with
    | CurrentTerm v ->
        PL.debug (fun m -> m "Updating currentTerm to %d" v) ;
        (* This ordering should preserve correctness of execution *)
        t.currentTerm <- Term.change t.currentTerm v ;
        let%lwt term = Term.sync t.currentTerm in 
        t.currentTerm <- term ;
        Lwt.return_unit
    | Log log ->
        t.log <- log ;
        Lwt_condition.broadcast t.log_cond () ;
        PL.debug (fun m -> m "Writing log to disk") ;
        Lwt.return_unit
    | CommitIndex v ->
        PL.debug (fun m -> m "Updating commitIndex to %d" v) ;
        t.commitIndex <- v ;
        Lwt_condition.broadcast t.commitIndex_cond () ;
        Lwt.return_unit
    | LastApplied v ->
        PL.debug (fun m -> m "Updating lastApplied to %d" v) ;
        t.lastApplied <- v ;
        Lwt.return_unit
    | NextIndex (key, data) ->
        PL.debug (fun m -> m "Updating NextIndex for %d to %d" key data) ;
        ( match t.node_state with
        | Leader s ->
            s.nextIndex <- Lookup.set s.nextIndex ~key ~data
        | _ ->
            () ) ;
        Lwt.return_unit
    | MatchIndex (key, data) ->
        PL.debug (fun m -> m "Updating MatchIndex for %d to %d" key data) ;
        ( match t.node_state with
        | Leader s ->
            s.matchIndex <- Lookup.set s.matchIndex ~key ~data ;
            Lwt_condition.broadcast s.matchIndex_cond ()
        | _ ->
            () ) ;
        Lwt.return_unit
end

open PaxosTypes

module rec Transition : sig
  val follower : t -> unit

  val candidate : t -> unit Lwt.t

  val leader : t -> log_entry list list -> term -> unit Lwt.t
end = struct
  let follower t =
    PL.debug (fun m -> m "Becomming a follower") ;
    t.node_state <- Follower {last_recv_from_leader= time_now ()} ;
    Lwt.async (Condition_checks.election_timeout_expired t)

  let leader t entries term =
    match t.node_state with
    | Candidate cterm when Int.(cterm = term) ->
        PL.debug (fun m -> m "Elected leader of term %d" t.currentTerm.t) ;
        let string_of_entries_list entries_list =
          let res =
            List.fold entries_list ~init:"(" ~f:(fun acc entries ->
                Printf.sprintf "%s %s" acc (string_of_entries entries))
          in
          res ^ " )"
        in
        PL.debug (fun m -> m "Entries: %s" (string_of_entries_list entries)) ;
        let rec merge xs ys =
          let rec loop :
                 log_entry list
              -> log_entry list
              -> log_entry list
              -> log_entry list =
           fun xs ys acc ->
            match (xs, ys) with
            | [], ys ->
                ys
            | xs, [] ->
                xs
            | x :: xs, y :: ys ->
                assert (Int.(x.index = y.index)) ;
                (if x.term > y.term then x else y) :: acc |> loop xs ys
          in
          loop xs ys [] |> List.rev
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
        let%lwt () =
          let log =
            List.fold_left entries_to_add ~init:t.log
              ~f:(fun log entry ->
                let log = Log.set log ~index:entry.index ~value:entry in
                log)
          in
          PL.debug (fun m -> m "Adding ops to log") ;
          let%lwt log = Log.sync log in
          update t @@ Log log
        in
        let nextIndex, matchIndex, heartbeat_last_sent =
          List.fold t.config.node_list
            ~init:
              ( Lookup.create (module Int)
              , Lookup.create (module Int)
              , Lookup.create (module Int) )
            ~f:(fun (nI, mI, hb) (id, _addr) ->
              ( Lookup.set nI ~key:id ~data:(t.commitIndex + 1)
              , Lookup.set mI ~key:id ~data:0
              , Lookup.set hb ~key:id ~data:0. ))
        in
        t.node_state <-
          Leader
            { nextIndex
            ; matchIndex
            ; matchIndex_cond= Lwt_condition.create ()
            ; heartbeat_last_sent
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

  let candidate t =
    PL.debug (fun m -> m "Trying to become leader") ;
    let updated_term : term =
      let id_in_current_epoch =
        Int.(
          t.currentTerm.t
          - (t.currentTerm.t % t.config.num_nodes)
          + t.config.node_id)
      in
      if id_in_current_epoch < t.currentTerm.t then
        id_in_current_epoch + t.config.num_nodes
      else id_in_current_epoch
    in
    t.currentTerm <- Term.change t.currentTerm updated_term ;
    t.node_state <- Candidate t.currentTerm.t ;
    let commitIndex = t.commitIndex in
    PL.debug (fun m -> m "New term = %d" updated_term) ;
    let%lwt () = update t @@ CurrentTerm updated_term in
    let%lwt resps =
      (* Only voteGranted requests return *)
      Utils.recv_quorum
        ~quorum:(t.config.quorum : unit -> log_entry list Quorum.t)
        ~followers:t.config.node_list
        ~send:(fun (_id, follower) ->
          let%lwt resp =
            follower
            |> Send.requestVote ~term:updated_term ~leader_commit:commitIndex
                 ~src_id:t.config.node_id
          in
          match resp.voteGranted with
          | true ->
              PL.debug (fun m -> m "Got vote granted") ;
              Lwt.return resp.entries
          | false ->
              PL.debug (fun m -> m "Did not get vote granted") ;
              Lwt.task () |> fst)
    in
    leader t resps updated_term
end

and Condition_checks : sig
  val commitIndex : t -> unit -> unit Lwt.t

  val election_timeout_expired : t -> unit -> unit Lwt.t

  val leader_heartbeat : t -> unit -> unit Lwt.t

  val log : t -> term -> unit -> unit Lwt.t

  val matchIndex : t -> unit -> unit Lwt.t
end = struct
  let commitIndex t =
    let rec loop () =
      PL.debug (fun m -> m "commitIndex_cond_check: waiting") ;
      let%lwt () = Lwt_condition.wait t.commitIndex_cond in
      PL.debug (fun m -> m "commitIndex_cond_check: running") ;
      let rec update_SM () =
        if t.commitIndex > t.lastApplied then (
          PL.debug (fun m -> m "commitIndex_cond_check: Updating SM") ;
          let%lwt () = update t @@ LastApplied (t.lastApplied + 1) in
          (*PL.debug (fun m -> m "Log state = \n%s" (Log.to_string t.log)) ;*)
          let command = (Log.get_exn t.log t.lastApplied).command in
          (* lastApplied must exist by match statement *)
          let result = StateMachine.update t.state_machine command in
          let _, fulfiller = Lookup.find_or_add t.client_request_results command.id ~default:Lwt.task in
          Lwt.wakeup_later fulfiller result;
          update_SM () )
        else Lwt.return_unit
      in
      let%lwt () = update_SM () in
      loop ()
    in
    loop

  let rec election_timeout_expired t =
    let rec loop () =
      PL.debug (fun m -> m "election_timeout_expired_check: sleeping") ;
      let%lwt () = Lwt_unix.sleep t.config.election_timeout in
      PL.debug (fun m -> m "election_timeout_expired_check: ended sleep") ;
      match t.node_state with
      | Follower s ->
          PL.debug (fun m ->
              m
                "election_timeout_expired_check: time since last recv from \
                 leader: %f"
                (time_now () -. s.last_recv_from_leader)) ;
          if
            Float.(
              time_now () -. s.last_recv_from_leader > t.config.election_timeout)
          then (
            PL.debug (fun m ->
                m "election_timeout_expired_check: becomming candidate") ;
            (* Election timeout expired => leader most likely dead *)
            Transition.candidate t )
          else (* If not becomming candidate then continue looping *)
            loop ()
      | _ ->
          Lwt.return_unit
    in
    loop

  let send_append_entries t ~leader_term ~host:(id, cap) =
    let%lwt log = Log.sync t.log in
    t.log <- log; (* Sync log to disk before sending anything *)
    let rec ae_req () =
      match t.node_state with
      | Leader {nextIndex; term; _} when term = leader_term -> (
          let entries_start_index = Lookup.get_exn nextIndex id in
          let entries =
            Log.entries_after_inc t.log ~index:entries_start_index
          in
          let prevLogIndex = entries_start_index - 1 in
          let prevLogTerm = Log.get_term t.log prevLogIndex in
          let leaderCommit = t.commitIndex in
          PL.debug (fun m ->
              m "append_entries: sending request: (%d %d %d %s %d) to %d" term
                prevLogIndex prevLogTerm
                (string_of_entries entries)
                leaderCommit id) ;
          let%lwt resp =
            Send.appendEntries cap ~term ~prevLogIndex ~prevLogTerm ~entries
              ~leaderCommit
          in
          match resp with
          | {term= resp_term; success= true} when Int.(resp_term = term) ->
              PL.debug (fun m ->
                  m "Update of %d starting at %d was successful" id
                    entries_start_index) ;
              let highest_replicated_index =
                match List.last entries with
                | Some {index; _} ->
                    index
                | None ->
                    entries_start_index - 1
              in
              let%lwt () =
                update t (NextIndex (id, highest_replicated_index + 1))
              in
              let%lwt () =
                update t (MatchIndex (id, highest_replicated_index))
              in
              Lwt.return_unit
          | {term= resp_term; success= false} when Int.(resp_term = term) ->
              (* Log inconsistency *)
              PL.debug (fun m ->
                  m "Failed in appendEntries on %d due to log inconsistency" id) ;
              let%lwt () = update t (NextIndex (id, entries_start_index - 1)) in
              ae_req ()
          | {success= false; _} ->
              PL.warn (fun m -> m "Preempted upon receipt from %d" id) ;
              (* Preempted *)
              Transition.follower t ;
              Lwt.return_unit
          | {success= true; _} ->
              assert false )
      | _ ->
          Lwt.return_unit
    in
    ae_req ()

  let rec leader_heartbeat t () =
    PL.debug (fun m -> m "leader_heartbeat_check: call entry") ;
    match t.node_state with
    | Leader {heartbeat_last_sent; _} ->
        List.iter t.config.node_list ~f:(fun ((id, _) as host) ->
            match Lookup.get_exn heartbeat_last_sent id with
            | lastSent when Float.(lastSent < t.config.idle_timeout) ->
                PL.debug (fun m -> m "managed lookup, timed out") ;
                Lwt.async (fun () ->
                    send_append_entries t ~leader_term:t.currentTerm.t ~host)
            | _ ->
                PL.debug (fun m ->
                    m "managed lookup, don't need to send keepalive") ;
                ()) ;
        PL.debug (fun m -> m "leader_heartbeat_check: sleeping") ;
        let%lwt () = Lwt_unix.sleep (t.config.election_timeout /. 2.1) in
        PL.debug (fun m -> m "leader_heartbeat_check: end sleep") ;
        leader_heartbeat t ()
    | _ ->
        PL.debug (fun m -> m "leader_heartbeat_check: call exit") ;
        Lwt.return_unit

  let rec log t leader_term () =
    (* Dispatch log loop for each node to simplify logic
     * Can't have multiple ongoing rpcs to remote but due 
     * to batching shouldn't have much effect
     *)
    let rec log_host_loop ((id, _) as host) =
      match t.node_state with
      | Leader {nextIndex; term; _} when Int.(term = leader_term) ->
          PL.debug (fun m -> m "log_cond_check%d: waiting" id) ;
          let%lwt () = Lwt_condition.wait t.log_cond in
          PL.debug (fun m -> m "log_cond_check%d: got cond" id) ;
          let max_log_index = Log.get_max_index t.log in
          let next_index = Lookup.get_exn nextIndex id in
          if max_log_index >= next_index then
            let%lwt () = send_append_entries t ~leader_term ~host in
            log_host_loop host
          else Lwt.return_unit
      | _ ->
          Lwt.return_unit
    in
    Lwt_list.iter_p (fun host -> log_host_loop host) t.config.node_list

  let matchIndex t =
    let rec loop () =
      match t.node_state with
      | Leader {matchIndex_cond; matchIndex; _} ->
          PL.debug (fun m -> m "matchIndex_cond_check: waiting") ;
          let%lwt () = Lwt_condition.wait matchIndex_cond in
          PL.debug (fun m -> m "matchIndex_cond_check: got cond") ;
          let n =
            Lookup.fold matchIndex ~init:[] ~f:(fun data acc -> data :: acc)
            |> List.sort ~compare:(fun x y -> -Int.compare x y)
            |> fun ls -> List.nth_exn ls (t.config.majority - 1)
            (* element exists bc defined as such *)
          in
          PL.debug (fun m -> m "matchIndex_cond_check: new commitIndex = %d" n) ;
          let%lwt () = update t @@ CommitIndex n in
          loop ()
      | _ ->
          PL.debug (fun m -> m "matchIndex_cond_check: call exit") ;
          Lwt.return_unit
    in
    loop
end

and Utils : sig
  val preempted_check : t -> term -> unit Lwt.t

  val update_last_recv : t -> unit

  val recv_quorum :
       quorum:(unit -> 'b Quorum.t)
    -> followers:'a list
    -> send:('a -> 'b Lwt.t)
    -> 'b list Lwt.t
end = struct
  let preempted_check t term =
    if term > t.currentTerm.t then (
      PL.debug (fun m -> m "preempted_check: preempted by term %d" term) ;
      let%lwt () = update t (CurrentTerm term) in
      Transition.follower t |> Lwt.return )
    else Lwt.return_unit

  let update_last_recv t =
    match t.node_state with
    | Follower s ->
        s.last_recv_from_leader <- time_now ()
    | _ ->
        ()

  let recv_quorum ~quorum ~followers ~send =
    let dispatches = List.map ~f:send followers in
    let quorum : 'a Quorum.t = quorum () in
    PL.debug (fun m -> m "recv_quorum: Waiting for quorum of responses") ;
    let rec loop ps acc =
      if quorum.sufficient () then Lwt.return (acc, ps)
      else
        let%lwt resolved, todo = Lwt.nchoose_split ps in
        List.iter resolved ~f:quorum.add ;
        loop todo (resolved @ acc)
    in
    let%lwt resolved, outstanding = loop dispatches [] in
    PL.debug (fun m -> m "recv_quorum: Got a quorum of responses") ;
    List.iter ~f:Lwt.cancel outstanding ;
    Lwt.return resolved
end

open Utils

module CoreRpcServer = struct
  type nonrec t = t

  let sync_term_log t =
    let%lwt log = Log.sync t.log 
    and currentTerm = Term.sync t.currentTerm 
    in 
    t.log <- log;
    t.currentTerm <- currentTerm;
    Lwt.return_unit


  let request_vote t ~term ~leaderCommit ~src_id : request_vote_response Lwt.t =
    let%lwt () = sync_term_log t in
    PL.debug (fun m -> m "request_vote: received request") ;
    if term < t.currentTerm.t then (
      PL.debug (fun m ->
          m "request_vote: vote not granted to %d for term %d" src_id term) ;
      Lwt.return {term= t.currentTerm.t; voteGranted= false; entries= []} )
    else
      let%lwt () = preempted_check t term in
      PL.debug (fun m ->
          m "request_vote: vote granted to %d for term %d" src_id term) ;
      update_last_recv t ;
      let entries = Log.entries_after_inc t.log ~index:leaderCommit in
      Lwt.return {term= t.currentTerm.t; voteGranted= true; entries}

  let append_entries t ~term ~prevLogIndex ~prevLogTerm ~entries ~leaderCommit =
    let%lwt () = sync_term_log t in
    PL.debug (fun m ->
        m "append_entries: received request: (%d %d %d %s %d)" term prevLogIndex
          prevLogTerm
          (string_of_entries entries)
          leaderCommit) ;
    let%lwt () = preempted_check t term in
    match (term < t.currentTerm.t, Log.get_term t.log prevLogIndex) with
    | false, term when term = prevLogTerm ->
        PL.debug (fun m -> m "append_entries: received valid") ;
        update_last_recv t ;
        PL.debug (fun m ->
            m
              "append_entries: removing log conflicts and adding received \
               entries") ;
        let%lwt () =
          update t (Log (Log.add_entries_remove_conflicts t.log entries))
        in
        let%lwt () =
          if leaderCommit > t.commitIndex then (
            PL.debug (fun m ->
                m "append_entries: Updating commit index to that of leader") ;
            (* if a last entry exists, take min, otherwise leaderCommit *)
            match List.last entries with
            | Some last ->
                update t @@ CommitIndex (min last.index leaderCommit)
            | None ->
                update t @@ CommitIndex leaderCommit )
          else Lwt.return_unit
        in
        Lwt.return {term= t.currentTerm.t; success= true}
    | false, term ->
        (* Log inconsistency *)
        PL.debug (fun m ->
            m
              "append_entries: responding negative due to log inconsistency, \
               expected term: %d, got %d"
              term prevLogTerm) ;
        (* term < currentTerm or log inconsistent *)
        Lwt.return {term= t.currentTerm.t; success= false}
    | true, _ ->
        PL.debug (fun m ->
            m "append_entries: responding negative due to preemption") ;
        (* term < currentTerm or log inconsistent *)
        Lwt.return {term= t.currentTerm.t; success= false}

  (* If leader will submit request
   * If not leader
       set up fulfillers
       wait until either
         leader -> recurse (and thus submit via leader branch)
         request is submitted by another node (the leader)
     (In effect the unsubmitted client requests are held in the Lwt promise queue 
         rather than an explicitly managed one)
   *)
  let rec client_req t (command : command) =
    match t.node_state with
    | Leader _ ->
        (* leader and not yet requested *)
        PL.debug (fun m -> m "client_req: Leader and unfulfilled") ;
        let result, _fulfiller =
          Hashtbl.find_or_add t.client_request_results command.id
            ~default:Lwt.task
        in
        let index = Log.get_max_index t.log + 1 in
        let entry = {command; term= t.currentTerm.t; index} in
        PL.debug (fun m ->
            m "client_req: adding entry to log %s" (string_of_entry entry)) ;
        let%lwt () =
          let log' =
            Log.set t.log ~index ~value:{command; term= t.currentTerm.t; index}
          in
          update t (Log log')
        in
        result
    | _ ->
        let leader_p =
          let%lwt () = Lwt_condition.wait t.is_leader in
          (* Is now the leader -> can submit the request *)
          client_req t command
        in
        let waiter, _ =
          Lookup.find_or_add t.client_request_results command.id
            ~default:Lwt.task
        in
        Lwt.pick [leader_p; waiter]
end

module Service = Messaging.Recv (CoreRpcServer)

let serve (t : t Lwt.t) ~public_address ~listen_address ~secret_key ~id
    ~cap_file ~client_cap_file =
  let config =
    Capnp_rpc_unix.Vat_config.create ~public_address ~secret_key listen_address
  in
  let sturdy_uri = Capnp_rpc_unix.Vat_config.sturdy_uri config in
  let services = Capnp_rpc_net.Restorer.Table.create sturdy_uri in
  let service_id =
    Capnp_rpc_unix.Vat_config.derived_id config ("service" ^ Int.to_string id)
  in
  let client_id =
    Capnp_rpc_unix.Vat_config.derived_id config ("client" ^ Int.to_string id)
  in
  let%lwt local = Service.local t and client = Service.client t in
  Capnp_rpc_net.Restorer.Table.add services service_id local ;
  Capnp_rpc_net.Restorer.Table.add services client_id client ;
  let restore = Capnp_rpc_net.Restorer.of_table services in
  let%lwt vat = Capnp_rpc_unix.serve config ~restore in
  let ( let* ) = Result.( >>= ) in
  let res =
    let* () = Capnp_rpc_unix.Cap_file.save_service vat service_id cap_file in
    let* () =
      Capnp_rpc_unix.Cap_file.save_service vat client_id client_cap_file
    in
    Ok ()
  in
  match res with
  | Error (`Msg m) ->
      failwith m
  | Ok () ->
      PL.info (fun m -> m "Server running. Connect using %S.@." cap_file) ;
      Lwt.return (local, fst @@ Lwt.wait ())

let create ~public_address ~listen_address ~secret_key ~node_list
    ~election_timeout ~idle_timeout ~log_path ~term_path ~node_id ~cap_file
    ~client_cap_file =
  let t_p, t_f = Lwt.wait () in
  (* Create t as a promise such that the cap files are created before it is needed *)
  let%lwt local_service, p_server =
    serve t_p ~public_address ~listen_address ~secret_key ~id:node_id ~cap_file
      ~client_cap_file
  in
  let majority = Int.((List.length node_list / 2) + 1) in
  let%lwt log = Log.of_file log_path in
  let%lwt currentTerm = Term.of_file term_path in
  PL.debug (fun m -> m "Got current log and term") ;
  let vat = Capnp_rpc_unix.client_only_vat () in
  (*- Create the follower capnp capabilities ---------*)
  PL.debug (fun m -> m "Trying to connect to caps") ;
  let%lwt node_caps =
    Lwt_list.map_p
      (fun (id, path) ->
        if id = node_id then
          let cap = RepairableRef.connect_local local_service in
          Lwt.return (id, cap)
        else
          let%lwt sr = Send.get_sr_from_path path vat in
          let%lwt cap = RepairableRef.connect sr in
          Lwt.return (id, cap))
      node_list
  in
  PL.debug (fun m -> m "Connected to other node's caps") ;
  let config =
    Config.
      { quorum= Quorum.make_quorum majority
      ; majority
      ; node_list= node_caps
      ; num_nodes= List.length node_caps
      ; election_timeout
      ; idle_timeout
      ; node_id }
  in
  let t =
    { currentTerm
    ; log
    ; commitIndex= 0
    ; lastApplied= 0
    ; node_state= Follower {last_recv_from_leader= 0.}
    ; state_machine= StateMachine.create ()
    ; config
    ; commitIndex_cond= Lwt_condition.create ()
    ; log_cond= Lwt_condition.create ()
    ; client_request_results= Lookup.create (module Int)
    ; is_leader= Lwt_condition.create () }
  in
  Lwt.wakeup_later t_f t ;
  Lwt.async @@ Condition_checks.commitIndex t ;
  (* Allow the server to respond to requests bc t is now created *)
  Lwt.join [p_server; Transition.candidate t]

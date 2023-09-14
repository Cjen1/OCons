open! Core
open! Types
open! Utils
open C.Types
open Actions_f
open Ocons_core.Consensus_intf
module IdMap = Iter.Map.Make (Int)

module Value = struct
  (* sorted list by command_id *)
  type t = command list [@@deriving compare, equal, hash, bin_io, sexp]

  let pp : t Fmt.t =
    let open Fmt in
    brackets @@ list ~sep:comma Command.pp

  let empty = []
end

module CommandTree = Conspire_sync.Make (Value)
module LogEntry = CommandTree.LogEntry

module GlobalTypes = struct
  type log_entry = LogEntry.t [@@deriving compare, equal, bin_io]

  type log_update =
    | Segment of {segment_entries: log_entry list; segment_start: log_index}
    | Ack of {idx: log_index; node_hash: LogEntry.hash}
  [@@deriving bin_io]

  let log_update_pp ppf v =
    let open Fmt in
    match v with
    | Segment {segment_entries; segment_start} ->
        let pp =
          record
            [ field "start" (fun _ -> segment_start) int
            ; field "entries"
                (fun _ -> segment_entries)
                (brackets @@ list ~sep:semi LogEntry.pp) ]
        in
        pf ppf "%a" pp ()
    | Ack {idx; node_hash} ->
        let pp =
          record
            [ field "idx" (fun _ -> idx) int
            ; field "hash" (fun _ -> node_hash) int ]
        in
        pf ppf "Ack%a" (parens pp) ()

  type state =
    { vval: log_entry Log.t
    ; mutable vterm: term
    ; mutable term: term
    ; mutable commit_index: log_index }
  [@@deriving equal]

  let state_pp =
    let open Fmt in
    record
      [ field "commit_index" (fun s -> s.commit_index) int
      ; field "term" (fun s -> s.term) int
      ; field "vterm" (fun s -> s.vterm) int
      ; field "vval"
          (fun s -> Log.iter s.vval |> Iter.to_list)
          (brackets @@ list ~sep:comma LogEntry.pp) ]

  let state_pp_short ?from =
    Fmt.(
      record
        [ field "commit_index" (fun (s : state) -> s.commit_index) int
        ; field "term" (fun (s : state) -> s.term) int
        ; field "vterm" (fun (s : state) -> s.vterm) int
        ; field "vval_length" (fun (s : state) -> Log.highest s.vval) int
        ; field "vval_seg"
            (fun (s : state) ->
              let delta = 5 in
              let lo =
                Option.value ~default:(Log.highest s.vval - delta) from
              in
              let lo = max 0 lo in
              let hi = lo + delta in
              let hi = min hi (Log.highest s.vval) in
              Segment
                { segment_start= lo
                ; segment_entries= Log.iter ~lo ~hi s.vval |> Iter.to_list } )
            log_update_pp ] )

  type message =
    {term: term; vval_seg: log_update; vterm: term; commit_index: log_index}
  [@@deriving bin_io]

  let message_pp =
    let open Fmt in
    record
      [ field "term" (fun (s : message) -> s.term) int
      ; field "commit_index" (fun (s : message) -> s.commit_index) int
      ; field "vval" (fun (s : message) -> s.vval_seg) log_update_pp
      ; field "vterm" (fun (s : message) -> s.vterm) int ]
end

let log_get log idx = if idx = -1 then LogEntry.bot else Log.get log idx

let log_find log idx = if idx = -1 then Some LogEntry.bot else Log.find log idx

module Replication = struct
  (* back replication with a hashtbl to value to allow re-construction of log, if state changed to prevent perfect match
     so store in hashtbl each (term, idx) -> (command, parent) for reconstruction over term increments
  *)
  open GlobalTypes

  type local = state

  type remote = state

  type update_tracker = {log_diverged_from: log_index option; other_change: bool}

  type remote_view =
    { expected_remote: remote
    ; mutable sent_upto: log_index
    ; mutable change_flag: bool }

  type t = {local_state: local; remote_views: remote_view Hashtbl.M(Int).t}

  let init_state () =
    {term= 0; vterm= 0; vval= Log.create LogEntry.bot; commit_index= -1}

  let create nodes =
    { local_state= init_state ()
    ; remote_views=
        Hashtbl.of_alist_exn
          (module Int)
          (List.map nodes ~f:(fun nid ->
               ( nid
               , { expected_remote= init_state ()
                 ; sent_upto= -1
                 ; change_flag= false } ) ) ) }

  let reset_sent_upto t diverge_from =
    Hashtbl.iter t.remote_views ~f:(fun v ->
        v.sent_upto <- min v.sent_upto diverge_from )

  let set_change_flag t =
    Hashtbl.iter t.remote_views ~f:(fun v -> v.change_flag <- true)

  (* Used to update cached state on remote *)
  let apply_update (ctree : CommandTree.t) (state : state) (m : message) =
    state.vterm <- m.vterm ;
    state.term <- m.term ;
    state.commit_index <- m.commit_index ;
    let log = state.vval in
    let _update_state : unit =
      match m.vval_seg with
      | Segment seg ->
          Log.cut_after log (seg.segment_start - 1) ;
          Iter.of_list seg.segment_entries
          |> Iter.iteri (fun i v -> Log.set log (i + seg.segment_start) v)
      | Ack {idx; node_hash} ->
          CommandTree.fixup ctree state.vval ~idx ~ack_hash:node_hash |> ignore
    in
    CommandTree.add_log ctree state.vval ()

  let requires_update t nid =
    let v = Hashtbl.find_exn t.remote_views nid in
    v.change_flag || Log.highest t.local_state.vval > v.sent_upto

  (* if ack is true we assume that the remote has the relevant commands in its command tree *)
  let update_to_msg t nid ~ack =
    let v = Hashtbl.find_exn t.remote_views nid in
    let segment_start = v.sent_upto + 1 in
    let vval_seg =
      match ack with
      | false ->
          Segment
            { segment_start
            ; segment_entries=
                Log.iter t.local_state.vval ~lo:segment_start |> Iter.to_list }
      | true ->
          let idx = Log.highest t.local_state.vval in
          Ack {idx; node_hash= (log_get t.local_state.vval idx).hist}
    in
    { term= t.local_state.term
    ; vterm= t.local_state.vterm
    ; vval_seg
    ; commit_index= t.local_state.commit_index }

  let get_msg_to_send t ctree ?(ack = false) nid =
    let v = Hashtbl.find_exn t.remote_views nid in
    let msg = update_to_msg t nid ~ack in
    apply_update ctree v.expected_remote msg ;
    v.sent_upto <- Log.highest t.local_state.vval ;
    v.change_flag <- false ;
    msg

  let find_diverge ~equal l1 l2 =
    let rec aux l1 l2 idx =
      match (Log.find l1 idx, Log.find l2 idx) with
      | None, None ->
          None
      | Some i, Some j ->
          if equal i j then aux l1 l2 (idx + 1) else Some idx
      | Some _, None | None, Some _ ->
          Some idx
    in
    aux l1 l2 0

  let log_pp ?from ppf v =
    let delta = 5 in
    let lo = Option.value ~default:(Log.highest v - delta) from in
    let lo = max 0 lo in
    let hi = lo + delta in
    let hi = min hi (Log.highest v) in
    let seg =
      Segment
        {segment_start= lo; segment_entries= Log.iter ~lo ~hi v |> Iter.to_list}
    in
    Fmt.pf ppf "%a" log_update_pp seg

  let set_ci t idx =
    if idx > t.local_state.commit_index then (
      t.local_state.commit_index <- idx ;
      set_change_flag t )

  let set_term t term =
    if term > t.local_state.term then (
      t.local_state.term <- term ;
      set_change_flag t )

  let set_vterm t term =
    if term > t.local_state.vterm then (
      t.local_state.vterm <- term ;
      set_change_flag t )

  let set_vval t idx v =
    if
      not
        ([%equal: log_entry option] (Some v) (Log.find t.local_state.vval idx))
    then (
      reset_sent_upto t (idx - 1) ;
      Log.set t.local_state.vval idx v )

  let command_added_reporter, should_run_ca_reporter =
    Ocons_core.Utils.InternalReporter.rate_reporter 0 "commands added"

  let add_commands t ctree citer =
    should_run_ca_reporter := true ;
    let idx = ref @@ Log.highest t.local_state.vval in
    let hist =
      ref @@ if !idx < 0 then 0 else (log_get t.local_state.vval !idx).hist
    in
    citer (fun c ->
        command_added_reporter () ;
        incr idx ;
        let le = LogEntry.make !hist [c] in
        hist := le.hist ;
        set_vval t !idx le ) ;
    CommandTree.add_log ctree t.local_state.vval

  (*
  let pp =
    let open Fmt in
    record
      [ field "change_flag" (fun t -> t.change_flag) bool
      ; field "sent_upto"
          (fun t -> Hashtbl.to_alist t.sent_upto)
          (brackets @@ list @@ pair ~sep:(Fmt.any ";@ ") int int)
      ; field "local" (fun t -> t.local_state) state_pp
      ; field "expected"
          (fun t -> Hashtbl.to_alist t.expected_remote)
          (brackets @@ list @@ pair ~sep:(Fmt.any ";@ ") int state_pp) ]
      *)
end

module StallChecker = struct
  type t =
    { mutable prev_commit_idx: log_index
    ; mutable ticks_since_commit: int
    ; tick_limit: int }

  let reset t commit_index =
    if commit_index > t.prev_commit_idx then t.prev_commit_idx <- commit_index ;
    t.ticks_since_commit <- t.tick_limit

  let is_stalled t = t.ticks_since_commit <= 0

  let check ?(pp : unit Fmt.t = fun _ () -> ()) ?(should_make_progress = false)
      t =
    t.ticks_since_commit <- t.ticks_since_commit - 1 ;
    if is_stalled t && should_make_progress then (
      traceln "========== Stalled ==========" ;
      traceln "%a" pp () ;
      traceln "========== Stalled ==========" ) ;
    if is_stalled t then reset t t.prev_commit_idx
end

module Types = struct
  include GlobalTypes

  type config =
    { node_id: node_id
    ; replica_ids: node_id list
    ; other_replica_ids: node_id list
    ; replica_count: int
    ; quorum_size: int
    ; fd_timeout: int
    ; max_outstanding: int }

  let make_config ~node_id ~replica_ids ~fd_timeout ?(max_outstanding = 8192) ()
      : config =
    let floor f = f |> Int.of_float in
    let replica_count = List.length replica_ids in
    let quorum_size = floor (2. *. Float.of_int replica_count /. 3.) + 1 in
    assert (3 * quorum_size > 2 * replica_count) ;
    { node_id
    ; replica_ids
    ; other_replica_ids= List.filter replica_ids ~f:(fun i -> not (i = node_id))
    ; replica_count
    ; quorum_size
    ; fd_timeout
    ; max_outstanding }

  type fd_sm = {state: (node_id, int) Hashtbl.t}

  type message = GlobalTypes.message

  type t =
    { ctree: CommandTree.t
    ; state: Replication.t
    ; state_cache: (node_id, Replication.remote, Int.comparator_witness) Map.t
    ; failure_detector: fd_sm
    ; config: config
    ; command_queue: Command.t Queue.t
    ; stall_checker: StallChecker.t }

  let get_command idx t =
    (log_get t.state.local_state.vval idx).value |> Iter.of_list

  let get_commit_index t = t.state.local_state.commit_index

  module PP = struct
    open Fmt

    let invrs_pp ppf invrs =
      match
        List.filter_map invrs ~f:(function
          | _, true ->
              None
          | s, false ->
              Some s )
      with
      | ls when List.is_empty ls ->
          pf ppf "Ok"
      | ls ->
          pf ppf "violated: %a" (brackets @@ list ~sep:comma string) ls

    let config_pp : config Fmt.t =
      let get_invrs cfg =
        [ ( "node_id not in other_replicas"
          , not @@ Stdlib.List.mem cfg.node_id cfg.other_replica_ids )
        ; ( "replica_count correct"
          , List.length cfg.replica_ids = cfg.replica_count ) ]
      in
      record
        [ field "node_id" (fun c -> c.node_id) int
        ; field "quorum_size" (fun c -> c.quorum_size) int
        ; field "fd_timeout" (fun c -> c.fd_timeout) int
        ; field "invrs" get_invrs invrs_pp
        ; field "replica_ids"
            (fun c -> c.replica_ids)
            (brackets @@ list ~sep:comma int) ]

    let fd_sm_pp =
      record
        [ field "state"
            (fun (v : fd_sm) ->
              Hashtbl.to_alist v.state
              |> List.sort ~compare:(fun (a, _) (b, _) -> Int.compare a b) )
            ( brackets @@ list ~sep:semi @@ parens
            @@ pair ~sep:(Fmt.any ":@ ") int int ) ]

    let message_pp = GlobalTypes.message_pp

    let t_pp =
      record
        [ field "config" (fun t -> t.config) config_pp
        ; field "failure_detector" (fun t -> t.failure_detector) fd_sm_pp
        ; field "local_state" (fun t -> t.state.local_state) state_pp
        ; field "state_cache"
            (fun t -> t.state_cache |> Map.to_alist)
            ( brackets @@ list @@ parens
            @@ pair ~sep:(Fmt.any ":@ ") int state_pp )
        ; field "command_queue"
            (fun t -> Queue.to_list t.command_queue)
            (brackets @@ list @@ Command.pp) ]
  end

  let log_continuous l ctree =
    Log.iteri l
    |> IterLabels.fold ~init:LogEntry.bot
         ~f:(fun (prev : LogEntry.t) (idx, v) ->
           if not @@ CommandTree.mem ctree v then
             Fmt.failwith "(%d: %d) is missing in cmdtree after handling" idx
               v.hist ;
           if not (v.parent_hist = prev.hist) then
             Fmt.failwith
               "(%d: %d) has a broken log whern %d doesn't equal prev.hist(%d)"
               idx v.hist v.parent_hist prev.hist ;
           v )
    |> ignore

  let log_invariant t =
    List.iter t.config.replica_ids ~f:(fun i ->
        let l =
          if i = t.config.node_id then t.state.local_state.vval
          else (Map.find_exn t.state_cache i).vval
        in
        log_continuous l t.ctree )
end

module Make
    (Act : ActionSig
             with type t = Types.t
              and type message = GlobalTypes.message) =
struct
  module R = Replication
  open GlobalTypes
  include Types
  open Act

  let is_live t nid =
    nid = t.config.node_id || Hashtbl.find_exn t.failure_detector.state nid > 0

  let stall_check t =
    StallChecker.check t.stall_checker ~pp:(fun ppf () ->
        let problem_idx = t.state.local_state.commit_index in
        Fmt.pf ppf "Stalled on %d at %d@.%a" t.config.node_id problem_idx
          Fmt.(
            record
              [ field "replica_states"
                  (fun t ->
                    t.config.replica_ids |> Iter.of_list
                    |> Iter.map (function
                         | nid when nid = t.config.node_id ->
                             (Fmt.str "local(%d)" nid, t.state.local_state)
                         | nid ->
                             ( Fmt.str "remote(%d)" nid
                             , Map.find_exn t.state_cache nid ) )
                    |> Iter.to_list )
                  ( braces @@ list @@ parens
                  @@ pair ~sep:(any ":@ ") string
                       (state_pp_short ~from:problem_idx) ) ] )
          t )

  let acceptor_reply t src log_update =
    let local = t.state.local_state in
    let remote = Map.find_exn t.state_cache src in
    let msg_term : term = remote.vterm in
    let ack t src =
      ignore (t, src)
      (*
      TODO
      let msg = Replication.get_msg_to_send t.state t.ctree ~ack:true src in
      send src msg
      *)
    in
    log_invariant t ;
    match (msg_term >= local.term, log_update) with
    (* Ack => term = current_term => must extend *)
    | true, Ack {idx; node_hash} ->
        traceln "(%d, %d) (%d, %d)" msg_term local.term
          (log_get local.vval idx).hist node_hash ;
        assert ((log_get local.vval idx).LogEntry.hist = node_hash)
    (* Just fully overwrite *)
    | true, Segment {segment_start; _} when msg_term > local.vterm ->
        Replication.set_term t.state msg_term ;
        Replication.set_vterm t.state msg_term ;
        (* no need to truncate since equiv to immediately voting for remaining entries *)
        log_invariant t ;
        Log.iteri remote.vval ~lo:segment_start (fun (idx, v) ->
            Replication.set_vval t.state idx v ) ;
        log_invariant t ;
        ack t src
    | true, Segment {segment_start; _} when msg_term = local.vterm ->
        let rec aux idx =
          match (Log.find local.vval idx, Log.find remote.vval idx) with
          (* remote extends local => write *)
          | None, Some _ ->
              log_invariant t ;
              Log.iteri remote.vval ~lo:idx (fun (idx, v) ->
                  R.set_vval t.state idx v ) ;
              log_invariant t ;
              ack t src
          (* same log => ack *)
          | None, None ->
              ack t src
          (* Logs match => recurse *)
          | Some l, Some r when [%equal: LogEntry.t] l r ->
              aux (idx + 1)
          (* logs don't match => increment term *)
          | Some l, Some r ->
              assert (not @@ [%equal: LogEntry.t] l r) ;
              R.set_term t.state (local.term + 1)
          | _ ->
              ()
        in
        aux segment_start
    | _ ->
        ()

  let acceptor_reply_term_increase t msg =
    if msg.term > t.state.local_state.term then R.set_term t.state msg.term

  let commit_rate_reporter, should_run_cr_reporter =
    Ocons_core.Utils.InternalReporter.rate_reporter 0 "commit rate"

  let check_commit (t : t) =
    should_run_cr_reporter := true ;
    let vterm_votes =
      List.filter_map t.config.other_replica_ids ~f:(fun nid ->
          let s = Map.find_exn t.state_cache nid in
          if s.vterm = t.state.local_state.vterm then Some s else None )
    in
    let rec aux idx =
      if Log.mem t.state.local_state.vval idx then
        let lv = Log.find t.state.local_state.vval idx in
        let vc =
          List.count vterm_votes ~f:(fun s ->
              [%equal: LogEntry.t option] lv (Log.find s.vval idx) )
        in
        if vc + 1 >= t.config.quorum_size then (
          R.set_ci t.state idx ;
          StallChecker.reset t.stall_checker idx ;
          commit_rate_reporter () ;
          aux (idx + 1) )
    in
    aux (t.state.local_state.commit_index + 1)

  let conflict_recovery_rate, should_run_cra_reporter =
    Ocons_core.Utils.InternalReporter.rate_reporter 0 "conflict rate"

  let conflict_recovery_success_rate, should_run_cras_reporter =
    Ocons_core.Utils.InternalReporter.rate_reporter 0 "conflict success rate"

  let max_term_report, should_run_mt_reporter =
    Ocons_core.Utils.InternalReporter.avg_reporter Int.to_float "max_term"

  let max_term_delta, should_run_mtd_reporter =
    Ocons_core.Utils.InternalReporter.avg_reporter Int.to_float
      "max_term - vterm"

  let check_conflict_recovery t =
    should_run_cra_reporter := true ;
    should_run_cras_reporter := true ;
    should_run_mt_reporter := true ;
    should_run_mtd_reporter := true ;
    let votes =
      t.config.replica_ids |> Iter.of_list
      |> Iter.map (function
           | nid when nid = t.config.node_id ->
               t.state.local_state
           | nid ->
               Map.find_exn t.state_cache nid )
    in
    let max_term =
      votes |> Iter.fold (fun acc (s : state) -> max acc s.term) (-1)
    in
    max_term_report max_term ;
    if max_term > t.state.local_state.vterm then (
      conflict_recovery_rate () ;
      max_term_delta (max_term - t.state.local_state.vterm) ;
      let num_max_term_votes =
        votes |> Iter.filter_count (fun (s : state) -> s.term = max_term)
      in
      let all_live_nodes_vote =
        t.config.replica_ids
        |> List.for_all ~f:(fun nid ->
               let ( => ) a b = (not a) || b in
               is_live t nid
               =>
               let s =
                 if nid = t.config.node_id then t.state.local_state
                 else Map.find_exn t.state_cache nid
               in
               s.term = max_term )
      in
      if
        num_max_term_votes >= t.config.quorum_size
        && (true || all_live_nodes_vote)
      then (
        conflict_recovery_success_rate () ;
        let votes =
          votes |> Iter.filter (fun (s : state) -> s.term = max_term)
        in
        let missing_votes = t.config.replica_count - Iter.length votes in
        let max_vterm =
          votes |> Iter.fold (fun acc (s : state) -> max acc s.vterm) (-1)
        in
        let vterm_votes =
          votes |> Iter.filter (fun (s : state) -> s.vterm = max_vterm)
        in
        let o4 num_value_votes =
          num_value_votes + missing_votes >= t.config.quorum_size
        in
        assert (
          let ci = t.state.local_state.commit_index in
          let lv = log_get t.state.local_state.vval ci in
          let vc =
            Iter.filter_count
              (fun v -> [%equal: LogEntry.t] (log_get v.vval ci) lv)
              votes
          in
          vc >= t.config.quorum_size ) ;
        (* idea:
             take current log as target
             if o4 condition holds on current idx, advance
             otherwise choose different log
               If no other log then choose this one (o4 point is in past)
        *)
        log_invariant t ;
        let hd_vote = Iter.head_exn vterm_votes in
        let start_idx = hd_vote.commit_index in
        Log.iteri hd_vote.vval ~lo:start_idx
          ~hi:t.state.local_state.commit_index (fun (idx, v) ->
            R.set_vval t.state idx v ) ;
        log_invariant t ;
        let rec choose_another_log curr others idx =
          match others with
          | [] ->
              (curr, idx)
          | l :: rest ->
              advance_index l rest idx
        and advance_index curr others idx =
          match Log.find curr idx with
          | None ->
              choose_another_log curr others idx
          | Some _ as lv ->
              let same_votes =
                List.filter others ~f:(fun l ->
                    [%equal: LogEntry.t option] lv (log_find l idx) )
              in
              if o4 (List.length same_votes + 1) then
                advance_index curr same_votes (idx + 1)
              else choose_another_log curr others idx
        in
        (* Update local log to maximum committed one *)
        let max_ci =
          vterm_votes
          |> Iter.fold (fun acc (s : state) -> max acc s.commit_index) (-1)
        in
        let max_ci_vote =
          vterm_votes
          |> Iter.find (fun (s : state) ->
                 if s.commit_index = max_ci then Some s else None )
          |> Option.value_exn
        in
        let valid_votes =
          vterm_votes
          |> Iter.map (fun vote -> vote.vval)
          |> Iter.filter (fun v ->
                 let mcv = log_find max_ci_vote.vval max_ci in
                 let rv = log_find v max_ci in
                 [%equal: LogEntry.t option] rv mcv )
          |> Iter.to_list
        in
        let chosen_value, diverge =
          advance_index (List.hd_exn valid_votes) (List.tl_exn valid_votes)
            start_idx
        in
        assert (
          Iter.int_range ~start:start_idx ~stop:(diverge - 1) (fun idx ->
              let lv = Log.get chosen_value idx in
              let vc =
                Iter.filter_count
                  (fun (v : state) ->
                    [%equal: LogEntry.t option] (Some lv) (Log.find v.vval idx)
                    )
                  vterm_votes
              in
              if not (o4 vc) then
                Fmt.failwith "Insufficient votes at %d, in %d %d" idx start_idx
                  diverge ) ;
          true ) ;
        log_invariant t ;
        log_continuous chosen_value t.ctree ;
        Log.iteri chosen_value ~lo:start_idx (fun (idx, v) ->
            R.set_vval t.state idx v ) ;
        log_invariant t ;
        R.set_term t.state max_term ;
        R.set_vterm t.state max_term ;
        R.add_commands t.state t.ctree
          (fun f ->
            Queue.iter t.command_queue ~f ;
            Queue.clear t.command_queue )
          () ) )

  let replicate_state ?(force = false) t =
    t.config.other_replica_ids
    |> List.iter ~f:(fun nid ->
           if force || R.requires_update t.state nid then
             send nid (R.get_msg_to_send t.state t.ctree nid) )

  let failure_detector_update t (event : message event) =
    match event with
    | Recv (_, src) ->
        Hashtbl.set t.failure_detector.state ~key:src ~data:t.config.fd_timeout
    | _ ->
        ()

  let handle_event t (event : message event) =
    failure_detector_update t event ;
    match event with
    | Tick ->
        Hashtbl.map_inplace t.failure_detector.state ~f:(fun v -> v - 1) ;
        check_conflict_recovery t ;
        replicate_state t ~force:true ;
        stall_check t
    | Recv (m, src) ->
        log_invariant t ;
        R.apply_update t.ctree (Map.find_exn t.state_cache src) m ;
        log_invariant t ;
        acceptor_reply t src m.vval_seg ;
        log_invariant t ;
        check_commit t ;
        log_invariant t ;
        check_conflict_recovery t ;
        log_invariant t ;
        replicate_state t
    | Commands ci when not (t.state.local_state.term = t.state.local_state.vterm)
      ->
        ci (Queue.enqueue t.command_queue)
    | Commands ci ->
        R.add_commands t.state t.ctree ci () ;
        check_commit t ;
        replicate_state t

  let advance t e =
    run_side_effects
      (fun () -> Exn.handle_uncaught_and_exit (fun () -> handle_event t e))
      t
  (*
    run_side_effects
      (fun () ->
        try handle_event t e
        with ex ->
          Exn.reraise ex
            (Fmt.str "Failed while handling %a"
               (event_pp ~pp_msg:PP.message_pp : message event Fmt.t)
               e ) )
      t
      *)

  let create (config : config) =
    let other_nodes_set =
      config.other_replica_ids |> Set.of_list (module Int)
    in
    { config
    ; state= Replication.create config.other_replica_ids
    ; ctree= CommandTree.create ()
    ; state_cache=
        Map.of_key_set other_nodes_set ~f:(fun _ -> Replication.init_state ())
    ; failure_detector=
        { state=
            Hashtbl.of_alist_exn
              (module Int)
              ( config.other_replica_ids
              |> List.map ~f:(fun id -> (id, config.fd_timeout)) ) }
    ; command_queue= Queue.create ()
    ; stall_checker= {ticks_since_commit= 2; prev_commit_idx= -1; tick_limit= 2}
    }
end

module Impl = Make (ImperativeActions (Types))

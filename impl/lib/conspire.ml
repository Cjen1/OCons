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
end

module LogEntry = struct
  module T = struct
    type t = Value.t [@@deriving compare, hash, bin_io, sexp]
  end

  include T
  include Core.Comparable.Make (T)
end

module GlobalTypes = struct
  type log_entry = Value.t [@@deriving compare, equal, bin_io]

  let log_entry_pp = Value.pp

  type log_update = {segment_entries: log_entry list; segment_start: log_index}
  [@@deriving bin_io]

  let log_update_pp =
    let open Fmt in
    record
      [ field "start" (fun u -> u.segment_start) int
      ; field "entries"
          (fun u -> u.segment_entries)
          (Fmt.brackets @@ list ~sep:semi log_entry_pp) ]

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
          (brackets @@ list ~sep:comma log_entry_pp) ]

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

module Replication = struct
  open GlobalTypes

  type local = state

  type remote = state

  type update_tracker = {log_diverged_from: log_index option; other_change: bool}

  type t =
    { local_state: local
    ; expected_remote: remote
    ; mutable sent_upto: log_index
    ; mutable change_flag: bool }

  let init_state () = {term= 0; vterm= 0; vval= Log.create []; commit_index= -1}

  let create () =
    { local_state= init_state ()
    ; expected_remote= init_state ()
    ; sent_upto= -1
    ; change_flag= false }

  (* Corresponding to new_state_cache := state with ![src] = m *)
  let apply_update (state : state) (m : message) =
    state.vterm <- m.vterm ;
    state.term <- m.term ;
    state.commit_index <- m.commit_index ;
    let log = state.vval in
    Log.cut_after log (m.vval_seg.segment_start - 1) ;
    Iter.of_list m.vval_seg.segment_entries
    |> Iter.iteri (fun i v -> Log.set log (i + m.vval_seg.segment_start) v)

  let requires_update t =
    t.change_flag || Log.highest t.local_state.vval > t.sent_upto

  let update_to_msg t =
    let segment_start = t.sent_upto + 1 in
    { term= t.local_state.term
    ; vterm= t.local_state.vterm
    ; vval_seg=
        { segment_start
        ; segment_entries=
            Log.iter t.local_state.vval ~lo:segment_start |> Iter.to_list }
    ; commit_index= t.local_state.commit_index }

  let get_msg_to_send t =
    let msg = update_to_msg t in
    apply_update t.expected_remote msg ;
    t.sent_upto <- Log.highest t.local_state.vval ;
    t.change_flag <- false ;
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
      {segment_start= lo; segment_entries= Log.iter ~lo ~hi v |> Iter.to_list}
    in
    Fmt.pf ppf "%a" log_update_pp seg

  type update_kind =
    | CommitIndex of log_index
    | Term of term
    | VTerm of term
    | VVal of (log_index * log_entry)

  let set t = function
    | CommitIndex idx when idx > t.local_state.commit_index ->
        t.local_state.commit_index <- idx ;
        t.change_flag <- true
    | Term term when not (term = t.local_state.term) ->
        assert (term > t.local_state.term) ;
        t.local_state.term <- term ;
        t.change_flag <- true
    | VTerm vterm when not (vterm = t.local_state.vterm) ->
        assert (vterm > t.local_state.vterm) ;
        t.local_state.vterm <- vterm ;
        t.change_flag <- true
    | VVal (idx, v)
      when not
             ([%equal: log_entry option] (Some v)
                (Log.find t.local_state.vval idx) ) ->
        t.sent_upto <- min t.sent_upto (idx - 1) ;
        Log.set t.local_state.vval idx v
    | _ ->
        ()

  let pp =
    let open Fmt in
    record
      [ field "change_flag" (fun t -> t.change_flag) bool
      ; field "sent_upto" (fun t -> t.sent_upto) int
      ; field "local" (fun t -> t.local_state) state_pp
      ; field "expected" (fun t -> t.expected_remote) state_pp ]
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
    { state: Replication.t
    ; state_cache: (node_id, Replication.remote, Int.comparator_witness) Map.t
    ; failure_detector: fd_sm
    ; config: config
    ; command_queue: Command.t Queue.t
    ; stall_checker: StallChecker.t }

  let get_command idx t = Log.get t.state.local_state.vval idx |> Iter.of_list

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
end

module Make
    (Act : ActionSig
             with type t = Types.t
              and type message = GlobalTypes.message) =
struct
  open GlobalTypes
  open Replication
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

  let replicate_state ?(force = false) t =
    if force || requires_update t.state then
      let msg = get_msg_to_send t.state in
      broadcast msg

  let acceptor_update t src (m : message) =
    let cached_state = Map.find_exn t.state_cache src in
    let should_overwrite =
      m.vterm > t.state.local_state.term || m.vterm > t.state.local_state.vterm
    in
    ( if
        m.vterm >= t.state.local_state.term
        && m.vterm >= t.state.local_state.vterm
      then
        let rec aux idx =
          match
            ( Log.find t.state.local_state.vval idx
            , Log.find cached_state.vval idx )
          with
          (* No more local state => diverge but not conflicting *)
          | None, Some _ ->
              Log.iteri cached_state.vval ~lo:idx (fun (idx, v) ->
                  set t.state (VVal (idx, v)) ) ;
              set t.state (VTerm m.vterm) ;
              set t.state (Term m.vterm)
          (* Logs don't match, check for overwrite*)
          | Some l, Some r when not @@ [%compare.equal: LogEntry.t] l r ->
              if should_overwrite then (
                (* newer vterm/term => overwrite local state *)
                Log.iteri cached_state.vval ~lo:idx (fun (idx, v) ->
                    set t.state (VVal (idx, v)) ) ;
                set t.state (VTerm m.vterm) ;
                set t.state (Term m.vterm) )
              else
                (* not matching and shouldn't overwrite => increment *)
                set t.state (Term (t.state.local_state.term + 1))
          (* logs match and local state => recurse *)
          | Some _, Some _ ->
              aux (idx + 1)
          (* no more remote state => no change *)
          | _, None ->
              ()
        in
        aux m.vval_seg.segment_start )

  let commit_rate_reporter, should_run_cr_reporter =
    Ocons_core.Utils.InternalReporter.rate_reporter 0 "commit rate"

  let rec check_commit t =
    should_run_cr_reporter := true ;
    let checked_index = t.state.local_state.commit_index + 1 in
    let votes =
      t.config.replica_ids |> Iter.of_list
      |> Iter.map (function
           | nid when nid = t.config.node_id ->
               t.state.local_state
           | nid ->
               Map.find_exn t.state_cache nid )
    in
    let max_vterm =
      votes |> Iter.fold (fun acc (s : state) -> max acc s.vterm) (-1)
    in
    let r, count =
      votes
      |> Iter.filter_map (fun (s : state) ->
             if s.vterm = max_vterm then Log.find s.vval checked_index else None )
      |> Iter.count ~hash:Value.hash ~eq:[%compare.equal: Value.t]
      |> fun r ->
      IterLabels.fold ~init:([], -1)
        ~f:(fun (max_v, c) (v, count) ->
          if count > c then (v, count) else (max_v, c) )
        r
    in
    if count >= t.config.quorum_size then (
      set t.state (VVal (checked_index, r)) ;
      set t.state (CommitIndex checked_index) ;
      StallChecker.reset t.stall_checker t.state.local_state.commit_index ;
      commit_rate_reporter () ;
      check_commit t )

  let conflict_recovery_rate, should_run_cra_reporter =
    Ocons_core.Utils.InternalReporter.rate_reporter 0 "conflict rate"

  let conflict_recovery_success_rate, should_run_cras_reporter =
    Ocons_core.Utils.InternalReporter.rate_reporter 0 "conflict success rate"

  let max_term_report, should_run_mt_reporter =
    Ocons_core.Utils.InternalReporter.avg_reporter Int.to_float "max_term"

  let max_term_delta, should_run_mtd_reporter =
    Ocons_core.Utils.InternalReporter.avg_reporter Int.to_float
      "max_term - vterm"

  (* All live nodes vote *)
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
      if num_max_term_votes >= t.config.quorum_size && all_live_nodes_vote then (
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
        let max_idx =
          vterm_votes
          |> Iter.fold (fun prev v -> max prev @@ Log.highest v.vval) (-1)
        in
        let merge values =
          let s = Set.empty (module Command) in
          IterLabels.fold values ~init:s ~f:(fun s vs ->
              List.fold vs ~init:s ~f:Set.add )
          |> Set.to_list
        in
        let state = ref `O4 in
        Iter.int_range ~start:(t.state.local_state.commit_index + 1)
          ~stop:max_idx (fun idx ->
            let values =
              vterm_votes |> Iter.filter_map (fun v -> Log.find v.vval idx)
            in
            match !state with
            | `O4 -> (
                let value_counts =
                  Iter.count ~hash:Value.hash ~eq:[%compare.equal: Value.t]
                    values
                in
                let o4_value = ref None in
                value_counts (fun (v, c) ->
                    if o4 c then (
                      assert (Option.is_none !o4_value) ;
                      o4_value := Some v ) ) ;
                match !o4_value with
                | None ->
                    set t.state (VVal (idx, merge values)) ;
                    state := `NonO4
                | Some v ->
                    set t.state (VVal (idx, v)) ;
                    state := `O4 )
            | `NonO4 ->
                set t.state (VVal (idx, merge values)) ;
                state := `NonO4 ) ;
        set t.state (VTerm max_term) ;
        set t.state (Term max_term) ;
        (* TODO avoid block when conflict *)
        let start = Log.highest t.state.local_state.vval + 1 in
        Queue.iteri t.command_queue ~f:(fun i c ->
            set t.state (VVal (i + start, [c])) ) ;
        Queue.clear t.command_queue ) )

  let failure_detector_update t (event : message event) =
    match event with
    | Recv (_, src) ->
        Hashtbl.set t.failure_detector.state ~key:src ~data:t.config.fd_timeout
    | _ ->
        ()

  let command_added_reporter, should_run_ca_reporter =
    Ocons_core.Utils.InternalReporter.rate_reporter 0 "commands added"

  let handle_event t (event : message event) =
    should_run_ca_reporter := true ;
    failure_detector_update t event ;
    match event with
    | Tick ->
        Hashtbl.map_inplace t.failure_detector.state ~f:(fun v -> v - 1) ;
        check_conflict_recovery t ;
        replicate_state t ~force:true ;
        stall_check t
    | Recv (m, src) ->
        Replication.apply_update (Map.find_exn t.state_cache src) m ;
        acceptor_update t src m ;
        check_commit t ;
        check_conflict_recovery t ;
        replicate_state t
    | Commands ci when not (t.state.local_state.term = t.state.local_state.vterm)
      ->
        ci
        |> Iter.iter (fun c ->
               Queue.enqueue t.command_queue c ;
               command_added_reporter () )
    | Commands ci ->
        let start = Log.highest t.state.local_state.vval + 1 in
        ci
        |> Iter.iteri (fun i c ->
               set t.state (VVal (i + start, [c])) ;
               command_added_reporter () ) ;
        check_commit t ;
        replicate_state t

  let advance t e =
    run_side_effects
      (fun () ->
        try handle_event t e
        with ex ->
          Exn.reraise ex
            (Fmt.str "Failed while handling %a"
               (event_pp ~pp_msg:PP.message_pp : message event Fmt.t)
               e ) )
      t

  let create (config : config) =
    let other_nodes_set =
      config.other_replica_ids |> Set.of_list (module Int)
    in
    { config
    ; state= Replication.create ()
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

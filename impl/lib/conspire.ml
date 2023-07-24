open! Core
open! Types
open! Utils
open C.Types
open Actions_f
open Ocons_core.Consensus_intf
module IdMap = Iter.Map.Make (Int)

module Value = struct
  open Core

  (* sorted list by command_id *)
  type t = command list [@@deriving compare, hash, bin_io, sexp]
end

module LogEntry = struct
  module T = struct
    type t = Value.t [@@deriving compare, hash, bin_io, sexp]
  end

  include T
  include Core.Comparable.Make (T)
end

module Types = struct
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

  type log_entry = Value.t [@@deriving compare, bin_io]

  type log_update = {segment_entries: log_entry list; segment_start: log_index}
  [@@deriving bin_io]

  type message =
    {term: term; vval: log_update; vterm: term; commit_index: log_index}
  [@@deriving bin_io]

  type state = {vval: log_entry Log.t; mutable vterm: term; mutable term: term}

  type stall_checker = {mutable ticks_since_commit: int}

  type t =
    { local_state: state
    ; state_cache: (node_id, state, Int.comparator_witness) Map.t
    ; sent_cache: (node_id, int ref, Int.comparator_witness) Map.t
    ; mutable commit_index: log_index
    ; failure_detector: fd_sm
    ; config: config
    ; command_queue: Command.t Queue.t
    ; stall_checker: stall_checker }

  let get_command idx t = Log.get t.local_state.vval idx |> Iter.of_list

  let get_commit_index t = t.commit_index

  module PP = struct
    open Fmt

    let value_pp : Value.t Fmt.t = brackets @@ list ~sep:comma Command.pp

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

    let log_entry_pp = value_pp

    let log_update_pp =
      record
        [ field "start" (fun u -> u.segment_start) int
        ; field "entries"
            (fun u -> u.segment_entries)
            (Fmt.brackets @@ list ~sep:semi log_entry_pp) ]

    let message_pp =
      record
        [ field "term" (fun (s : message) -> s.term) int
        ; field "commit_index" (fun (s : message) -> s.commit_index) int
        ; field "vval" (fun (s : message) -> s.vval) log_update_pp
        ; field "vterm" (fun (s : message) -> s.vterm) int ]

    let state_pp =
      record
        [ field "term" (fun s -> s.term) int
        ; field "vterm" (fun s -> s.vterm) int
        ; field "vval"
            (fun s -> Log.iter s.vval |> Iter.to_list)
            (brackets @@ list ~sep:comma log_entry_pp) ]

    let fd_sm_pp =
      record
        [ field "state"
            (fun v ->
              Hashtbl.to_alist v.state
              |> List.sort ~compare:(fun (a, _) (b, _) -> Int.compare a b) )
            ( brackets @@ list ~sep:semi @@ parens
            @@ pair ~sep:(Fmt.any ":@ ") int int ) ]

    let t_pp =
      record
        [ field "config" (fun t -> t.config) config_pp
        ; field "commit_index" (fun t -> t.commit_index) int
        ; field "failure_detector" (fun t -> t.failure_detector) fd_sm_pp
        ; field "local_state" (fun t -> t.local_state) state_pp
        ; field "sent_cache"
            (fun t ->
              t.sent_cache |> Map.to_alist
              |> List.map ~f:(fun (a, b) -> (a, !b)) )
            (list @@ parens @@ pair ~sep:(Fmt.any ":@ ") int int)
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
    (Act : ActionSig with type t = Types.t and type message = Types.message) =
struct
  include Types
  open Act

  let is_live t nid =
    nid = t.config.node_id || Hashtbl.find_exn t.failure_detector.state nid > 0

  let reset_stall_checker t = t.stall_checker.ticks_since_commit <- 10

  let stall_check t =
    let is_stalled = t.stall_checker.ticks_since_commit <= 0 in
    let should_make_progress =
      Log.highest t.local_state.vval > t.commit_index
    in
    if should_make_progress && is_stalled then (
      let problem_idx = t.commit_index + 1 in
      traceln "========== Stalled ==========" ;
      traceln "%d stalled on %d" t.config.node_id problem_idx ;
      (* Print the index for each replica
         Print the sent_cache for each*)
      let states =
        t.config.replica_ids |> Iter.of_list
        |> Iter.map (function
             | nid when nid = t.config.node_id ->
                 (nid, t.local_state)
             | nid ->
                 (nid, Map.find_exn t.state_cache nid) )
      in
      let s_pp : (node_id * state) Fmt.t =
        Fmt.(
          record
            [ field "nid" (fun (id, _) -> id) int
            ; field "term" (fun (_, (s : state)) -> s.term) int
            ; field "vterm" (fun (_, (s : state)) -> s.vterm) int
            ; field "vval"
                (fun (_, (s : state)) ->
                  let lo = problem_idx - 5 in
                  let lo = max 0 lo in
                  let hi = problem_idx + 5 in
                  let hi = min hi (Log.highest s.vval) in
                  { segment_start= lo
                  ; segment_entries= Log.iter ~lo ~hi s.vval |> Iter.to_list }
                  )
                PP.log_update_pp ] )
      in
      traceln "%a@." Fmt.(list ~sep:cut s_pp) (states |> Iter.to_list) ;
      traceln "Sent cache" ;
      traceln "%a@."
        Fmt.(list @@ parens @@ pair ~sep:(Fmt.any ":@ ") int int)
        (t.sent_cache |> Map.to_alist |> List.map ~f:(fun (a, b) -> (a, !b))) ;
      traceln "========== Stalled ==========" ) ;
    if is_stalled then reset_stall_checker t

  type update_tracker =
    {mutable diverge_from: log_index option; mutable force: bool}

  let new_update_tracker () = {diverge_from= None; force= false}

  let update_diverge track idx =
    let r =
      match track.diverge_from with None -> idx | Some prev -> min prev idx
    in
    track.diverge_from <- Some r

  let tracked_set track log idx v =
    let exists = Log.mem log idx in
    if (not exists) || not ([%compare.equal: Value.t] (Log.get log idx) v) then (
      update_diverge track idx ; Log.set log idx v )

  let replicate_state t tracker =
    let rep ?diverge_from t dst =
      let sent_upto = Map.find_exn t.sent_cache dst in
      let segment_start =
        match diverge_from with
        | None ->
            !sent_upto + 1
        | Some idx ->
            min idx (!sent_upto + 1)
      in
      send dst
        { term= t.local_state.term
        ; vterm= t.local_state.vterm
        ; vval=
            { segment_start
            ; segment_entries=
                Log.iter t.local_state.vval ~lo:segment_start |> Iter.to_list }
        ; commit_index= t.commit_index } ;
      sent_upto := Log.highest t.local_state.vval
    in
    match tracker with
    | {diverge_from= Some _; _} | {force= true; _} ->
        List.iter t.config.other_replica_ids
          ~f:(rep t ?diverge_from:tracker.diverge_from)
    | _ ->
        ()

  (* Corresponding to new_state_cache := state with ![src] = m *)
  let update_cache t src (m : message) =
    let cached_state = Map.find_exn t.state_cache src in
    cached_state.vterm <- m.vterm ;
    cached_state.term <- m.term ;
    let log = cached_state.vval in
    Log.cut_after log (m.vval.segment_start - 1) ;
    Iter.of_list m.vval.segment_entries
    |> Iter.iteri (fun i v -> Log.set log (i + m.vval.segment_start) v)

  let update_commit_index_from_msg t src (m : message) tracker =
    if t.commit_index < m.commit_index then (
      let old_ci = t.commit_index in
      let cached_state = Map.find_exn t.state_cache src in
      Iter.int_range ~start:(t.commit_index + 1) ~stop:m.commit_index
      |> Iter.iter (fun idx ->
             tracked_set tracker t.local_state.vval idx
               (Log.get cached_state.vval idx) ) ;
      t.commit_index <- m.commit_index ;
      update_diverge tracker old_ci )

  let acceptor_update t src (m : message) tracker =
    let cached_state = Map.find_exn t.state_cache src in
    let should_overwrite =
      m.vterm > t.local_state.term || m.vterm > t.local_state.vterm
    in
    if m.vterm >= t.local_state.term && m.vterm >= t.local_state.vterm then
      let rec aux idx =
        match
          (Log.find t.local_state.vval idx, Log.find cached_state.vval idx)
        with
        (* No more local state => diverge but not conflicting *)
        | None, Some _ ->
            Log.iteri cached_state.vval ~lo:idx (fun (idx, v) ->
                Log.set t.local_state.vval idx v ) ;
            t.local_state.vterm <- m.vterm ;
            t.local_state.term <- m.vterm ;
            update_diverge tracker idx
        (* Logs don't match, check for overwrite*)
        | Some l, Some r when not @@ [%compare.equal: LogEntry.t] l r ->
            if should_overwrite then (
              (* newer vterm/term => overwrite local state *)
              Log.iteri cached_state.vval ~lo:idx (fun (idx, v) ->
                  Log.set t.local_state.vval idx v ) ;
              t.local_state.vterm <- m.vterm ;
              t.local_state.term <- m.vterm ;
              update_diverge tracker idx )
            else (
              (* not matching and shouldn't overwrite => increment *)
              t.local_state.term <- t.local_state.term + 1 ;
              tracker.force <- true )
        (* logs match and local state => recurse *)
        | Some _, Some _ ->
            aux (idx + 1)
        (* no more remote state => no change *)
        | _, None ->
            ()
      in
      aux m.vval.segment_start

  let rec check_commit t tracker =
    let checked_index = t.commit_index + 1 in
    let votes =
      t.config.replica_ids |> Iter.of_list
      |> Iter.map (function
           | nid when nid = t.config.node_id ->
               t.local_state
           | nid ->
               Map.find_exn t.state_cache nid )
    in
    let max_vterm = votes |> Iter.fold (fun acc s -> max acc s.vterm) (-1) in
    let r, count =
      votes
      |> Iter.filter_map (fun s ->
             if s.vterm = max_vterm then Log.find s.vval checked_index else None )
      |> Iter.count ~hash:Value.hash ~eq:[%compare.equal: Value.t]
      |> fun r ->
      IterLabels.fold ~init:([], -1)
        ~f:(fun (max_v, c) (v, count) ->
          if count > c then (v, count) else (max_v, c) )
        r
    in
    if count >= t.config.quorum_size then (
      Log.set t.local_state.vval checked_index r ;
      t.commit_index <- checked_index ;
      reset_stall_checker t ;
      tracker.force <- true ;
      check_commit t tracker )

  (* All live nodes vote *)
  let check_conflict_recovery t tracker =
    let votes =
      t.config.replica_ids |> Iter.of_list
      |> Iter.map (function
           | nid when nid = t.config.node_id ->
               t.local_state
           | nid ->
               Map.find_exn t.state_cache nid )
    in
    let max_term = votes |> Iter.fold (fun acc s -> max acc s.term) (-1) in
    if max_term > t.local_state.vterm then
      let num_max_term_votes =
        votes |> Iter.filter_count (fun s -> s.term = max_term)
      in
      let all_live_nodes_vote =
        t.config.replica_ids
        |> List.for_all ~f:(fun nid ->
               let ( => ) a b = (not a) || b in
               is_live t nid
               =>
               let s =
                 if nid = t.config.node_id then t.local_state
                 else Map.find_exn t.state_cache nid
               in
               s.term = max_term )
      in
      if num_max_term_votes >= t.config.quorum_size && all_live_nodes_vote then (
        let votes = votes |> Iter.filter (fun s -> s.term = max_term) in
        let missing_votes = t.config.replica_count - Iter.length votes in
        let max_vterm =
          votes |> Iter.fold (fun acc s -> max acc s.vterm) (-1)
        in
        let vterm_votes = votes |> Iter.filter (fun s -> s.vterm = max_vterm) in
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
        Iter.int_range ~start:(t.commit_index + 1) ~stop:max_idx (fun idx ->
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
                    tracked_set tracker t.local_state.vval idx (merge values) ;
                    state := `NonO4
                | Some v ->
                    tracked_set tracker t.local_state.vval idx v ;
                    state := `O4 )
            | `NonO4 ->
                tracked_set tracker t.local_state.vval idx (merge values) ;
                state := `NonO4 ) ;
        t.local_state.vterm <- max_term ;
        t.local_state.term <- max_term ;
        (* TODO avoid block when conflict *)
        let start = Log.highest t.local_state.vval + 1 in
        Queue.iteri t.command_queue ~f:(fun i c ->
            tracked_set tracker t.local_state.vval (i + start) [c] ) ;
        Queue.clear t.command_queue )

  let failure_detector_update t (event : message event) =
    match event with
    | Recv (_, src) ->
        Hashtbl.set t.failure_detector.state ~key:src ~data:t.config.fd_timeout
    | _ ->
        ()

  let trace_local_state change t =
    if Ocons_core.Utils.debug_flag && false then
      dtraceln "%s: %a" change (Fmt.braces PP.state_pp) t.local_state

  let handle_event t (event : message event) =
    failure_detector_update t event ;
    match event with
    | Tick ->
        Hashtbl.map_inplace t.failure_detector.state ~f:(fun v -> v - 1) ;
        let update_tracker = new_update_tracker () in
        check_conflict_recovery t update_tracker ;
        update_tracker.force <- true ;
        replicate_state t update_tracker
    | Recv (m, src) ->
        let update_tracker = new_update_tracker () in
        update_cache t src m ;
        trace_local_state "init" t ;
        update_commit_index_from_msg t src m update_tracker ;
        trace_local_state "commit_index update" t ;
        acceptor_update t src m update_tracker ;
        trace_local_state "acceptor_resp" t ;
        check_commit t update_tracker ;
        trace_local_state "try_commit" t ;
        check_conflict_recovery t update_tracker ;
        trace_local_state "conflict_recovery" t ;
        replicate_state t update_tracker
    | Commands ci when not (t.local_state.term = t.local_state.vterm) ->
        ci |> Iter.iter (Queue.enqueue t.command_queue)
    | Commands ci ->
        let update_tracker = new_update_tracker () in
        let start = Log.highest t.local_state.vval + 1 in
        ci
        |> Iter.iteri (fun i c ->
               tracked_set update_tracker t.local_state.vval (i + start) [c] ) ;
        check_commit t update_tracker ;
        replicate_state t update_tracker

  let advance t e = run_side_effects (fun () -> handle_event t e) t

  let init_state () = {term= 0; vterm= 0; vval= Log.create []}

  let create (config : config) =
    let other_nodes_set =
      config.other_replica_ids |> Set.of_list (module Int)
    in
    { config
    ; local_state= init_state ()
    ; state_cache= Map.of_key_set other_nodes_set ~f:(fun _ -> init_state ())
    ; sent_cache= Map.of_key_set other_nodes_set ~f:(fun _ -> ref (-1))
    ; commit_index= -1
    ; failure_detector=
        { state=
            Hashtbl.of_alist_exn
              (module Int)
              ( config.other_replica_ids
              |> List.map ~f:(fun id -> (id, config.fd_timeout)) ) }
    ; command_queue= Queue.create ()
    ; stall_checker= {ticks_since_commit= 10} }
end

module Impl = Make (ImperativeActions (Types))

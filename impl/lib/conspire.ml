open Core
open Types
open Utils
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

module LogEntryOptionalImmediate = struct end

module Types = struct
  type config =
    { node_id: node_id
    ; replica_ids: node_id list
    ; other_replica_ids: node_id list
    ; replica_count: int
    ; quorum_size: int
    ; fd_timeout: int }

  let make_config ~node_id ~replica_ids ~fd_timeout () : config =
    let floor f = f |> Int.of_float in
    let replica_count = List.length replica_ids in
    let quorum_size = floor (2. *. Float.of_int replica_count /. 3.) + 1 in
    assert (3 * quorum_size > 2 * replica_count) ;
    { node_id
    ; replica_ids
    ; other_replica_ids= List.filter replica_ids ~f:(fun i -> not (i = node_id))
    ; replica_count
    ; quorum_size
    ; fd_timeout }

  type fd_sm = {state: (node_id, int) Hashtbl.t}

  type log_entry = Value.t [@@deriving compare, bin_io]

  type log_update = {segment_entries: log_entry list; segment_start: log_index}

  type message =
    {term: term; vval: log_update; vterm: term; commit_index: log_index}

  type state = {vval: log_entry Log.t; mutable vterm: term; mutable term: term}

  type t =
    { local_state: state
    ; state_cache: (node_id, state, Int.comparator_witness) Map.t
    ; sent_cache: (node_id, int ref, Int.comparator_witness) Map.t
    ; mutable commit_index: log_index
    ; failure_detector: fd_sm
    ; config: config
    ; command_queue: Command.t Queue.t }

  let get_command idx t = Log.get t.local_state.vval idx |> Iter.of_list

  let get_commit_index t = t.commit_index

  module PP = struct
    open Fmt

    let value_pp : Value.t Fmt.t = brackets @@ list ~sep:comma Command.pp

    let invrs_pp ppf invrs =
      match
        List.filter_map invrs ~f:(function
          | _, false ->
              None
          | s, true ->
              Some s )
      with
      | ls when List.is_empty ls ->
          pf ppf "Ok"
      | ls ->
          pf ppf "violated: %a" (brackets @@ list ~sep:comma string) ls

    let config_pp : config Fmt.t =
      let get_invrs cfg =
        [ ( "node_id not in other_replicas"
          , Stdlib.List.mem cfg.node_id cfg.other_replica_ids )
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
            @@ pair ~sep:(Fmt.any ":@ ") int state_pp ) ]
  end
end

module Make
    (Act : ActionSig with type t = Types.t and type message = Types.message) =
struct
  include Types
  open Act

  type update_tracker =
    {mutable diverge_from: log_index option; mutable force: bool}

  let new_update_tracker () = {diverge_from= None; force= false}

  let update_diverge track idx =
    Option.iter track.diverge_from ~f:(fun d ->
        track.diverge_from <- Some (min d idx) )

  let tracked_set track log idx v =
    let exists = Log.mem log idx in
    if (not exists) || not ([%compare.equal: Value.t] (Log.get log idx) v) then (
      update_diverge track idx ; Log.set log idx v )

  let replicate_state t tracker =
    Fmt.pr "diverge: %a@."
      Fmt.(option ~none:(Fmt.any "None") int)
      tracker.diverge_from ;
    let rep ?diverge_from t dst =
      let segment_start =
        let sent_upto = !(Map.find_exn t.sent_cache dst) in
        match diverge_from with
        | None ->
            sent_upto
        | Some idx ->
            min idx sent_upto
      in
      send dst
        { term= t.local_state.term
        ; vterm= t.local_state.vterm
        ; vval=
            { segment_start
            ; segment_entries=
                Log.iter t.local_state.vval ~lo:segment_start |> Iter.to_list }
        ; commit_index= t.commit_index }
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
      tracker.force <- true ;
      check_commit t tracker )

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
    let num_max_term_votes =
      votes |> Iter.filter_count (fun s -> s.term = max_term)
    in
    if num_max_term_votes >= t.config.quorum_size then
      let votes = votes |> Iter.filter (fun s -> s.term = max_term) in
      let missing_votes = t.config.replica_count - Iter.length votes in
      let max_vterm = votes |> Iter.fold (fun acc s -> max acc s.vterm) (-1) in
      let vterm_votes = votes |> Iter.filter (fun s -> s.vterm = max_vterm) in
      let o4 num_value_votes =
        num_value_votes + missing_votes >= t.config.quorum_size
      in
      let unfolder s idx =
        if Log.mem s.vval idx then Some (Log.get s.vval idx, idx + 1) else None
      in
      let seqs =
        vterm_votes
        |> Iter.map (fun s -> Seq.unfold (unfolder s) (t.commit_index + 1))
        |> seq_zip
        |> Seq.mapi (fun i s -> (i + t.commit_index + 1, s))
      in
      let merge_votes (counts : (log_entry, int) Hashtbl.t) idx =
        let v =
          counts |> Hashtbl.keys |> List.concat
          |> List.sort ~compare:Command.compare
        in
        tracked_set tracker t.local_state.vval idx v
      in
      let found_non_o4 = ref false in
      let check_o4_value counts idx =
        let o4_value =
          Hashtbl.fold counts ~init:None ~f:(fun ~key ~data:count prev ->
              match prev with
              | Some v ->
                  Some v
              | None ->
                  if o4 count then Some key else None )
        in
        match o4_value with
        | Some v ->
            tracked_set tracker t.local_state.vval idx v
        | None ->
            found_non_o4 := true ;
            merge_votes counts idx
      in
      seqs
      |> Seq.iter (fun (idx, votes_for_idx) ->
             let counts = Hashtbl.create (module Value) in
             votes_for_idx |> Iter.iter (fun v -> Hashtbl.incr counts v) ;
             if not !found_non_o4 then check_o4_value counts idx
             else merge_votes counts idx )
  (* TODO add commands from q *)

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
        let update_tracker = new_update_tracker () in
        check_conflict_recovery t update_tracker ;
        update_tracker.force <- true ;
        replicate_state t update_tracker
    | Recv (m, src) ->
        let update_tracker = new_update_tracker () in
        update_cache t src m ;
        update_commit_index_from_msg t src m update_tracker ;
        acceptor_update t src m update_tracker ;
        check_commit t update_tracker ;
        check_conflict_recovery t update_tracker ;
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
    ; sent_cache= Map.of_key_set other_nodes_set ~f:(fun _ -> ref 0)
    ; commit_index= -1
    ; failure_detector=
        { state=
            Hashtbl.of_alist_exn
              (module Int)
              ( config.other_replica_ids
              |> List.map ~f:(fun id -> (id, config.fd_timeout)) ) }
    ; command_queue= Queue.create () }
end

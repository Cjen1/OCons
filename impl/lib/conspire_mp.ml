open! Core
open Types

module Value = struct
  let pp_command = Command.pp

  type t = command list [@@deriving compare, equal, hash, bin_io, sexp, show]

  let empty = []
end

module Conspire = Conspire_f.Make (Value)

(* Actions
   - Command when leader => broadcast
   - Command otherwise => ignore
*)

module StallChecker = struct
  open Utils

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

  let create = {ticks_since_commit= 2; prev_commit_idx= -1; tick_limit= 2}
end

module FailureDetector = struct
  type t =
    { state: (node_id, int) Hashtbl.t
          [@printer Utils.pp_hashtbl [%compare: int * int] Fmt.int Fmt.int]
    ; timeout: int }
  [@@deriving show]

  let tick t = Hashtbl.map_inplace t.state ~f:Int.pred

  let is_live (t : t) nid =
    Hashtbl.find t.state nid
    |> Option.value_map ~default:true ~f:(fun c -> c > 0)

  let reset t nid = Hashtbl.set t.state ~key:nid ~data:t.timeout

  let create timeout other_nodes =
    let state =
      other_nodes
      |> List.map ~f:(fun id -> (id, timeout))
      |> Hashtbl.of_alist_exn (module Int)
    in
    {state; timeout}
end

module Types = struct
  type config =
    { conspire: Conspire_f.config
    ; higher_replica_ids: node_id list
    ; other_replica_ids: node_id list
    ; fd_timeout: int
    ; max_outstanding: int }
  [@@deriving show {with_path= false}]

  let make_config ~node_id ~replica_ids ~fd_timeout ?(max_outstanding = 8192) ()
      : config =
    let floor f = f |> Int.of_float in
    let replica_count = List.length replica_ids in
    let quorum_size = floor (2. *. Float.of_int replica_count /. 3.) + 1 in
    assert (3 * quorum_size > 2 * replica_count) ;
    let other_replica_ids =
      List.filter replica_ids ~f:(fun i -> not (i = node_id))
    in
    let higher_replica_ids =
      List.take_while replica_ids ~f:(fun id -> id <> node_id)
    in
    let conspire =
      Conspire_f.
        {node_id; replica_ids; other_replica_ids; replica_count; quorum_size}
    in
    { conspire
    ; other_replica_ids
    ; higher_replica_ids
    ; fd_timeout
    ; max_outstanding }

  type message = Conspire.message [@@deriving show, bin_io]

  type t =
    { config: config [@opaque]
    ; conspire: Conspire.t
    ; failure_detector: FailureDetector.t
    ; stall_checker: StallChecker.t [@opaque] }
  [@@deriving show {with_path= false}]

  let get_command idx t =
    if idx < 0 then Iter.empty
    else Log.get t.conspire.commit_log idx |> Iter.of_list

  let get_commit_index t = Log.highest t.conspire.commit_log

  module PP = struct
    let message_pp = pp_message

    let t_pp = pp

    let config_pp = pp_config
  end
end

module Make
    (Act : Actions_f.ActionSig
             with type t = Types.t
              and type message = Types.message) =
struct
  include Conspire
  include Types

  let is_leader t =
    List.for_all t.config.higher_replica_ids ~f:(fun nid ->
        not @@ FailureDetector.is_live t.failure_detector nid )

  let can_apply_requests t =
    let term_valid = t.conspire.rep.state.vterm = t.conspire.rep.state.term in
    term_valid && is_leader t

  let available_space_for_commands t =
    if can_apply_requests t then t.config.max_outstanding else 0

  let send ?(force = false) t dst =
    let open Rep in
    let update = get_update_to_send t.conspire.rep dst in
    if force || Option.is_some update.ctree || Option.is_some update.cons then
      Act.send dst (Ok update)

  let nack t dst =
    Act.send dst (Error {commit= t.conspire.rep.state.commit_index})

  let broadcast ?(force = false) t =
    List.iter t.config.other_replica_ids ~f:(fun nid -> send ~force t nid)

  let stall_check (t : t) =
    StallChecker.check t.stall_checker ~pp:(fun ppf () ->
        let problem_idx = t.conspire.rep.state.commit_index in
        Fmt.pf ppf "Stalled on %d at %a@." t.config.conspire.node_id
          (Fmt.option Conspire.CTree.pp_parent_ref_node)
          (Conspire.CTree.get t.conspire.rep.store problem_idx) )

  let nack_counter, run_nc =
    Ocons_core.Utils.InternalReporter.rate_reporter "nacks"

  let ack_counter, run_ac =
    Ocons_core.Utils.InternalReporter.rate_reporter "acks"

  let term_tracker, run_tt =
    Ocons_core.Utils.InternalReporter.avg_reporter Float.of_int "term"

  let value_length, run_vl =
    Ocons_core.Utils.InternalReporter.avg_reporter Float.of_int "value_length"

  let handle_event t (event : message Ocons_core.Consensus_intf.event) =
    run_nc := true ;
    run_ac := true ;
    run_tt := true ;
    run_vl := true ;
    match event with
    | Tick ->
        stall_check t ;
        term_tracker t.conspire.rep.state.term ;
        FailureDetector.tick t.failure_detector ;
        broadcast t ~force:true
    | Commands ci when can_apply_requests t ->
        let commands = Iter.to_list ci in
        value_length (List.length commands) ;
        Conspire.add_commands t.conspire (commands |> Iter.singleton) ;
        broadcast t
    | Commands _ ->
        Fmt.invalid_arg "Commands cannot yet be applied"
    | Recv (m, src) -> (
        FailureDetector.reset t.failure_detector src ;
        let recovery_started, prev_ci =
          ( t.conspire.rep.state.term > t.conspire.rep.state.vterm
          , get_commit_index t )
        in
        let msg_result = Conspire.handle_message t.conspire src m in
        let now_steady_state, committed =
          ( t.conspire.rep.state.term = t.conspire.rep.state.vterm
          , get_commit_index t > prev_ci )
        in
        match (msg_result, recovery_started && now_steady_state, committed) with
        | Error `MustAck, _, _ ->
            ack_counter () ; send t src
        | Error `MustNack, _, _ ->
            nack_counter () ; nack t src
        | Ok _, true, _ | Ok _, _, true ->
            broadcast t
        | _ ->
            send t src )

  let advance t e =
    Act.run_side_effects
      (fun () -> Exn.handle_uncaught_and_exit (fun () -> handle_event t e))
      t

  let create (config : config) =
    let conspire = Conspire.create config.conspire in
    let failure_detector =
      FailureDetector.create config.fd_timeout config.conspire.other_replica_ids
    in
    {config; conspire; failure_detector; stall_checker= StallChecker.create}
end

module Impl = Make (Actions_f.ImperativeActions (Types))

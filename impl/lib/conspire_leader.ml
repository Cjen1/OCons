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
    ; lower_replica_ids: node_id list
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
    let lower_replica_ids =
      List.filter replica_ids ~f:(fun id -> id < node_id)
    in
    let conspire =
      Conspire_f.
        {node_id; replica_ids; other_replica_ids; replica_count; quorum_size}
    in
    {conspire; other_replica_ids; lower_replica_ids; fd_timeout; max_outstanding}

  type message = Conspire.message [@@deriving show, bin_io]

  type t =
    { config: config [@opaque]
    ; conspire: Conspire.t
    ; failure_detector: FailureDetector.t
    }
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

  let is_leader =
    let rep, run =
      Ocons_core.Utils.InternalReporter.rate_reporter "is_leader"
    in
    fun t ->
      run := true ;
      let res =
        List.for_all t.config.lower_replica_ids ~f:(fun nid ->
            not @@ FailureDetector.is_live t.failure_detector nid )
      in
      if res then rep () ;
      res

  let can_apply_requests t =
    let term_valid = t.conspire.rep.state.vterm = t.conspire.rep.state.term in
    term_valid && is_leader t

  let available_space_for_commands t =
    if can_apply_requests t then t.config.max_outstanding else 0

  let send ?(force = false) ?(prune = false) t dst =
    let open Rep in
    let update = get_update_to_send ~prune t.conspire.rep dst in
    if force || Option.is_some update.ctree || Option.is_some update.cons then
      Act.send dst (Ok update)

  let nack t dst =
    Act.send dst (Error {commit= t.conspire.rep.state.commit_index})

  let broadcast ?(force = false) t =
    List.iter t.config.other_replica_ids ~f:(fun nid ->
        send ~force t nid ~prune:(not @@ is_leader t) )

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
        term_tracker t.conspire.rep.state.term ;
        FailureDetector.tick t.failure_detector ;
        if is_leader t then Conspire.conflict_recovery t.conspire |> ignore ;
        broadcast t ~force:true
    | Commands ci when can_apply_requests t ->
        let commands = Iter.to_list ci in
        value_length (List.length commands) ;
        let _ = Conspire.add_commands t.conspire (commands |> Iter.singleton) in
        broadcast t
    | Commands _ ->
        Fmt.invalid_arg "Commands cannot yet be applied"
    | Recv (m, src) -> (
        FailureDetector.reset t.failure_detector src ;
        let update_result = Conspire.handle_update_message t.conspire src m in
        match update_result with
        | Error `MustAck ->
            ack_counter () ; send t src
        | Error (`MustNack reason) ->
            ( match reason with
            | `Root_of_update_not_found _ ->
                Utils.dtraceln "Nack: Update is not rooted"
            | `Commit_index_not_in_tree ->
                Utils.dtraceln "Nack: Commit index not in tree"
            | `VVal_not_in_tree ->
                Utils.dtraceln "Nack: VVal not int tree" ) ;
            nack_counter () ; nack t src
        | Ok () ->
            process_acceptor_state t.conspire src ;
            let conflict_recovery_attempt =
              if is_leader t then Conspire.conflict_recovery t.conspire
              else Error `NotLeader
            in
            Result.iter conflict_recovery_attempt ~f:(fun () ->
                Utils.traceln "Recovery complete term: {t:%d,vt:%d}"
                  t.conspire.rep.state.term t.conspire.rep.state.vterm ) ;
            let recovery_started =
              t.conspire.rep.state.term > t.conspire.rep.state.vterm
            in
            let committed =
              Conspire.check_commit t.conspire |> Option.is_some
            in
            let should_broadcast =
              Result.is_ok conflict_recovery_attempt
              || committed || recovery_started
            in
            if should_broadcast then broadcast t
            else send ~prune:(not @@ is_leader t) t src )

  let advance t e =
    Act.run_side_effects
      (fun () -> Exn.handle_uncaught_and_exit (fun () -> handle_event t e))
      t

  let create (config : config) =
    let conspire = Conspire.create config.conspire in
    let failure_detector =
      FailureDetector.create config.fd_timeout config.conspire.other_replica_ids
    in
    {config; conspire; failure_detector}
end

module Impl = Make (Actions_f.ImperativeActions (Types))

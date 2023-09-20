open! Core
open! Types
open! Utils
open C.Types
open Actions_f
open Ocons_core.Consensus_intf
module IdMap = Iter.Map.Make (Int)
open Conspire_command_tree

let commands_enqueued, run_ce =
  Ocons_core.Utils.InternalReporter.rate_reporter 0 "commands_enqueued"

let commands_applied, run_ca =
  Ocons_core.Utils.InternalReporter.rate_reporter 0 "commands_applied"

module Value = struct
  let pp_command = Command.pp

  (* sorted list by command_id *)
  type t = command list [@@deriving compare, equal, hash, bin_io, sexp, show]

  let pp : t Fmt.t =
    let open Fmt in
    brackets @@ list ~sep:comma Command.pp

  let empty = []
end

module CTree = CommandTree (Value)

module GlobalTypes = struct
  type state =
    { mutable vval: VectorClock.t
    ; mutable vterm: term
    ; mutable term: term
    ; mutable commit_index: VectorClock.t }
  [@@deriving show {with_path= false}, bin_io]

  let init_state nodes =
    { term= 0
    ; vterm= 0
    ; vval= VectorClock.empty nodes 0
    ; commit_index= VectorClock.empty nodes 0 }

  type message = CTreeUpdate of CTree.update | ConsUpdate of state
  [@@deriving show {with_path= false}, bin_io]
end

module Replication = struct
  open GlobalTypes

  type remote_view = {mutable expected: CTree.t [@opaque]}
  [@@deriving show {with_path= false}]

  type t =
    { state: state
    ; mutable store: CTree.t
    ; mutable change_flag: bool
    ; remotes: remote_view Map.M(Int).t
          [@printer Conspire_command_tree.map_pp Fmt.int pp_remote_view] }
  [@@deriving show {with_path= false}]

  let recv_update t src update =
    t.store <- CTree.apply_update t.store update ;
    let remote = Map.find_exn t.remotes src in
    remote.expected <- CTree.apply_update remote.expected update

  let get_ctree_msg t dst =
    let remote = Map.find_exn t.remotes dst in
    if not (CTree.mem remote.expected t.state.vval) then
      let update = CTree.make_update t.store t.state.vval remote.expected in
      Some update
    else None

  let get_cons_update ?(force = false) t =
    if force || t.change_flag then Some t.state else None

  let add_commands t ~node vals =
    assert (t.state.term = t.state.vterm) ;
    let ctree, hd =
      CTree.addv t.store ~node ~parent:t.state.vval ~term:t.state.term vals
    in
    t.state.vval <- hd ;
    t.store <- ctree ;
    t.change_flag <- true

  let create nodes other_nodes =
    let empty_tree = CTree.create nodes 0 in
    { state= init_state nodes
    ; store= empty_tree
    ; change_flag= false
    ; remotes=
        List.map other_nodes ~f:(fun i -> (i, {expected= empty_tree}))
        |> Map.of_alist_exn (module Int) }
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
  [@@deriving show {with_path= false}]

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

  let pp_fd_sm ppf {state} =
    let open Fmt in
    pf ppf "%a"
      (braces @@ list ~sep:comma @@ pair ~sep:(any ":@ ") int int)
      (Hashtbl.to_alist state)

  type t =
    { rep: Replication.t
    ; other_nodes: state Map.M(Int).t [@printer map_pp Fmt.int pp_state]
    ; failure_detector: fd_sm
    ; config: config [@opaque]
    ; command_queue: Value.t Queue.t [@opaque]
    ; stall_checker: StallChecker.t [@opaque]
    ; commit_log: Value.t Log.t }
  [@@deriving show {with_path= false}]

  let get_command idx t =
    if idx < 0 then Iter.empty else Log.get t.commit_log idx |> Iter.of_list

  let get_commit_index t = Log.highest t.commit_log

  module PP = struct
    let message_pp = pp_message

    let t_pp = pp

    let config_pp = pp_config
  end
end

module Make
    (Act : ActionSig with type t = Types.t and type message = Types.message) =
struct
  open Types
  module R = Replication

  let replication ?(force = false) (t : t) =
    List.iter t.config.other_replica_ids ~f:(fun dst ->
        let update = R.get_ctree_msg t.rep dst in
        Option.iter update ~f:(fun update ->
            let remote = Map.find_exn t.rep.remotes dst in
            remote.expected <- CTree.apply_update remote.expected update ;
            Act.send dst (GlobalTypes.CTreeUpdate update) ) ) ;
    Option.iter (R.get_cons_update ~force t.rep) ~f:(fun up ->
        Act.broadcast (ConsUpdate up) ) ;
    t.rep.change_flag <- false

  let is_live (t : t) nid =
    nid = t.config.node_id || Hashtbl.find_exn t.failure_detector.state nid > 0

  let stall_check (t : t) =
    StallChecker.check t.stall_checker ~pp:(fun ppf () ->
        let problem_idx = t.rep.state.commit_index in
        Fmt.pf ppf "Stalled on %d at %a@." t.config.node_id VectorClock.pp
          problem_idx )

  let acceptor_reply t src =
    let local = t.rep.state in
    let remote = Map.find_exn t.other_nodes src in
    let msg_term : term = remote.vterm in
    match msg_term >= local.term with
    (* overwrite *)
    | true when msg_term > local.vterm ->
        local.term <- msg_term ;
        local.vterm <- msg_term ;
        local.vval <- remote.vval ;
        t.rep.change_flag <- true
    (* extends local *)
    | true when msg_term = local.vterm -> (
      match CTree.compare t.rep.store local.vval remote.vval with
      | None ->
          (* conflict *)
          dtraceln "CONFLICT from %d" src ;
          dtraceln "local %a does not prefix of remote %a" VectorClock.pp
            local.vval VectorClock.pp remote.vval ;
          local.term <- msg_term + 1 ;
          t.rep.change_flag <- true
      | Some 0 ->
          (* equal *)
          ()
      | Some i when i > 0 ->
          (* local > remote *)
          ()
      | Some i ->
          (* local < remote *)
          assert (i < 0) ;
          local.vval <- remote.vval ;
          t.rep.change_flag <- true )
    | _ ->
        assert (msg_term < local.term || msg_term < local.vterm)

  let check_commit t =
    let vterm_votes =
      List.filter_map t.config.other_replica_ids ~f:(fun nid ->
          let s = Map.find_exn t.other_nodes nid in
          if s.vterm = t.rep.state.vterm then Some s else None )
    in
    let voted_path =
      CTree.path_between t.rep.store t.rep.state.commit_index t.rep.state.vval
      |> List.tl_exn
    in
    List.iter voted_path ~f:(fun vc ->
        let votes =
          List.count vterm_votes ~f:(fun s ->
              CTree.prefix t.rep.store vc s.vval )
        in
        if votes + 1 >= t.config.quorum_size then (
          t.rep.state.commit_index <- vc ;
          Option.iter (CTree.get_value t.rep.store vc) ~f:(Log.add t.commit_log)
          ) )

  let check_conflict_recovery t =
    let votes =
      t.config.replica_ids |> Iter.of_list
      |> Iter.map (function
           | nid when nid = t.config.node_id ->
               t.rep.state
           | nid ->
               Map.find_exn t.other_nodes nid )
    in
    let max_term =
      votes |> Iter.fold (fun acc (s : state) -> max acc s.term) (-1)
    in
    if max_term > t.rep.state.vterm then
      let num_max_term_votes =
        votes |> Iter.filter_count (fun (s : state) -> s.term = max_term)
      in
      let all_live_nodes_vote () =
        t.config.replica_ids
        |> List.for_all ~f:(fun nid ->
               let ( => ) a b = (not a) || b in
               is_live t nid
               =>
               let s =
                 if nid = t.config.node_id then t.rep.state
                 else Map.find_exn t.other_nodes nid
               in
               s.term = max_term )
      in
      if num_max_term_votes >= t.config.quorum_size && all_live_nodes_vote ()
      then (
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
        let o4_prefix =
          CTree.greatest_sufficiently_common_prefix t.rep.store
            (vterm_votes |> Iter.map (fun s -> s.vval) |> Iter.to_list)
            (t.config.quorum_size - missing_votes)
        in
        let max_length_o4_vote =
          vterm_votes
          |> Iter.filter (fun s -> CTree.prefix t.rep.store o4_prefix s.vval)
          |> Iter.max_exn ~lt:(fun a b ->
                 CTree.get_idx t.rep.store a.vval
                 < CTree.get_idx t.rep.store b.vval )
        in
        t.rep.state.term <- max_term ;
        t.rep.state.vterm <- max_term ;
        t.rep.state.vval <- max_length_o4_vote.vval ;
        (* ensure commands re-replicated *)
        let command_iter : Value.t Iter.t =
         fun f ->
          let f v = commands_applied () ; f v in
          Queue.iter t.command_queue ~f ;
          Queue.clear t.command_queue
        in
        R.add_commands t.rep ~node:t.config.node_id command_iter ;
        t.rep.change_flag <- true )

  let failure_detector_update t (event : message event) =
    match event with
    | Recv (_, src) ->
        Hashtbl.set t.failure_detector.state ~key:src ~data:t.config.fd_timeout
    | _ ->
        ()

  let handle_event t (event : message event) =
    run_ce := true ;
    run_ca := true ;
    failure_detector_update t event ;
    match event with
    | Tick ->
        Hashtbl.map_inplace t.failure_detector.state ~f:(fun v -> v - 1) ;
        check_conflict_recovery t ;
        replication t ~force:true ;
        stall_check t
    | Commands ci when not (t.rep.state.term = t.rep.state.vterm) ->
        ci
        |> Iter.map (fun v -> [v])
        |> Iter.iter (fun v ->
               commands_enqueued () ;
               Queue.enqueue t.command_queue v )
    | Commands ci ->
        R.add_commands t.rep ~node:t.config.node_id
          (ci |> Iter.map (fun v -> commands_applied () ; [v])) ;
        check_commit t ;
        replication t
    | Recv (m, src) -> (
      match m with
      | CTreeUpdate update ->
          R.recv_update t.rep src update
      | ConsUpdate state ->
          let remote = Map.find_exn t.other_nodes src in
          remote.term <- state.term ;
          remote.vterm <- state.vterm ;
          remote.vval <- state.vval ;
          remote.commit_index <- state.commit_index ;
          acceptor_reply t src ;
          check_commit t ;
          check_conflict_recovery t ;
          replication t )

  let advance t e =
    Act.run_side_effects
      (fun () -> Exn.handle_uncaught_and_exit (fun () -> handle_event t e))
      t

  let create (config : config) =
    let rep = R.create config.replica_ids config.other_replica_ids in
    let other_nodes =
      List.map config.other_replica_ids ~f:(fun i ->
          (i, init_state config.replica_ids) )
      |> Map.of_alist_exn (module Int)
    in
    let failure_detector =
      { state=
          List.map config.other_replica_ids ~f:(fun i -> (i, config.fd_timeout))
          |> Hashtbl.of_alist_exn (module Int) }
    in
    { rep
    ; other_nodes
    ; failure_detector
    ; config
    ; command_queue= Queue.create ()
    ; stall_checker= {ticks_since_commit= 2; prev_commit_idx= -1; tick_limit= 2}
    ; commit_log= Log.create [] }
end

module Impl = Make (ImperativeActions (Types))

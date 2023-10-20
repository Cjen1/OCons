open! Core
open Conspire_command_tree
module Log = Utils.SegmentLog
open Types

type config =
  { node_id: node_id
  ; replica_ids: node_id list
  ; other_replica_ids: node_id list
  ; replica_count: int
  ; quorum_size: int }
[@@deriving show {with_path= false}]

module type Value = sig
  type t [@@deriving compare, equal, hash, bin_io, sexp, show]

  val empty : t
end

module Make (Value : Value) = struct
  module CTree = CommandTree (Value)

  type state =
    { mutable vval: CTree.key
    ; mutable vterm: term
    ; mutable term: term
    ; mutable commit_index: CTree.key }
  [@@deriving show {with_path= false}, bin_io, equal]

  let init_state ctree =
    let open CTree in
    {term= 0; vterm= 0; vval= ctree.root; commit_index= ctree.root}

  let set_state t tar =
    t.vval <- tar.vval ;
    t.vterm <- tar.vterm ;
    t.term <- tar.term ;
    t.commit_index <- tar.commit_index

  module Rep = struct
    type remote_view =
      { mutable known_tree: CTree.t
      ; mutable expected_tree: CTree.t
      ; mutable expected_state: state }
    [@@deriving show {with_path= false}]

    type rep =
      { state: state
      ; mutable store: CTree.t
      ; remotes: remote_view Map.M(Int).t [@opaque] }
    [@@deriving show {with_path= false}]

    type success = {ctree: CTree.update option; cons: state option}
    [@@deriving show {with_path= false}, bin_io]

    type failure = {commit: CTree.key}
    [@@deriving show {with_path= false}, bin_io]

    type message =
      ((success, failure) Result.t
      [@printer Fmt.result ~ok:pp_success ~error:pp_failure] )
    [@@deriving show {with_path= false}, bin_io]

    let get_update rep dst =
      let remote = Map.find_exn rep.remotes dst in
      let ctree =
        if not (CTree.mem remote.expected_tree rep.state.vval) then
          let update =
            CTree.make_update rep.store rep.state.vval remote.expected_tree
          in
          Some update
        else None
      in
      let cons =
        if not ([%equal: state] rep.state remote.expected_state) then
          Some rep.state
        else None
      in
      {ctree; cons}

    let valid_update rep update =
      match update.CTree.extension with
      | [] ->
          false
      | (_, par, _) :: _ ->
          CTree.mem rep.store par

    let recv_update rep src update =
      let%map.Result new_tree = CTree.apply_update rep.store update in
      rep.store <- new_tree ;
      let remote = Map.find_exn rep.remotes src in
      remote.known_tree <-
        CTree.copy_update rep.store remote.known_tree update.new_head ;
      remote.expected_tree <-
        CTree.copy_update rep.store remote.expected_tree update.new_head

    let get_update_to_send rep dst =
      let update = get_update rep dst in
      let remote = Map.find_exn rep.remotes dst in
      Option.iter update.ctree ~f:(fun update ->
          remote.expected_tree <-
            CTree.apply_update remote.expected_tree update
            |> Result.ok |> Option.value_exn ) ;
      Option.iter update.cons ~f:(fun s ->
          set_state (Map.find_exn rep.remotes dst).expected_state s ) ;
      update

    let add_commands rep ~node vals =
      assert (rep.state.term = rep.state.vterm) ;
      let ctree, hd = CTree.addv rep.store ~node ~parent:rep.state.vval vals in
      rep.state.vval <- hd ;
      rep.store <- ctree

    let create other_nodes =
      let empty_tree = CTree.empty in
      { state= init_state empty_tree
      ; store= empty_tree
      ; remotes=
          List.map other_nodes ~f:(fun i ->
              ( i
              , { known_tree= empty_tree
                ; expected_tree= empty_tree
                ; expected_state= init_state empty_tree } ) )
          |> Map.of_alist_exn (module Int) }
  end

  type message = Rep.message [@@deriving show, bin_io]

  type t =
    { rep: Rep.rep
    ; other_nodes_state: state Map.M(Int).t [@printer Utils.pp_map Fmt.int pp_state]
    ; config: config [@opaque]
    ; commit_log: Value.t Log.t }
  [@@deriving show {with_path= false}]

  let acceptor_reply t src =
    let local = t.rep.state in
    let remote = Map.find_exn t.other_nodes_state src in
    let msg_term : term = remote.vterm in
    match msg_term >= local.term with
    (* overwrite *)
    | true when msg_term > local.vterm ->
        local.term <- msg_term ;
        local.vterm <- msg_term ;
        local.vval <- remote.vval
    (* extends local *)
    | true when msg_term = local.vterm -> (
        let res = CTree.compare_keys t.rep.store local.vval remote.vval in
        match res with
        | None ->
            (* conflict *)
            Utils.dtraceln "CONFLICT from %d" src ;
            Utils.dtraceln "local %a does not prefix of remote %a"
              (Fmt.option CTree.pp_parent_ref_node)
              (CTree.get t.rep.store local.vval)
              (Fmt.option CTree.pp_parent_ref_node)
              (CTree.get t.rep.store remote.vval) ;
            local.term <- msg_term + 1
        | Some EQ ->
            (* equal *)
            ()
        | Some GT ->
            (* local > remote *)
            ()
        | Some LT ->
            (* local < remote *)
            local.vval <- remote.vval )
    | _ ->
        assert (msg_term < local.term || msg_term < local.vterm)

  let check_commit t =
    let old_ci = t.rep.state.commit_index in
    let vterm_votes =
      List.filter_map t.config.replica_ids ~f:(fun nid ->
          if nid = t.config.node_id then Some t.rep.state.vval
          else
            let s = Map.find_exn t.other_nodes_state nid in
            if s.vterm = t.rep.state.vterm then Some s.vval else None )
    in
    let new_commit =
      CTree.greatest_sufficiently_common_prefix t.rep.store vterm_votes
        t.config.quorum_size
    in
    match new_commit with
    | None ->
        ()
    | Some ci ->
        (* can update since once committed, never overwritten *)
        t.rep.state.commit_index <- ci ;
        let cis = CTree.path_between t.rep.store old_ci ci in
        List.iter cis ~f:(fun (_, _, v) -> Log.add t.commit_log v)

  let check_conflict_recovery t =
    let votes =
      t.config.replica_ids |> Iter.of_list
      |> Iter.map (function
           | nid when nid = t.config.node_id ->
               t.rep.state
           | nid ->
               Map.find_exn t.other_nodes_state nid )
    in
    let max_term =
      votes |> Iter.fold (fun acc (s : state) -> max acc s.term) (-1)
    in
    if max_term > t.rep.state.vterm then
      let num_max_term_votes =
        votes |> Iter.filter_count (fun (s : state) -> s.term = max_term)
      in
      if num_max_term_votes >= t.config.quorum_size then (
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
          |> Iter.filter (fun s ->
                 Option.value_map o4_prefix ~default:true ~f:(fun o4_prefix ->
                     CTree.prefix t.rep.store o4_prefix s.vval ) )
          |> Iter.max_exn ~lt:(fun a b ->
                 CTree.get_idx t.rep.store a.vval
                 < CTree.get_idx t.rep.store b.vval )
        in
        t.rep.state.term <- max_term ;
        t.rep.state.vterm <- max_term ;
        t.rep.state.vval <- max_length_o4_vote.vval )

  let add_commands t (ci : Value.t Iter.t) =
    Rep.add_commands t.rep ~node:t.config.node_id ci ;
    check_commit t

  let acceptor_term_tick t term' =
    if t.rep.state.term < term' then
      t.rep.state.term <- term'

  let handle_steady_state t src (msg : Rep.success) =
    let option_bind o ~f = Option.value_map o ~default:(Ok ()) ~f in
    let%bind.Result () = option_bind msg.ctree ~f:(Rep.recv_update t.rep src) in
    let%bind.Result () =
      option_bind msg.cons ~f:(fun s ->
          CTree.mem t.rep.store s.vval
          |> Result.ok_if_true ~error:(`Msg "Invalid vval") )
    in
    let%bind.Result () =
      option_bind msg.cons ~f:(fun s ->
          CTree.mem t.rep.store s.commit_index
          |> Result.ok_if_true ~error:(`Msg "Invalid commit index") )
    in
    Option.iter msg.cons ~f:(fun new_state ->
        let remote = Map.find_exn t.other_nodes_state src in
        set_state remote new_state ;
        acceptor_reply t src ;
        check_commit t ;
        check_conflict_recovery t;
        acceptor_term_tick t new_state.term;
      ) ;
    Result.return ()

  let handle_message t src (msg : Rep.message) :
      (unit, [`MustAck | `MustNack]) result =
    match msg with
    | Ok msg ->
        handle_steady_state t src msg
        |> Result.map_error ~f:(function `Msg _ -> `MustNack)
    | Error _ ->
        (* reset expected state to commit index and initial state *)
        let remote = Map.find_exn t.rep.remotes src in
        (* commit guaranteed to exist locally otherwise rejected update :) *)
        remote.expected_tree <- remote.known_tree ;
        Error `MustAck

  let create (config : config) =
    let rep = Rep.create config.other_replica_ids in
    let other_nodes_state =
      List.map config.other_replica_ids ~f:(fun i -> (i, init_state rep.store))
      |> Map.of_alist_exn (module Int)
    in
    {rep; other_nodes_state; config; commit_log= Log.create Value.empty}
end

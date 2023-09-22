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
    { mutable vval: VectorClock.t
    ; mutable vterm: term
    ; mutable term: term
    ; mutable commit_index: VectorClock.t }
  [@@deriving show {with_path= false}, bin_io, equal]

  let init_state nodes =
    { term= 0
    ; vterm= 0
    ; vval= VectorClock.empty nodes 0
    ; commit_index= VectorClock.empty nodes 0 }

  module Rep = struct
    type remote_view =
      {mutable expected_tree: CTree.t [@opaque]; mutable expected_state: state}
    [@@deriving show {with_path= false}]

    type rep =
      { state: state
      ; mutable store: CTree.t
      ; remotes: remote_view Map.M(Int).t
            [@printer Conspire_command_tree.map_pp Fmt.int pp_remote_view] }
    [@@deriving show {with_path= false}]

    type message = {ctree: CTree.update option; cons: state option}
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

    let apply_ctree_update rep src update =
      rep.store <- CTree.apply_update rep.store update ;
      let remote = Map.find_exn rep.remotes src in
      remote.expected_tree <- CTree.apply_update remote.expected_tree update

    let get_update_to_send rep dst =
      let update = get_update rep dst in
      Option.iter update.ctree ~f:(apply_ctree_update rep dst) ;
      Option.iter update.cons ~f:(fun s ->
          (Map.find_exn rep.remotes dst).expected_state <- s ) ;
      update

    let add_commands rep ~node vals =
      assert (rep.state.term = rep.state.vterm) ;
      let ctree, hd =
        CTree.addv rep.store ~node ~parent:rep.state.vval ~term:rep.state.term
          vals
      in
      rep.state.vval <- hd ;
      rep.store <- ctree

    let create nodes other_nodes =
      let empty_tree = CTree.create nodes 0 in
      { state= init_state nodes
      ; store= empty_tree
      ; remotes=
          List.map other_nodes ~f:(fun i ->
              (i, {expected_tree= empty_tree; expected_state= init_state nodes}) )
          |> Map.of_alist_exn (module Int) }
  end

  type message = Rep.message [@@deriving show, bin_io]

  type t =
    { rep: Rep.rep
    ; other_nodes_state: state Map.M(Int).t [@printer map_pp Fmt.int pp_state]
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
      match CTree.compare t.rep.store local.vval remote.vval with
      | None ->
          (* conflict *)
          Utils.dtraceln "CONFLICT from %d" src ;
          Utils.dtraceln "local %a does not prefix of remote %a" VectorClock.pp
            local.vval VectorClock.pp remote.vval ;
          local.term <- msg_term + 1
      | Some 0 ->
          (* equal *)
          ()
      | Some i when i > 0 ->
          (* local > remote *)
          ()
      | Some i ->
          (* local < remote *)
          assert (i < 0) ;
          local.vval <- remote.vval )
    | _ ->
        assert (msg_term < local.term || msg_term < local.vterm)

  let check_commit t =
    let vterm_votes =
      List.filter_map t.config.other_replica_ids ~f:(fun nid ->
          let s = Map.find_exn t.other_nodes_state nid in
          if s.vterm = t.rep.state.vterm then Some s else None )
    in
    let voted_path =
      CTree.path_between t.rep.store t.rep.state.commit_index t.rep.state.vval
      |> List.tl_exn
    in
    let rec aux vp =
      match vp with
      | [] ->
          ()
      | vc :: vp ->
          let votes =
            List.count vterm_votes ~f:(fun s ->
                CTree.prefix t.rep.store vc s.vval )
          in
          if votes + 1 >= t.config.quorum_size then (
            t.rep.state.commit_index <- vc ;
            Option.iter
              (CTree.get_value t.rep.store vc)
              ~f:(Log.add t.commit_log) ;
            aux vp )
    in
    aux voted_path

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
          |> Iter.filter (fun s -> CTree.prefix t.rep.store o4_prefix s.vval)
          |> Iter.max_exn ~lt:(fun a b ->
                 CTree.get_idx t.rep.store a.vval
                 < CTree.get_idx t.rep.store b.vval )
        in
        t.rep.state.term <- max_term ;
        t.rep.state.vterm <- max_term ;
        t.rep.state.vval <- max_length_o4_vote.vval )

  let add_commands t (ci : Value.t Iter.t) =
    Rep.add_commands t.rep ~node:t.config.node_id ci;
    check_commit t

  let handle_message t src (msg : Rep.message) =
    Option.iter msg.ctree ~f:(Rep.apply_ctree_update t.rep src) ;
    Option.iter msg.cons ~f:(fun new_state ->
        let remote = Map.find_exn t.other_nodes_state src in
        remote.term <- new_state.term ;
        remote.vterm <- new_state.vterm ;
        remote.vval <- new_state.vval ;
        remote.commit_index <- new_state.commit_index ;
        acceptor_reply t src ;
        check_commit t ;
        check_conflict_recovery t )

  let create (config : config) =
    let rep = Rep.create config.replica_ids config.other_replica_ids in
    let other_nodes_state =
      List.map config.other_replica_ids ~f:(fun i ->
          (i, init_state config.replica_ids) )
      |> Map.of_alist_exn (module Int)
    in
    {rep; other_nodes_state; config; commit_log= Log.create Value.empty}
end

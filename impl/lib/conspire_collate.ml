(*open Core
  open Types
  open Utils
  open C.Types
  open Actions_f
  open A.O
  open Ocons_core.Consensus_intf
  module IdMap = Iter.Map.Make (Int)

  module Value = struct
    type t = command list [@@deriving sexp, hash, compare, bin_io]
  end

  type vote = {mutable vvalue: Value.t; mutable vterm: term; mutable term: term}
  [@@deriving compare, bin_io]

  module VoteImmediate : sig
    type t

    val none : t

    val some : vote -> t

    val is_none : t -> bool

    val unsafe_value : t -> vote

    module Optional_syntax :
      Optional_syntax.S with type t := t with type value := vote
  end = struct
    type t = vote

    let none = {vvalue= []; vterm= -1; term= -1}

    let some t = t

    let is_none = function {term= -1; _} -> true | _ -> false

    let unsafe_value = Fun.id

    module Optional_syntax :
      Optional_syntax.S with type t := t with type value := vote = struct
      module Optional_syntax = struct
        let is_none = is_none

        let unsafe_value = unsafe_value
      end
    end
  end

  module Message = struct
    type sync = {value: Value.t; term: term} [@@deriving compare, bin_io]

    type t = Sync of log_index * sync | SyncResp of log_index * vote | Heartbeat
    [@@deriving compare, bin_io]
  end

  module Types = struct
    type message = Message.t [@@deriving bin_io]

    type config =
      { node_id: node_id
      ; replica_ids: node_id list
      ; quorum_size: int
      ; fd_timeout: int
      ; max_outstanding: log_index }

    let make_config ~node_id ~replica_ids ~fd_timeout ?(max_outstanding = 8192) ()
        : config =
      let floor f = f |> Int.of_float in
      assert (List.mem replica_ids node_id ~equal:Int.equal) ;
      let cluster_size = List.length replica_ids |> Float.of_int in
      let quorum_size = floor (2. *. cluster_size /. 3.) + 1 in
      assert (3 * quorum_size > 2 * Float.to_int cluster_size) ;
      {node_id; replica_ids; quorum_size; fd_timeout; max_outstanding}

    type entry_state =
      { acc: vote
      ; mutable pterm: term
      ; mutable committed: bool
      ; mutable votes:
          (node_id, VoteImmediate.t) Hashtbl.t (* Every node state bur this one *)
      ; mutable max_term: term
      }

    type t =
      { log: entry_state SegmentLog.t
      ; commit_index: log_index
      ; config: config
      ; failure_detector: (node_id, int) Hashtbl.t }
  end

  let htbl_iter htbl f = Hashtbl.iter htbl ~f

  let htbl_iteri htbl f = Hashtbl.iteri htbl ~f:(fun ~key ~data -> f key data)

  module Make
      (Act : ActionSig with type t = Types.t and type message = Types.message) =
  struct
    include Types
    open Act
    open Message

    let get_msg_term = function
      | Sync (_, {term; _}) | SyncResp (_, {term; _}) ->
          term
      | Heartbeat ->
          assert false

    let get_msg_idx = function
      | Sync (idx, _) | SyncResp (idx, _) ->
          idx
      | Heartbeat ->
          assert false

    let is_live node_id =
      let ct = get_t () in
      if ct.config.node_id = node_id then true
      else Hashtbl.find_exn ct.failure_detector node_id > 0

    let is_local node_id =
      let ct = get_t () in
      ct.config.node_id = node_id

    let valid_quorum votes : bool = votes >= (get_t ()).config.quorum_size

    let get_vote le src =
      if is_local src then VoteImmediate.some le.acc
      else Hashtbl.find_exn le.votes src

    let get_value (votes : vote Iter.t) ~is_recovery =
      let ct = get_t () in
      let config = ct.config in
      let total_nodes = List.length config.replica_ids in
      let missing_votes = total_nodes - Iter.length votes in
      let max_vterm =
        votes |> IterLabels.fold ~init:(-1) ~f:(fun a (b : vote) -> max a b.vterm)
      in
      let votes_max_vterm =
        Iter.filter (fun (b : vote) -> b.vterm = max_vterm) votes
      in
      let vote_counts =
        let res = Hashtbl.create (module Value) in
        votes_max_vterm
        |> Iter.iter (fun vote ->
               if Hashtbl.mem res vote.vvalue then
                 Hashtbl.set res ~key:vote.vvalue
                   ~data:(Hashtbl.find_exn res vote.vvalue + 1)
               else Hashtbl.set res ~key:vote.vvalue ~data:1 ) ;
        res
      in
      let merge vcs : Value.t =
        let res = Hash_set.create (module Command) in
        Hashtbl.iter_keys vcs ~f:(List.iter ~f:(fun c -> Hash_set.add res c)) ;
        res |> Hash_set.to_list |> List.sort ~compare:Command.compare
      in
      let res = ref None in
      if is_recovery then (
      Hashtbl.iteri vote_counts ~f:(fun ~key:v ~data:c ->
          if valid_quorum (c + missing_votes) then (
            assert (Option.is_none !res) ;
            res := Some v ) )
      ;
      if (Option.is_none !res) then res := Some (merge vote_counts)
      ) else (
        Hashtbl.iteri vote_counts ~f:(fun ~key:v ~data:c ->
            if valid_quorum c then (
              assert (Option.is_none !res) ;
              res := Some v ) )
      );
      !res

    let vote_for le value term =
      le.acc.vvalue <- value ;
      le.acc.vterm <- term ;
      le.acc.term <- term

    let check_commit le =
      if not le.committed then (
          let votes =
            (get_t ()).config.replica_ids |> Iter.of_list
            |> Iter.map (get_vote le)
            |> Iter.filter (fun v -> not @@ VoteImmediate.is_none v)
            |> Iter.map VoteImmediate.unsafe_value
            |> Iter.filter (fun (v : vote) -> v.term = le.max_term)
          in
          match get_value votes ~is_recovery:false with
          | None -> ()
          | Some v ->
              (* TODO continue*)
      )

    let check_conflict_res idx =
      let ct = get_t () in
      let le = Log.get ct.log idx in
      match le with
      (* Already committed *)
      | {committed= true; _} ->
          ()
      (* No conflicts *)
      | _ when le.pterm = le.max_term ->
          ()
      | _ ->
          let voted vote max_term =
            match%optional.VoteImmediate vote with
            | None ->
                false
            | Some x ->
                x.term = max_term
          in
          let node_id_iter = ct.config.replica_ids |> Iter.of_list in
          let all_live_nodes_vote =
            let ( => ) a b = (not a) || b in
            node_id_iter
            |> Iter.for_all (fun src ->
                   is_live src => voted (get_vote le src) le.max_term )
          in
          let num_votes =
            node_id_iter
            |> Iter.filter_count (fun src -> voted (get_vote le src) le.max_term)
          in
          if all_live_nodes_vote && valid_quorum num_votes then (
            let votes =
              node_id_iter
              |> Iter.map (get_vote le)
              |> Iter.filter (fun v -> not @@ VoteImmediate.is_none v)
              |> Iter.map VoteImmediate.unsafe_value
            in
            let value = get_value votes in
            vote_for le value le.max_term ;
            le.pterm <- le.max_term ;
            Hashtbl.map_inplace le.votes ~f:(fun _ -> VoteImmediate.none) ;
            broadcast @@ Sync (idx, {term= le.pterm; value= le.acc.vvalue}) )

    let send_sync_resp dst idx conflict =
      let le = Log.get (get_t ()).log idx in
      let msg = SyncResp (idx, le.acc) in
      if conflict then broadcast msg else send dst msg

    let recv_sync_msg src msg =
      let idx = get_msg_idx msg in
      let ct = get_t () in
      let log = ct.log in
      if not @@ Log.mem log idx then Log.allocate log idx ;
      let le = Log.get log idx in
      let msg_term = get_msg_term msg in
      match (msg, comp Int.compare msg_term le.acc.term) with
      (* New ballot *)
      | Sync (_, {term; value}), GT ->
          le.acc.term <- term ;
          le.max_term <- max le.max_term term;
          le.acc.vterm <- term ;
          le.acc.vvalue <- value ;
          send_sync_resp src idx false
      (* New ballot after conflict *)
      | Sync (_, {term; value; _}), EQ when le.acc.vterm < term ->
          le.acc.vterm <- term ;
          le.max_term <- max le.max_term term;
          le.acc.vvalue <- value ;
          send_sync_resp src idx false
      | Sync (_, {value; term}), EQ ->
          let is_conflict =
            le.acc.vterm < term || [%compare.equal: Value.t] le.acc.vvalue value
          in
          if is_conflict then le.acc.term <- le.acc.term + 1 ;
          le.max_term <- max le.max_term term;
          send_sync_resp src idx is_conflict ;
          if is_conflict then check_conflict_res idx
      (* Nack for old ballot *)
      | Sync _, LT ->
          send_sync_resp src idx false
      | SyncResp _, _ | Heartbeat, _ ->
          assert false
  end
*)

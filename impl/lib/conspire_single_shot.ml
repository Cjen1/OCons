open Core
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

module Types = struct
  type value = Value.t [@@deriving bin_io]

  let compare_value = List.compare Types.Command.compare

  type config =
    { node_id: node_id
    ; replica_ids: node_id list
    ; quorum_size: int
    ; fd_timeout: int
    ; max_outstanding: log_index }
  [@@deriving accessors]

  let make_config ~node_id ~replica_ids ~fd_timeout ?(max_outstanding = 8192) ()
      : config =
    let floor f = f |> Int.of_float in
    assert (List.mem replica_ids node_id ~equal:Int.equal) ;
    let cluster_size = List.length replica_ids |> Float.of_int in
    let quorum_size = floor (2. *. cluster_size /. 3.) + 1 in
    assert (3 * quorum_size > 2 * Float.to_int cluster_size) ;
    {node_id; replica_ids; quorum_size; fd_timeout; max_outstanding}

  type sync_state = {value: value; term: term}
  [@@deriving accessors, compare, bin_io]

  type vote = {vvalue: value; vterm: term; term: term}
  [@@deriving accessors, compare, bin_io]

  type message =
    | Sync of log_index * sync_state
    | SyncResp of log_index * vote
    | Heartbeat
  [@@deriving accessors, compare, bin_io]

  type prop_sm =
    | Undecided of {term: term; value: value; votes: vote IdMap.t; sent: bool}
    | Committed of {value: value}
  [@@deriving accessors]

  type fd_sm = {state: (node_id, int) Hashtbl.t} [@@deriving accessors]

  type t =
    { rep_log: vote SegmentLog.t
    ; prop_log: prop_sm SegmentLog.t
    ; commit_index: log_index
    ; config: config
    ; failure_detector: fd_sm }
  [@@deriving accessors]

  let command_from_index idx =
    prop_log
    @> [%accessor A.getter (function s -> Log.get s idx)]
    @> [%accessor
         A.getter (function Committed {value} | Undecided {value; _} ->
             Iter.of_list value )]

  module PP = struct
    open Fmt

    let value_pp : value Fmt.t = brackets @@ list ~sep:comma Command.pp

    let config_pp : config Fmt.t =
      record
        [ field "node_id" (fun c -> c.node_id) int
        ; field "quorum_side" (fun c -> c.quorum_size) int
        ; field "fd_timeout" (fun c -> c.fd_timeout) int
        ; field "replica_ids"
            (fun c -> c.replica_ids)
            (brackets @@ list ~sep:comma int) ]

    let vote_pp =
      record
        [ field "term" (fun (m : vote) -> m.term) int
        ; field "vterm" (fun (m : vote) -> m.vterm) int
        ; field "vvalue" (fun (m : vote) -> m.vvalue) value_pp ]

    let message_pp : message Fmt.t =
     fun ppf v ->
      match v with
      | Sync (idx, m) ->
          pf ppf "Sync(%a)"
            (record
               [ field "idx" (fun _ -> idx) int
               ; field "term" (fun (m : sync_state) -> m.term) int
               ; field "value" (fun (m : sync_state) -> m.value) value_pp ] )
            m
      | SyncResp (idx, m) ->
          pf ppf "SyncResp(%d,%a)" idx vote_pp m
      | Heartbeat ->
          pf ppf "Heartbeat"

    let rep_log_entry_pp =
      braces
      @@ record
           [ field "term" (fun (le : vote) -> le.term) int
           ; field "vterm" (fun (le : vote) -> le.vterm) int
           ; field "vvalue" (fun (le : vote) -> le.vvalue) value_pp ]

    let prop_sm_pp ppf v =
      match v with
      | Committed {value} ->
          pf ppf "Committed(%a)"
            (record [field "value" (fun _ -> value) value_pp])
            ()
      | Undecided {term; value; votes; sent} ->
          pf ppf "Undecided(%a)"
            (record
               [ field "term" (fun _ -> term) int
               ; field "sent" (fun _ -> sent) bool
               ; field "value" (fun _ -> value) value_pp
               ; field "votes"
                   (fun _ -> IdMap.bindings votes)
                   ( brackets @@ list ~sep:semi @@ parens
                   @@ pair ~sep:(Fmt.any ":@ ") int vote_pp ) ] )
            ()

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
        ; field "rep_log"
            (fun t -> Log.iter t.rep_log |> Iter.to_list)
            (brackets @@ list rep_log_entry_pp)
        ; field "prop_log"
            (fun t -> Log.iter t.prop_log |> Iter.to_list)
            (brackets @@ list prop_sm_pp)
        ; field "fd" (fun t -> t.failure_detector) fd_sm_pp ]
  end
end

module Make
    (Act : ActionSig with type t = Types.t and type message = Types.message) =
struct
  include Types
  module PP = PP
  open Act

  let ex = ()

  let log_idx idx = [%accessor Accessor.getter (fun at -> Log.get at idx)]

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

  let send_sync_resp dst idx conflict =
    let le = Log.get (get_t ()).rep_log idx in
    let msg =
      SyncResp (idx, {term= le.term; vvalue= le.vvalue; vterm= le.vterm})
    in
    if conflict then broadcast msg else send dst msg

  let recv_sync_msg src msg =
    let idx = get_msg_idx msg in
    let ct = get_t () in
    let log = ct.rep_log in
    if not @@ Log.mem log idx then Log.allocate log idx ;
    let le = Log.get log idx in
    match (msg, comp Int.compare (get_msg_term msg) le.term) with
    (* New ballot *)
    | Sync (_, {term; value}), GT ->
        Log.set log idx {term; vvalue= value; vterm= term} ;
        send_sync_resp src idx false
    (* New ballot after conflict *)
    | Sync (_, {term; value; _}), EQ when le.vterm < term ->
        Log.set log idx {term= le.term; vvalue= value; vterm= term} ;
        send_sync_resp src idx false
    (* Revote for existing ballot *)
    | Sync (_, {term; value; _}), EQ
      when le.vterm = term && [%compare.equal: value] value le.vvalue ->
        send_sync_resp src idx false
    (* Conflict for existing ballot (given above) *)
    | Sync _, EQ ->
        Log.set log idx {le with term= le.term + 1} ;
        send_sync_resp src idx true
    (* Nack for old ballot *)
    | Sync _, LT ->
        send_sync_resp src idx false
    | SyncResp _, _ | Heartbeat, _ ->
        assert false

  let handle_msg src (msg : vote) sm =
    let add_vote (votes : vote IdMap.t) (vote : vote) =
      IdMap.update src
        (function
          | None ->
              Some vote
          | Some m' when m'.term > vote.term ->
              Some m'
          | Some _ ->
              Some vote )
        votes
    in
    match sm with
    | Committed _ ->
        sm
    | Undecided s -> (
      match comp Int.compare msg.term s.term with
      (* Vote for current value *)
      | EQ ->
          let votes = add_vote s.votes msg in
          Undecided {s with votes}
      (* Conflict vote for next term*)
      | GT ->
          let vote =
            IdMap.find_opt src s.votes
            |> Option.value_map ~default:msg ~f:(fun (old_msg : vote) ->
                   if old_msg.term < msg.term then msg else old_msg )
          in
          let votes = add_vote s.votes vote in
          Undecided {s with votes}
      (* Conflict for for much higher term => missing msgs *)
      (* Old msg => just ignore *)
      | LT ->
          sm )

  let valid_quorum votes : bool = votes >= (get_t ()).config.quorum_size

  module CommandMap = Iter.Map.Make (struct
    type t = Command.t

    let compare = Command.compare
  end)

  (*
  let merge (votes : (node_id * vote) Iter.t) : value =
    votes
    |> IterLabels.fold ~init:CommandMap.empty
         ~f:(fun a (_, (b : vote)) ->
           b.vvalue |> Iter.of_list
           |> IterLabels.fold ~init:a ~f:(fun a c -> CommandMap.add c () a) )
    |> CommandMap.keys |> Iter.to_list
    *)

  module ValueMap = Iter.Map.Make (struct
    type t = value

    let compare = compare_value
  end)

  let get_value (votes : vote Iter.t) =
    let ct = get_t () in
    let total_nodes = List.length ct.config.replica_ids in
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
    Hashtbl.iteri vote_counts ~f:(fun ~key:v ~data:c ->
        if valid_quorum (c + missing_votes) then (
          assert (Option.is_none !res) ;
          res := Some v ) ) ;
    match !res with Some v -> v | None -> merge vote_counts

  let check_commit sm =
    match sm with
    | Committed _ ->
        sm
    | Undecided s ->
        let voting_replicas =
          IdMap.to_iter s.votes
          |> Iter.filter (fun ((_, v) : node_id * vote) ->
                 [%compare.equal: value] v.vvalue s.value && v.vterm = s.term )
          |> Iter.map fst
        in
        if valid_quorum (Iter.length voting_replicas) then
          Committed {value= s.value}
        else sm

  let check_conflict sm =
    let ct = get_t () in
    match sm with
    | Committed _ ->
        sm
    | Undecided s ->
        let max_term =
          IdMap.to_iter s.votes
          |> IterLabels.fold ~init:(-1) ~f:(fun acc ((_, s) : node_id * vote) ->
                 max acc s.term )
        in
        let votes =
          IdMap.to_iter s.votes
          |> Iter.filter (fun ((_, v) : node_id * vote) -> v.term = max_term)
        in
        let all_live_nodes_vote =
          let ( => ) a b = (not a) || b in
          ct.config.replica_ids |> Iter.of_list
          |> Iter.for_all (fun src ->
                 let is_live =
                   Hashtbl.find_exn ct.failure_detector.state src > 0
                 in
                 let is_vote =
                   match IdMap.find_opt src s.votes with
                   | Some v ->
                       v.term = max_term
                   | None ->
                       false
                 in
                 is_live => is_vote )
        in
        if all_live_nodes_vote && valid_quorum (Iter.length votes) then
          let value = votes |> Iter.map snd |> get_value in
          Undecided {term= max_term; value; votes= IdMap.empty; sent= false}
        else sm

  let check_send idx sm =
    match sm with
    | Committed _ | Undecided {sent= true; _} ->
        sm
    | Undecided s ->
        broadcast @@ Sync (idx, {term= s.term; value= s.value}) ;
        Undecided {s with sent= true}

  let update_commit_index () =
    let ct = get_t () in
    let is_commit = function Committed _ -> true | _ -> false in
    let rec highest_commit ct idx =
      if Log.mem ct.prop_log idx && Log.get ct.prop_log idx |> is_commit then
        highest_commit ct (idx + 1)
      else idx - 1
    in
    set_t {ct with commit_index= highest_commit ct (ct.commit_index + 1)}

  let failure_detector_update (event : message event) =
    match event with
    | Recv (_, src) ->
        let ct = get_t () in
        Hashtbl.set ct.failure_detector.state ~key:src
          ~data:ct.config.fd_timeout
    | _ ->
        ()

  let handle_event (event : message event) =
    failure_detector_update event ;
    let ct = get_t () in
    match event with
    | Tick ->
        Hashtbl.map_inplace ct.failure_detector.state ~f:(fun v -> v - 1) ;
        Iter.int_range ~start:(ct.commit_index + 1)
          ~stop:(Log.highest ct.prop_log)
        |> Iter.iter (fun idx ->
               Log.map ct.prop_log idx (fun sm ->
                   sm |> check_conflict |> check_send idx ) ) ;
        broadcast Heartbeat
    | Recv (Heartbeat, _) ->
        ()
    | Recv ((Sync _ as m), src) ->
        recv_sync_msg src m
    | Recv ((SyncResp (idx,m)), src) ->
        let log = ct.prop_log in
        Log.allocate log idx ;
        Log.map log idx (fun sm ->
            sm |> handle_msg src m |> check_commit |> check_conflict
            |> check_send idx )
    | Commands ci ->
        let start = Log.highest ct.prop_log + 1 in
        ci
        |> Iter.iter (fun c ->
               Log.add ct.prop_log
               @@ Undecided
                    {term= 0; value= [c]; votes= IdMap.empty; sent= false} ) ;
        let stop = Log.highest ct.prop_log in
        Iter.int_range ~start ~stop
        |> Iter.iter
           @@ fun idx ->
           Log.map ct.prop_log idx (fun sm ->
               sm |> check_commit |> check_send idx )

  let advance_raw (event : message event) =
    handle_event event ; update_commit_index ()

  let do_until_const t e =
    let run t e =
      let t, acts = run_side_effects (fun () -> advance_raw e) t in
      let acts_iter = Iter.of_list acts in
      let local_msgs =
        acts_iter
        |> Iter.filter_map (function
             | Broadcast m ->
                 Some m
             | Send (dst, m) when dst = t.config.node_id ->
                 Some m
             | _ ->
                 None )
        |> Iter.map (fun m -> Recv (m, t.config.node_id))
      in
      let non_local_msgs =
        acts_iter
        |> Iter.filter (function
             | Send (dst, _) when dst = t.config.node_id ->
                 false
             | _ ->
                 true )
      in
      (t, local_msgs, non_local_msgs)
    in
    let rec aux t eq acts =
      match Core.Fqueue.dequeue eq with
      | None ->
          (t, acts)
      | Some (e, eq) ->
          let t, l, nl = run t e in
          let eq = Iter.fold Core.Fqueue.enqueue eq l in
          aux t eq (Iter.append acts nl)
    in
    aux t (Core.Fqueue.singleton e) Iter.empty

  let advance t e = do_until_const t e

  let create config =
    { config
    ; commit_index= -1
    ; rep_log= SegmentLog.create {term= 0; vvalue= []; vterm= -1}
    ; prop_log=
        SegmentLog.create
          (Undecided {term= 0; value= []; votes= IdMap.empty; sent= false})
    ; failure_detector=
        { state=
            Hashtbl.of_alist_exn
              (module Int)
              ( config.replica_ids
              |> List.map ~f:(fun id -> (id, config.fd_timeout)) ) } }
end

module Impl = Make (ImperativeActions (Types))

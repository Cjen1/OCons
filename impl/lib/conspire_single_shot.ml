open Types
open Utils
open C.Types
open Actions_f
open A.O
open Ocons_core.Consensus_intf
module IdMap = Iter.Map.Make (Int)

module Types = struct
  type value = command list

  let compare_value = List.compare Command.compare

  type config = {node_id: node_id; replica_ids: node_id list; quorum_size: int}
  [@@deriving accessors]

  type sync_state = {idx: log_index; value: value; term: term}
  [@@deriving accessors, compare]

  type sync_resp_state = {idx: log_index; vvalue: value; vterm: term; term: term}
  [@@deriving accessors, compare]

  type message = Sync of sync_state | SyncResp of sync_resp_state
  [@@deriving accessors, compare]

  type rep_log_entry =
    {mutable term: term; mutable vvalue: value; mutable vterm: term}
  [@@deriving accessors]

  type prop_sm =
    | Undecided of
        {term: term; value: value; votes: sync_resp_state IdMap.t; sent: bool}
    | Committed of {value: value}
  [@@deriving accessors]

  type t =
    { rep_log: rep_log_entry SegmentLog.t
    ; prop_log: prop_sm SegmentLog.t
    ; config: config }
  [@@deriving accessors]

  let command_from_index idx =
    prop_log @> [%accessor A.getter (function s -> Log.get s idx)] @> [%accessor A.getter (function Committed {value} | Undecided {value; _} -> value)]

  module PP = struct
    open Fmt

    let value_pp : value Fmt.t = list ~sep:comma Command.pp

    let config_pp : config Fmt.t =
      record
        [ field "node_id" (fun c -> c.node_id) int
        ; field "quorum_side" (fun c -> c.quorum_size) int
        ; field "replica_ids"
            (fun c -> c.replica_ids)
            (braces @@ list ~sep:comma int) ]

    let sync_resp_state_pp =
      record
        [ field "idx" (fun (m : sync_resp_state) -> m.idx) int
        ; field "term" (fun (m : sync_resp_state) -> m.term) int
        ; field "vterm" (fun (m : sync_resp_state) -> m.vterm) int
        ; field "vvalue" (fun (m : sync_resp_state) -> m.vvalue) value_pp ]

    let message_pp : message Fmt.t =
     fun ppf v ->
      match v with
      | Sync m ->
          pf ppf "Sync(%a)"
            (record
               [ field "idx" (fun (m : sync_state) -> m.idx) int
               ; field "term" (fun (m : sync_state) -> m.term) int
               ; field "value" (fun (m : sync_state) -> m.value) value_pp ] )
            m
      | SyncResp m ->
          pf ppf "Sync(%a)" sync_resp_state_pp m

    let rep_log_entry_pp =
      record
        [ field "term" (fun (le : rep_log_entry) -> le.term) int
        ; field "vterm" (fun (le : rep_log_entry) -> le.vterm) int
        ; field "vvalue" (fun (le : rep_log_entry) -> le.vvalue) value_pp ]

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
                   (list ~sep:semi @@ parens @@ pair int sync_resp_state_pp) ] )
            ()

    let t_pp =
      record
        [ field "config" (fun t -> t.config) config_pp
        ; field "rep_log"
            (fun t -> Log.iter t.rep_log |> Iter.to_list)
            (list rep_log_entry_pp)
        ; field "prop_log"
            (fun t -> Log.iter t.prop_log |> Iter.to_list)
            (list prop_sm_pp) ]
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

  let get_msg_term = function Sync {term; _} | SyncResp {term; _} -> term

  let get_msg_idx = function Sync {idx; _} | SyncResp {idx; _} -> idx

  let send_sync_resp dst idx =
    let le = ex.@(t @> rep_log @> log_idx idx) in
    send dst @@ SyncResp {idx; term= le.term; vvalue= le.vvalue; vterm= le.vterm}

  let recv_sync_msg src msg =
    let idx = get_msg_idx msg in
    let le = ex.@(t @> rep_log @> log_idx idx) in
    match (msg, comp Int.compare (get_msg_term msg) le.term) with
    (* New ballot *)
    | Sync {idx; term; value}, GT ->
        le.term <- term ;
        le.vvalue <- value ;
        le.vterm <- term ;
        send_sync_resp src idx
    (* New ballot after conflict *)
    | Sync {term; value; _}, EQ when le.vterm < term ->
        le.vvalue <- value ;
        le.vterm <- term ;
        send_sync_resp src idx
    (* Revote for existing ballot *)
    | Sync {term; value; _}, EQ
      when le.vterm = term && [%compare.equal: value] value le.vvalue ->
        send_sync_resp src idx
    (* Conflict for existing ballot (given above) *)
    | Sync _, EQ ->
        le.term <- le.term + 1 ;
        send_sync_resp src idx
    (* Nack for old ballot *)
    | Sync _, LT ->
        send_sync_resp src idx
    | SyncResp _, _ ->
        assert false

  let handle_msg src msg sm =
    match sm with
    | Committed _ ->
        sm
    | Undecided s -> (
      match (msg, comp Int.compare (get_msg_term msg) s.term) with
      (* Vote for current value *)
      | SyncResp msg, EQ ->
          let votes' = IdMap.add src msg s.votes in
          Undecided {s with votes= votes'}
      (* Conflict vote for next term*)
      | SyncResp msg, GT ->
          let vote =
            IdMap.find_opt src s.votes
            |> Option.fold ~none:msg ~some:(fun (old_msg : sync_resp_state) ->
                   if old_msg.term < msg.term then msg else old_msg )
          in
          let votes' = IdMap.add src vote s.votes in
          Undecided {s with votes= votes'}
      (* Conflict for for much higher term => missing msgs *)
      (* Old msg => just ignore *)
      | SyncResp _, LT ->
          sm
      | Sync _, _ ->
          assert false )

  (* TODO use failure detectors to ensure majority *)
  let valid_quorum votes : bool =
    Iter.length votes >= ex.@(t @> config @> quorum_size)

  module CommandMap = Iter.Map.Make (struct
    type t = Command.t

    let compare = Command.compare
  end)

  let merge (votes : (node_id * sync_resp_state) Iter.t) : value =
    votes
    |> IterLabels.fold ~init:CommandMap.empty
         ~f:(fun a (_, (b : sync_resp_state)) ->
           b.vvalue |> Iter.of_list
           |> IterLabels.fold ~init:a ~f:(fun a c -> CommandMap.add c () a) )
    |> CommandMap.keys |> Iter.to_list

  module ValueMap = Iter.Map.Make (struct
    type t = value

    let compare = compare_value
  end)

  let get_value (votes : (node_id * sync_resp_state) Iter.t) =
    let total_nodes = List.length ex.@(t @> config @> replica_ids) in
    let missing_votes = total_nodes - ex.@(t @> config @> quorum_size) in
    let max_vterm =
      votes
      |> IterLabels.fold ~init:(-1) ~f:(fun a (_, (b : sync_resp_state)) ->
             max a b.vterm )
    in
    let votes_max_vterm =
      Iter.filter (fun (_, (b : sync_resp_state)) -> b.vterm = max_vterm) votes
    in
    let vote_counts =
      votes_max_vterm
      |> IterLabels.fold ~init:ValueMap.empty
           ~f:(fun a (_, (vote : sync_resp_state)) ->
             ValueMap.update vote.vvalue
               (function None -> Some 1 | Some v -> Some (v + 1))
               a )
    in
    let o4_value =
      ValueMap.to_iter vote_counts
      |> IterLabels.fold ~init:None ~f:(fun s (v, c) ->
             match s with
             | Some v ->
                 Some v
             | None when c + missing_votes >= ex.@(t @> config @> quorum_size)
               ->
                 Some v
             | None ->
                 None )
    in
    match o4_value with Some v -> v | None -> merge votes

  let check_commit sm =
    match sm with
    | Committed _ ->
        sm
    | Undecided s ->
        let voting_replicas =
          IdMap.to_iter s.votes
          |> Iter.filter (fun ((_, v) : node_id * sync_resp_state) ->
                 v.vvalue = s.value && v.vterm = s.term )
          |> Iter.map fst
        in
        if valid_quorum voting_replicas then Committed {value= s.value} else sm

  let check_conflict sm =
    match sm with
    | Committed _ ->
        sm
    | Undecided s ->
        let max_term =
          IdMap.to_iter s.votes
          |> IterLabels.fold ~init:(-1)
               ~f:(fun acc ((_, s) : node_id * sync_resp_state) ->
                 max acc s.term )
        in
        let votes =
          IdMap.to_iter s.votes
          |> Iter.filter (fun ((_, v) : node_id * sync_resp_state) ->
                 v.term = max_term )
        in
        if valid_quorum (Iter.map fst votes) then
          let value = get_value votes in
          Undecided {term= max_term; value; votes= IdMap.empty; sent= false}
        else sm

  let check_send idx sm =
    match sm with
    | Committed _ | Undecided {sent= true; _} ->
        sm
    | Undecided s ->
        broadcast @@ Sync {idx; term= s.term; value= s.value} ;
        sm

  let advance_prop_sm idx src msg sm =
    sm |> handle_msg src msg |> check_commit |> check_conflict |> check_send idx

  let advance_raw (event : message event) =
    match event with
    | Tick ->
        ()
    | Recv ((Sync _ as m), src) ->
        recv_sync_msg src m
    | Recv ((SyncResp _ as m), src) ->
        let idx = get_msg_idx m in
        Log.map
          ex.@(t @> prop_log)
          idx
          (fun sm ->
            sm |> handle_msg src m |> check_commit |> check_conflict
            |> check_send idx )
    | Commands ci ->
        let start = Log.highest ex.@(t @> prop_log) + 1 in
        ci
        |> Iter.iter (fun c ->
               Log.add ex.@(t @> prop_log)
               @@ Undecided
                    {term= 0; value= [c]; votes= IdMap.empty; sent= false} ) ;
        let stop = Log.highest ex.@(t @> prop_log) in
        Iter.int_range ~start ~stop
        |> Iter.iter
           @@ fun idx ->
           Log.map
             ex.@(t @> prop_log)
             idx
             (fun sm -> sm |> check_commit |> check_conflict |> check_send idx)

  let rec do_until_const t e acc_acts =
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
    match () with
    | () when Iter.is_empty local_msgs ->
        (t, Iter.append acc_acts acts_iter)
    | _ ->
        let other_acts =
          acts_iter
          |> Iter.filter (function
               | Send (dst, _) when dst = t.config.node_id ->
                   false
               | _ ->
                   true )
        in
        IterLabels.fold local_msgs
          ~init:(t, Iter.append other_acts acc_acts)
          ~f:(fun (t, acc) e -> do_until_const t e acc)

  let advance t e = do_until_const t e Iter.empty
end

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

  type sync_resp_state = {idx: log_index; vvalue: value; vterm: term; term: term}
  [@@deriving accessors, compare]

  type message =
    | Sync of {idx: log_index; value: value; term: term}
    | SyncResp of sync_resp_state
  [@@deriving accessors, compare]

  type rep_log_entry =
    {mutable term: term; mutable vvalue: value; mutable vterm: term}
  [@@deriving accessors]

  type prop_sm =
    | Undecided of
        {term: term; value: value; votes: sync_resp_state IdMap.t; sent: bool}
    | Committed of {value: value}
  [@@deriving accessors]

  let get_msg_term = function Sync {term; _} | SyncResp {term; _} -> term

  let get_msg_idx = function Sync {idx; _} | SyncResp {idx; _} -> idx

  type t = {rep_log: rep_log_entry SegmentLog.t; prop_log: prop_sm SegmentLog.t}
  [@@deriving accessors]
end

type comp = GT | EQ | LT

let comp cmp a b =
  let r = cmp a b in
  if r < 0 then LT else if r > 0 then GT else EQ

module Make
    (Act : ActionSig with type t = Types.t and type message = Types.message) =
struct
  include Types
  open Act

  let ex = ()

  let log_idx idx = [%accessor Accessor.getter (fun at -> Log.get at idx)]

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

  let get_quorum _ = assert false

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

  let valid_quorum _ : bool = assert false

  let get_value _votes = assert false

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

end

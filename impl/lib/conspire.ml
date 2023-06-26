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

  type hash = int

  let compare_hash = Int.compare

  let hash = Hashtbl.hash

  let prepare_hash (prev_hist : hash) (value : value) =
    let v' =
      value |> Iter.of_list
      |> Iter.sort_uniq ~cmp:Command.compare
      |> Iter.to_list
    in
    (v', hash (prev_hist, v'))

  type log_entry = {value: value; hist: hash} [@@deriving compare]

  type segment = {prev_hist: hash; start: log_index; entries: log_entry list}
  [@@deriving accessors]

  type ballot = {segment: segment; term: term} [@@deriving accessors]

  type message =
    | Sync of ballot
    | SyncResp of
        { term: term
        ; voted_term: term
        ; voted_log:
            ( log_index
            , [ `Conflict of segment
              | `MissingStartMaxRep of log_index
              | `MissingStartCommit of log_index ] )
            result
              (* If success shows point up to correct replication, otherwise non-matching segment *)
        }
  [@@deriving accessors]

  type other_nodes_state =
    { match_commit: log_index (* On and committed *)
    ; matching: log_index (* On other node*)
    ; sent: log_index (* Just sent *)
    ; conflicted: segment option
    ; current_term: term }
  [@@deriving accessors]

  type proposer_state =
    { local_log: log_entry SegmentLog.t
    ; local_term: term
    ; commit_index: log_index
    ; other_nodes: other_nodes_state IdMap.t }
  [@@deriving accessors]

  type replica_state =
    {max_vterm: term; max_vlog: log_entry Log.t; max_term: term}
  [@@deriving accessors]

  type fd_state = {exp_timeout: int}

  type t = {rep: replica_state; pro: proposer_state; fd: fd_state}
  [@@deriving accessors]
end

module Make
    (Act : ActionSig with type t = Types.t and type message = Types.message) =
struct
  include Types
  open Act

  let ex = ()

  (* Proposer functions *)

  let other_node_state i =
    t @> pro @> other_nodes
    @> [%accessor
         A.field
           ~get:(function s -> IdMap.find i s)
           ~set:(fun s v -> IdMap.add i v s)]

  let is_alive _ = assert false

  (* Replica functions *)

  let zip_from ~start i =
    i |> Iter.zip_i |> Iter.map (fun (i, v) -> (i + start, v))

  let fold_short_circuit :
         'a
      -> (('a, 'b) result -> 'c -> ('a, 'b) result)
      -> 'c Iter.t
      -> ('a, 'b) result =
   fun a f i ->
    let open struct
      exception Exit
    end in
    let loc = ref @@ Ok a in
    ( try
        i (fun v ->
            loc := f !loc v ;
            if Result.is_error !loc then raise Exit )
      with
    | Exit ->
        ()
    | e ->
        raise e ) ;
    !loc

  let get_hist i log = if i < 0 then 0 else (Log.get log i).hist

  let make_seg start log =
    let entries = Log.iter log ~lo:start |> Iter.to_list in
    {start; prev_hist= get_hist (start - 1) log; entries}

  let recv_sync src msg =
    let check_match ballot =
      assert (
        get_hist (ballot.segment.start - 1) ex.@(t @> rep @> max_vlog)
        = ballot.segment.prev_hist )
    in
    match msg with
    | SyncResp _ ->
        assert false
    | Sync ballot when ballot.term = ex.@(t @> rep @> max_vterm) -> (
        check_match ballot ;
        (* Guaranteed that start - 1 matches since reliable inord delivery and
           conflict would increment term, dropping all delayed or otherwise conflicting msgs *)
        let conflict_check_result =
          ballot.segment.entries |> Iter.of_list
          |> zip_from ~start:ballot.segment.start
          |> fold_short_circuit ballot.segment.start (fun (i, v) _ ->
                 let local = Log.get ex.@(t @> rep @> max_vlog) i in
                 let matches = List.equal Command.equal local.value v.value in
                 if matches then Ok i else Error i )
        in
        match conflict_check_result with
        | Ok i ->
            send src
              (SyncResp
                 {term= ballot.term; voted_term= ballot.term; voted_log= Ok i}
              )
        | Error i ->
            ex.@(t @> rep @> max_term) <- ballot.term + 1 ;
            broadcast
              (SyncResp
                 { term= ballot.term + 1
                 ; voted_term= ballot.term
                 ; voted_log=
                     Error (`Conflict (make_seg i ex.@(t @> rep @> max_vlog)))
                 } ) )
    | Sync ballot when ballot.term > ex.@(t @> rep @> max_vterm) ->
        check_match ballot ;
        ex.@(t @> rep @> max_vterm) <- ballot.term ;
        Log.cut_after ex.@(t @> rep @> max_vlog) ballot.segment.start ;
        ballot.segment.entries |> Iter.of_list
        |> zip_from ~start:ballot.segment.start
        |> Iter.iter (fun (i, v) -> Log.set ex.@(t @> rep @> max_vlog) i v) ;
        let max_entry =
          ballot.segment.entries |> Iter.of_list
          |> zip_from ~start:ballot.segment.start
          |> Iter.fold (fun a (i, _) -> max i a) (-1)
        in
        send src
          (SyncResp
             { term= ballot.term
             ; voted_term= ballot.term
             ; voted_log= Ok max_entry } )
    | _ ->
        assert false

  let recv_sync_resp src msg =
    match msg with
    | SyncResp ({voted_log= Ok i; _} as s) ->
        (* If recv ack => same term as this
           => must extend local log (only non-append local log on term increment)
        *)
        ex.@(other_node_state src @> matching) <- i ;
        ex.@(other_node_state src @> current_term) <- s.term
    | SyncResp ({voted_log= Error (`Conflict seg); _} as s) ->
        ex.@(other_node_state src @> conflicted) <- Some seg ;
        ex.@(other_node_state src @> current_term) <- s.term
    | SyncResp {voted_log= Error _; _} | Sync _ ->
        assert false

  let is_quorum _votes = true

  let can_be_quorum _votes _voting_nodes = false

  let get_ballot src start =
    let matching_seg =
      Log.to_seqi
        ex.@(t @> pro @> local_log)
        ~lo:start
        ~hi:ex.@(other_node_state src @> matching)
    in
    let conflicted_seg =
      match ex.@(other_node_state src).conflicted with
      | None ->
          Seq.empty
      | Some seg ->
          seg.entries
          |> List.mapi (fun i v -> (i + seg.start, v))
          |> List.to_seq
    in
    let entries = Seq.append matching_seg conflicted_seg in
    let all_entries_valid =
      let res, _ =
        Seq.fold_left
          (fun (sum, prev) (i, _) ->
            match prev with
            | None ->
                (sum, Some i)
            | Some p ->
                (sum && p + 1 = i, Some i) )
          (true, None) entries
      in
      res
    in
    assert all_entries_valid ;
    let term = ex.@(other_node_state src @> current_term) in
    (term, entries)

  module LogEntryMap = Iter.Map.Make (struct
    type t = log_entry

    let compare = compare_log_entry
  end)

  let o4 (idx_votes : (node_id * log_entry) list) voting_nodes =
    let per_le_votes =
      List.fold_left
        (fun votes (src, le) ->
          votes
          |> LogEntryMap.update le
             @@ function None -> Some [src] | Some vs -> Some (src :: vs) )
        LogEntryMap.empty idx_votes
    in
    per_le_votes |> LogEntryMap.to_iter
    |> fold_short_circuit () (fun _ (le, votes) ->
           if can_be_quorum votes voting_nodes then Error le else Ok () )
    |> function Ok () -> None | Error cid -> Some cid

  let update_propose_votes () =
    let max_term =
      ex.@(t @> pro @> other_nodes)
      |> IdMap.to_iter
      |> Iter.map (fun (_, {current_term; _}) -> current_term)
      |> Iter.fold max (-1)
    in
    (* TODO ensure that once triggered, we do eventually get sufficient votes => recover msg? *)
    (* If there is a vote for a higher term and sufficient votes, then update each local state *)
    if ex.@(t @> pro @> local_term) < max_term then
      let voting_nodes =
        ex.@(t @> pro @> other_nodes)
        |> IdMap.to_iter
        |> Iter.filter_map (fun (i, {current_term; _}) ->
               if current_term = max_term then Some i else None )
      in
      if is_quorum voting_nodes then
        let max_voted_term =
          ex.@(t @> pro @> other_nodes)
          |> IdMap.to_iter
          |> Iter.fold (fun mt (_, s) -> max s.current_term mt) (-1)
        in
        let start = ex.@(t @> pro @> commit_index) + 1 in
        let max_vterm_ballots : (node_id * log_entry) Seq.t list =
          ex.@(t @> pro @> other_nodes)
          |> IdMap.to_iter
          |> Iter.filter_map (fun (i, s) ->
                 if s.current_term = max_voted_term then
                   Some (i, get_ballot i start |> snd |> Seq.map snd)
                 else None )
          |> Iter.map (fun (src, vs) -> Seq.map (fun v -> (src, v)) vs)
          |> Iter.to_list
        in
        let prev_hist = get_hist (start - 1) ex.@(t @> pro @> local_log) in
        let per_log_entry_seq : (node_id * log_entry) list Seq.t =
          let rec combine ls () =
            match ls with
            | [] ->
                Seq.Nil
            | seqs ->
                let p = List.filter_map (fun s -> Seq.uncons s) seqs in
                let vs = List.map fst p in
                let seqs = List.map snd p in
                Cons (vs, combine seqs)
          in
          combine max_vterm_ballots
        in
        let combined_log_entries =
          let stateful_map init f s =
            let rec aux a s () =
              match s () with
              | Seq.Nil ->
                  Seq.Nil
              | Cons (x, next) ->
                  let a, v = f a x in
                  Cons (v, aux a next)
            in
            aux init s
          in
          per_log_entry_seq
          |> stateful_map (true, prev_hist) (fun (o4holds, prev_hist) votes ->
                 match
                   (o4holds, if o4holds then o4 votes voting_nodes else None)
                 with
                 | true, Some v ->
                     ((true, v.hist), v)
                 | _ ->
                     let value =
                       votes |> Iter.of_list
                       |> Iter.flat_map (fun (_, le) ->
                              le.value |> Iter.of_list )
                       |> Iter.to_list
                       |> List.sort_uniq Command.compare
                     in
                     let value, hist = prepare_hash prev_hist value in
                     ((false, hist), {hist; value}) )
        in
        let log = ex.@(t @> pro @> local_log) in
        (* This is the point in the log after which is undefined *)
        (* Hence if the log matched a remote before then it should match after *)
        let match_upto =
          combined_log_entries |> Iter.of_seq |> zip_from ~start
          |> IterLabels.fold ~init:None ~f:(fun match_upto (idx, le) ->
                 match match_upto with
                 (* matching case*)
                 | None when le.hist = get_hist idx log ->
                     None
                 (* Logs have diverged *)
                 | None ->
                     Log.cut_after log (idx - 1) ;
                     Log.set log idx le ;
                     Some (idx - 1)
                 | Some _ ->
                     Log.set log idx le ; match_upto )
        in
        A.map
          (t @> pro @> other_nodes)
          ()
          ~f:(fun otm ->
            IdMap.map
              (fun {match_commit; matching; sent; conflicted; current_term} ->
                let matching', sent' =
                  match match_upto with
                  | None ->
                      (matching, sent)
                  | Some idx ->
                      let matching' = min matching idx in
                      (matching', min sent matching')
                in
                { match_commit
                ; current_term
                ; conflicted
                ; matching= matching'
                ; sent= sent' } )
              otm )
end

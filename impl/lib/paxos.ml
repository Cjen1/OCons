open Types
open Utils
open C.Types
open Actions_f
open A.O
open Ocons_core.Consensus_intf

module Types = struct
  type message =
    | RequestVote of {term: term; leader_commit: log_index}
    | RequestVoteResponse of
        {term: term; start_index: log_index; entries: log_entry Iter.t * int}
    | AppendEntries of
        { term: term
        ; leader_commit: log_index
        ; prev_log_index: log_index
        ; prev_log_term: term
        ; entries: log_entry Iter.t * int }
    | AppendEntriesResponse of
        {term: term; success: (log_index, log_index) Result.t; trace: time}

  type node_state =
    | Follower of {timeout: int}
    | Candidate of
        { mutable quorum: (log_index * log_entry) Iter.t Quorum.t
        ; mutable timeout: int }
    | Leader of
        { mutable rep_ackd: log_index IntMap.t (* MatchIdx *)
        ; mutable rep_sent: log_index IntMap.t (* NextIdx *)
        ; heartbeat: int }
  [@@deriving accessors]

  let timeout_a =
    [%accessor
      A.field
        ~get:(function
          | Follower {timeout} ->
              timeout
          | Candidate {timeout; _} ->
              timeout
          | Leader {heartbeat; _} ->
              heartbeat )
        ~set:(fun ns v ->
          match ns with
          | Follower _ ->
              A.set Follower.timeout ~to_:v ns
          | Candidate _ ->
              A.set Candidate.timeout ~to_:v ns
          | Leader _ ->
              A.set Leader.heartbeat ~to_:v ns )]

  type t =
    { log: log_entry SegmentLog.t
    ; log_contains: unit CIDHashtbl.t
    ; commit_index: log_index (* Guarantee that [commit_index] is >= log.vlo *)
    ; config: config
    ; node_state: node_state
    ; current_term: term
    ; current_leader: node_id option }
  [@@deriving accessors]

  let get_command idx t = (Log.get t.log idx).command |> Iter.singleton

  let get_commit_index t = t.commit_index

  module PP = struct
    let message_pp ppf v =
      let verb = true in
      let open Fmt in
      match v with
      | RequestVote {term; leader_commit} ->
          pf ppf "RequestVote {term:%d; leader_commit:%d}" term leader_commit
      | RequestVoteResponse {term; start_index; entries= entries, len} ->
          if verb then
            pf ppf
              "RequestVoteResponse {term:%d; start_index:%d; \
               entries_length:%d; entries: %a}"
              term start_index len
              (brackets @@ list ~sep:(const char ',') log_entry_pp)
              (entries |> Iter.to_list)
          else
            pf ppf
              "RequestVoteResponse {term:%d; start_index:%d; \
               entries_length:%d; entries: _ }"
              term start_index len
      | AppendEntries
          {term; leader_commit; prev_log_index; prev_log_term; entries} ->
          if verb then
            pf ppf
              "AppendEntries {term: %d; leader_commit: %d; prev_log_index: %d; \
               prev_log_term: %d; entries_length: %d; entries: %a}"
              term leader_commit prev_log_index prev_log_term (snd entries)
              (brackets @@ list ~sep:(const char ',') log_entry_pp)
              (fst entries |> Iter.to_list)
          else
            pf ppf
              "AppendEntries {term: %d; leader_commit: %d; prev_log_index: %d; \
               prev_log_term: %d; entries_length: %d; entries: _}"
              term leader_commit prev_log_index prev_log_term (snd entries)
      | AppendEntriesResponse {term; success; _} ->
          pf ppf "AppendEntriesResponse {term: %d; success: %a}" term
            (result
               ~ok:(const string "Ok: " ++ int)
               ~error:(const string "Error: " ++ int) )
            success

    let node_state_pp : node_state Fmt.t =
     fun ppf v ->
      let open Fmt in
      match v with
      | Follower {timeout} ->
          pf ppf "Follower(%d)" timeout
      | Candidate {quorum; timeout} ->
          pf ppf "Candidate {quorum:%a, timeout:%d}" Quorum.pp quorum timeout
      | Leader {rep_ackd; rep_sent; heartbeat} ->
          let format_rep_map : int IntMap.t Fmt.t =
           fun ppf v ->
            let open Fmt in
            let elts = v |> IntMap.bindings in
            pf ppf "%a"
              (brackets @@ list ~sep:comma @@ braces @@ pair ~sep:comma int int)
              elts
          in
          pf ppf "Leader{heartbeat:%d; rep_ackd:%a; rep_sent:%a" heartbeat
            format_rep_map rep_ackd format_rep_map rep_sent

    let t_pp : t Fmt.t =
     fun ppf t ->
      Fmt.pf ppf "{log: %a; commit_index:%d; current_term: %d; node_state:%a}"
        Fmt.(brackets @@ list ~sep:(const char ',') log_entry_pp)
        (t.log |> Log.iter |> Iter.to_list)
        t.commit_index t.current_term node_state_pp t.node_state
  end
end

module Make
    (Act : ActionSig with type t = Types.t and type message = Types.message) =
struct
  include Types
  module PP = PP
  open Act

  let ex = ()

  let get_log_term log idx = if idx < 0 then 0 else (Log.get log idx).term

  let set_le ct idx le =
    if Log.mem ct.log idx then
      CIDHashtbl.remove ct.log_contains (Log.get ct.log idx).command.id ;
    CIDHashtbl.replace ct.log_contains le.command.id () ;
    Log.set ct.log idx le

  let send_append_entries ?(force = false) highest =
    let ct = ex.@(t) in
    match ct.node_state with
    | Leader s ->
        let highest = Option.value ~default:(Log.highest ct.log) highest in
        ex.@(t @> node_state @> Leader.rep_sent) <-
          IntMap.mapi
            (fun id highest_sent ->
              let lo = highest_sent + 1 in
              let len =
                min (highest - lo) ex.@(t @> config @> max_append_entries)
              in
              let hi = lo + len in
              (* so we want to send the segment [lo -> hi] inclusive *)
              ( if lo <= hi || force then
                  let prev_log_index = lo - 1 in
                  let entries = Log.iter_len ct.log ~lo ~hi () in
                  send id
                  @@ AppendEntries
                       { term= ct.current_term
                       ; leader_commit= ex.@(t @> commit_index)
                       ; prev_log_index
                       ; prev_log_term= get_log_term ct.log prev_log_index
                       ; entries } ) ;
              hi )
            s.rep_sent
    | _ ->
        assert false

  let get_next_term () =
    let cterm = ex.@(t @> current_term) in
    let num_nodes = ex.@(t @> config @> num_nodes) in
    let quot, _ = (Int.div cterm num_nodes, cterm mod num_nodes) in
    let curr_epoch_term = (quot * num_nodes) + ex.@(t @> config @> node_id) in
    if cterm < curr_epoch_term then curr_epoch_term
    else curr_epoch_term + num_nodes

  let send_request_vote () =
    broadcast
    @@ RequestVote
         {term= ex.@(t @> current_term); leader_commit= ex.@(t @> commit_index)}

  let transit_follower term =
    traceln "Follower for term %d" term ;
    ex.@(t @> node_state) <-
      Follower {timeout= ex.@(t @> config @> election_timeout)} ;
    ex.@(t @> current_term) <- term

  let transit_candidate () =
    let new_term = get_next_term () in
    let num_nodes = ex.@(t @> config @> num_nodes) in
    traceln "Candidate for term %d" new_term ;
    (* Vote for self *)
    let threshold = (num_nodes / 2) + 1 - 1 in
    ex.@(t @> node_state) <-
      Candidate {quorum= Quorum.empty threshold; timeout= 1} ;
    ex.@(t @> current_term) <- new_term ;
    send_request_vote ()

  let transit_leader () =
    let ct = ex.@(t) in
    match ct.node_state with
    | Candidate {quorum; _} ->
        traceln "Leader for term %d" ct.current_term ;
        let per_seq (_, seq) =
          seq
          |> Iter.iter (fun (idx, le) ->
                 if
                   (not (Log.mem ct.log idx))
                   || (Log.get ct.log idx).term < le.term
                 then set_le ct idx le )
        in
        (*
        let optimistic_match_upto =
          quorum.Quorum.elts
          |> IntMap.map (fun elts ->
                 elts
                 |> IterLabels.fold ~init:ct.commit_index
                      ~f:(fun max_idx (idx, elt) ->
                        if [%compare.equal: log_entry] elt (Log.get ct.log idx)
                        then max max_idx idx
                        else max_idx ) )
        in
        *)
        quorum.Quorum.elts |> IntMap.to_seq |> Seq.iter per_seq ;
        (* replace term with current term since we are re-proposing it *)
        for idx = ct.commit_index + 1 to Log.highest ct.log do
          let le = Log.get ct.log idx in
          Log.set ct.log idx {le with term= ct.current_term}
        done ;
        let rep_ackd =
          ct.config.other_nodes |> List.to_seq
          |> Seq.map (fun i -> (i, -1))
          |> IntMap.of_seq
        in
        let rep_sent =
          ct.config.other_nodes |> List.to_seq
          |> Seq.map (fun i -> (i, ct.commit_index))
          |> IntMap.of_seq
        in
        ex.@(t @> node_state) <- Leader {rep_ackd; rep_sent; heartbeat= 1} ;
        send_append_entries ~force:true (Some ct.commit_index);
        send_append_entries None
    | _ ->
        assert false

  let if_recv_advance_term e =
    match (e, ex.@(t @> node_state)) with
    | ( Recv
          ( ( AppendEntries {term; _}
            | AppendEntriesResponse {term; _}
            | RequestVote {term; _}
            | RequestVoteResponse {term; _} )
          , src )
      , _ )
      when term > ex.@(t @> current_term) ->
        traceln "Follower for node %d for term %d" src term ;
        transit_follower term
    | _ ->
        ()

  let decr i = i - 1

  let resolve_event e =
    if_recv_advance_term e ;
    match (e, ex.@(t @> node_state)) with
    (* Decr ticks *)
    | Tick, _ ->
        A.map (t @> node_state @> timeout_a) ~f:decr ()
    (* Recv commands *)
    | Commands cs, Leader _ ->
        let current_ids = ex.@(t @> log_contains) in
        cs
        |> Iter.filter (fun c -> not @@ CIDHashtbl.mem current_ids c.Command.id)
        |> Iter.iter (fun c ->
               CIDHashtbl.replace current_ids c.Command.id () ;
               Log.add ex.@(t @> log) {command= c; term= ex.@(t @> current_term)} )
    | Commands _cs, _ ->
        assert false (* only occurs if space_available > 0 and not leader *)
    (* Ignore msgs from lower terms *)
    | ( Recv
          ( ( RequestVote {term; _}
            | RequestVoteResponse {term; _}
            | AppendEntries {term; _}
            | AppendEntriesResponse {term; _} )
          , _ )
      , _ )
      when term < ex.@(t @> current_term) ->
        ()
    (* Recv msgs from this term*)
    (* Candidate *)
    | Recv (RequestVoteResponse m, src), Candidate _ ->
        traceln "Received vote from %d" src ;
        assert (m.term = ex.@(t @> current_term)) ;
        let entries, _ = m.entries in
        let q_entries =
          entries |> Iter.zip_i
          |> Iter.map (fun (i, e) -> (i + m.start_index, e))
        in
        A.map
          (t @> node_state @> Candidate.quorum)
          ~f:(Quorum.add src q_entries) ()
    (* Leader *)
    | Recv (AppendEntriesResponse ({success= Ok idx; _} as m), src), Leader _ ->
        assert (m.term = ex.@(t @> current_term)) ;
        A.map (t @> node_state @> Leader.rep_ackd) () ~f:(IntMap.add src idx)
    | Recv (AppendEntriesResponse ({success= Error idx; _} as m), src), Leader _
      ->
        (* This case happens if a message is lost *)
        assert (m.term = ex.@(t @> current_term)) ;
        A.map (t @> node_state @> Leader.rep_sent) () ~f:(IntMap.add src idx)
    (* Follower *)
    | Recv (RequestVote m, cid), Follower _ ->
        ex.@(t @> node_state @> Follower.timeout) <-
          ex.@(t @> config @> election_timeout) ;
        (* Reply *)
        let t = ex.@(t) in
        let start = m.leader_commit + 1 in
        let entries = Log.iter_len t.log ~lo:start () in
        send cid
        @@ RequestVoteResponse
             {term= t.current_term; start_index= start; entries}
    | ( Recv
          ( AppendEntries
              {prev_log_term; prev_log_index; entries; leader_commit; _}
          , lid )
      , Follower _ ) -> (
        ex.@(t @> current_leader) <- Some lid ;
        (* Reset follower state *)
        ex.@(t @> node_state @> Follower.timeout) <-
          ex.@(t @> config @> election_timeout) ;
        (* reply to append entries request *)
        let ct = ex.@(t) in
        let rooted_at_start = prev_log_index = -1 && prev_log_term = 0 in
        let matching_index_and_term () =
          Log.mem ct.log prev_log_index
          && (Log.get ct.log prev_log_index).term = prev_log_term
        in
        match rooted_at_start || matching_index_and_term () with
        | false ->
            (* Reply with the highest index known not to be replicated *)
            (* This will be the prev_log_index of the next msg *)
            dtraceln
              "Failed to match\n\
               rooted_at_start(%b), matching_index_and_term(%b):\n"
              rooted_at_start
              (matching_index_and_term ()) ;
            send lid
            @@ AppendEntriesResponse
                 { term= ct.current_term
                 ; success=
                     Error (min (prev_log_index - 1) (Log.highest ct.log))
                 ; trace= Unix.gettimeofday () }
        | true ->
            let index_iter =
              fst entries |> Iter.zip_i
              |> Iter.map (fun (i, v) -> (i + prev_log_index + 1, v))
            in
            index_iter
            |> Iter.iter (fun (idx, le) ->
                   if
                     (not (Log.mem ct.log idx))
                     || (Log.get ct.log idx).term < le.term
                   then set_le ct idx le ) ;
            let max_entry =
              index_iter |> Iter.map fst |> Iter.fold max prev_log_index
            in
            Log.cut_after ct.log max_entry ;
            A.map (t @> commit_index) ~f:(max leader_commit) () ;
            send lid
            @@ AppendEntriesResponse
                 { term= ct.current_term
                 ; success= Ok max_entry
                 ; trace= Unix.gettimeofday () } )
    (*Invalid or already handled *)
    | _ ->
        ()

  let check_conditions () =
    let ct = ex.@(t) in
    match ct.node_state with
    (* check if can become leader *)
    | Candidate {quorum; _} when Quorum.satisified quorum ->
        transit_leader ()
    (* send msg if exists entries to send *)
    | Leader _ ->
        send_append_entries None
    | _ ->
        ()

  let resolve_timeouts () =
    let ct = ex.@(t) in
    match ct.node_state with
    (* When should ticking result in an action? *)
    | Follower s when s.timeout <= 0 ->
        transit_candidate ()
    | Candidate {timeout; _} when timeout <= 0 ->
        send_request_vote () ;
        ex.@(t @> node_state @> Candidate.timeout) <-
          ex.@(t @> config @> election_timeout)
    | Leader {heartbeat; _} when heartbeat <= 0 ->
        send_append_entries ~force:true None ;
        ex.@(t @> node_state @> Leader.heartbeat) <- 1
    | _ ->
        ()

  let check_commit () =
    let ct = ex.@(t) in
    match ct.node_state with
    | Leader {rep_ackd; _} ->
        let acks =
          rep_ackd |> IntMap.to_seq
          |> Seq.map (fun (_, v) -> v)
          |> List.of_seq
          |> List.cons (ex.@(t @> log) |> Log.highest)
          |> List.sort (fun a b -> Int.neg @@ Int.compare a b)
        in
        let majority_rep = List.nth acks (ct.config.phase2quorum - 1) in
        (* only commit if the commit index is from this term *)
        if get_log_term ex.@(t @> log) majority_rep = ex.@(t @> current_term)
        then A.map (t @> commit_index) ~f:(max majority_rep) ()
    | _ ->
        ()

  let advance_raw e =
    resolve_event e ;
    resolve_timeouts () ;
    check_commit () ;
    check_conditions ()

  let advance t e = run_side_effects (fun () -> advance_raw e) t

  let create config =
    let log = SegmentLog.create {term= -1; command= empty_command} in
    { log
    ; log_contains= CIDHashtbl.create 0
    ; commit_index= -1
    ; config
    ; node_state= Follower {timeout= 0}
    ; current_term= 0
    ; current_leader= None }

  let get_leader t = t.current_leader
end

module Impl = Make (ImperativeActions (Types))

open Types
open Base
open Utils
module L = Log

let src = Logs.Src.create "Paxos" ~doc:"Paxos core algorithm"

module Log = (val Logs.src_log src : Logs.LOG)

(* R before means receiving *)
type event =
  [ `Tick
  | `RRequestVote of node_id * request_vote
  | `RRequestVoteResponse of node_id * request_vote_response
  | `RAppendEntries of node_id * append_entries
  | `RAppendEntiresResponse of node_id * append_entries_response
  | `LogAddition ]

type action =
  [ `SendRequestVote of node_id * request_vote
  | `SendRequestVoteResponse of node_id * request_vote_response
  | `SendAppendEntries of node_id * append_entries
  | `SendAppendEntriesResponse of node_id * append_entries_response
  | `CommitIndexUpdate of log_index ]

let pp_action f (x : action) =
  match x with
  | `SendRequestVote (id, _rv) ->
      Fmt.pf f "SendRequestVote to %a" Fmt.int64 id
  | `SendRequestVoteResponse (id, _) ->
      Fmt.pf f "SendRequestVoteResponse to %a" Fmt.int64 id
  | `SendAppendEntries (id, _) ->
      Fmt.pf f "SendAppendEntries to %a" Fmt.int64 id
  | `SendAppendEntriesResponse (id, _) ->
      Fmt.pf f "SendAppendEntriesResponse to %a" Fmt.int64 id
  | `CommitIndexUpdate i ->
      Fmt.pf f "CommitIndexUpdate to %a" Fmt.int64 i

type config =
  { phase1majority: int
  ; phase2majority: int
  ; other_nodes: node_id list
  ; num_nodes: int
  ; node_id: node_id }

type node_state =
  | Follower of {heartbeat: bool}
  | Candidate of
      {quorum: node_id Quorum.t; entries: log_entry list; start_index: log_index}
  | Leader of
      { match_index: (node_id, log_index, Int64.comparator_witness) Map.t
      ; next_index: (node_id, log_index, Int64.comparator_witness) Map.t
      ; heartbeat: bool }

let pp_node_state f (x : node_state) =
  match x with
  | Follower _ ->
      Fmt.pf f "Follower"
  | Candidate _ ->
      Fmt.pf f "Candidate"
  | Leader _ ->
      Fmt.pf f "Leader"

type t =
  { config: config
  ; current_term: Term.t
  ; log: L.t
  ; commit_index: log_index
  ; node_state: node_state }

let make_append_entries (t : t) next_index =
  let term = t.current_term.t in
  let prev_log_index = Int64.(next_index - one) in
  let prev_log_term = L.get_term_exn t.log prev_log_index in
  let entries = L.entries_after_inc t.log next_index in
  let leader_commit = t.commit_index in
  {term; prev_log_index; prev_log_term; entries; leader_commit}

let check_commit_index t =
  match t.node_state with
  | Leader s ->
      let commit_index =
        Map.fold s.match_index ~init:[L.get_max_index t.log]
          ~f:(fun ~key:_ ~data acc -> data :: acc)
        |> List.sort ~compare:Int64.compare
        |> List.rev
        |> fun ls -> List.nth_exn ls (t.config.phase2majority - 1)
      in
      let actions =
        if Int64.(commit_index <> t.commit_index) then
          [`CommitIndexUpdate commit_index]
        else []
      in
      (commit_index, actions)
  | _ ->
      (t.commit_index, [])

let transition_to_leader t =
  match t.node_state with
  | Candidate s ->
      Log.info (fun m -> m "Transition to leader") ;
      let entries =
        List.map s.entries ~f:(fun entry -> {entry with term= t.current_term.t})
      in
      let log =
        L.add_entries_remove_conflicts t.log ~start_index:s.start_index entries
      in
      let match_index, next_index =
        List.fold t.config.other_nodes
          ~init:(Map.empty (module Int64), Map.empty (module Int64))
          ~f:(fun (mi, ni) id ->
            let mi = Map.set mi ~key:id ~data:Int64.zero in
            let ni = Map.set ni ~key:id ~data:Int64.(t.commit_index + one) in
            (mi, ni))
      in
      let node_state = Leader {match_index; next_index; heartbeat= false} in
      let t = {t with node_state; log} in
      let fold actions node_id =
        let next_index = Map.find_exn next_index node_id in
        `SendAppendEntries (node_id, make_append_entries t next_index)
        :: actions
      in
      let actions = List.fold t.config.other_nodes ~init:[] ~f:fold in
      (t, actions)
  | _ ->
      raise
      @@ Invalid_argument
           "Cannot transition to leader from states other than candidate"

let update_current_term term t =
  let current_term = Term.update_current_term term t.current_term in
  {t with current_term}

let transition_to_candidate t =
  let updated_term : term =
    let open Int64 in
    let id_in_current_epoch =
      t.current_term.t
      - (t.current_term.t % of_int t.config.num_nodes)
      + t.config.node_id
    in
    if id_in_current_epoch <= t.current_term.t then
      id_in_current_epoch + of_int t.config.num_nodes
    else id_in_current_epoch
  in
  Log.info (fun m ->
      m "Transition to candidate term = %a" Fmt.int64 updated_term) ;
  let quorum = Quorum.empty t.config.phase1majority Int64.equal in
  let quorum =
    match Quorum.add t.config.node_id quorum with
    | Ok quorum ->
        quorum
    | Error _ ->
        assert false
  in
  let entries = L.entries_after_inc t.log Int64.(t.commit_index + one) in
  let node_state =
    Candidate {quorum; entries; start_index= Int64.(t.commit_index + one)}
  in
  let t = {t with node_state} |> update_current_term updated_term in
  if Quorum.satisified quorum then transition_to_leader t
  else
    let actions =
      let fold actions node_id =
        `SendRequestVote
          (node_id, {term= t.current_term.t; leader_commit= t.commit_index})
        :: actions
      in
      List.fold t.config.other_nodes ~init:[] ~f:fold
    in
    (t, actions)

let transition_to_follower t =
  Log.info (fun m -> m "Transition to Follower") ;
  {t with node_state= Follower {heartbeat= false}}

let rec handle t : event -> t * action list =
 fun event ->
  match (event, t.node_state) with
  | `Tick, Follower {heartbeat= false} ->
      transition_to_candidate t
  | `Tick, Follower {heartbeat= true} ->
      ({t with node_state= Follower {heartbeat= false}}, [])
  | `Tick, Leader ({heartbeat= false; _} as s) ->
      let highest_index = L.get_max_index t.log in
      let fold actions node_id =
        let next_index = Map.find_exn s.next_index node_id in
        if Int64.(next_index >= highest_index) then
          `SendAppendEntries (node_id, make_append_entries t next_index)
          :: actions
        else actions
      in
      let actions = List.fold t.config.other_nodes ~f:fold ~init:[] in
      ({t with node_state= Leader {s with heartbeat= true}}, actions)
  | `Tick, _ ->
      (t, [])
  | (`RRequestVote (_, {term; _}) as event), _
  | (`RRequestVoteResponse (_, {term; _}) as event), _
  | (`RAppendEntries (_, {term; _}) as event), _
  | (`RAppendEntiresResponse (_, {term; _}) as event), _
    when Int64.(t.current_term.t < term) ->
      let t = t |> update_current_term term |> transition_to_follower in
      handle t event
  | `RRequestVote (src, msg), _ when Int64.(msg.term < t.current_term.t) ->
      let actions =
        [ `SendRequestVoteResponse
            ( src
            , { term= t.current_term.t
              ; vote_granted= false
              ; entries= []
              ; start_index= msg.leader_commit } ) ]
      in
      (t, actions)
  | `RRequestVote (src, msg), _ ->
      let actions =
        let entries = L.entries_after_inc t.log msg.leader_commit in
        [ `SendRequestVoteResponse
            ( src
            , { term= t.current_term.t
              ; vote_granted= true
              ; entries
              ; start_index= Int64.(msg.leader_commit + one) } ) ]
      in
      let t =
        match t.node_state with
        | Follower _s ->
            {t with node_state= Follower {heartbeat= true}}
        | _ ->
            t
      in
      (t, actions)
  | `RRequestVoteResponse (src_id, msg), Candidate s
    when Int64.(msg.term = t.current_term.t) && msg.vote_granted -> (
    match Quorum.add src_id s.quorum with
    | Error `AlreadyInList ->
        (t, [])
    | Ok (quorum : node_id Quorum.t) ->
        let merge x sx y sy =
          assert (Int64.(sx = sy)) ;
          let rec loop acc : 'a -> log_entry list = function
            | xs, [] ->
                xs
            | [], ys ->
                ys
            | x :: xs, y :: ys ->
                let res = if Int64.(x.term < y.term) then y else x in
                loop (res :: acc) (xs, ys)
          in
          loop [] (x, y) |> List.rev
        in
        let entries =
          merge s.entries s.start_index msg.entries msg.start_index
        in
        let node_state = Candidate {s with quorum; entries} in
        let t = {t with node_state} in
        if Quorum.satisified quorum then t |> transition_to_leader else (t, [])
    )
  | `RRequestVoteResponse _, _ ->
      (t, [])
  | `RAppendEntries (src, msg), _ -> (
      let send t res =
        `SendAppendEntriesResponse (src, {term= t.current_term.t; success= res})
      in
      match L.get_term t.log msg.prev_log_index with
      | _ when Int64.(msg.term < t.current_term.t) ->
          (t, [send t (Error msg.prev_log_index)])
      | Error _ ->
          (t, [send t (Error msg.prev_log_index)])
      | Ok log_term when Int64.(log_term <> msg.prev_log_term) ->
          (t, [send t (Error msg.prev_log_index)])
      | Ok _ ->
          let log =
            L.add_entries_remove_conflicts t.log
              ~start_index:Int64.(msg.prev_log_index + one)
              msg.entries
          in
          let match_index =
            Int64.(msg.prev_log_index + (of_int @@ List.length msg.entries))
          in
          let commit_index =
            let last_entry =
              match List.hd msg.entries with
              | None ->
                  msg.prev_log_index
              | Some _ ->
                  match_index
            in
            if Int64.(msg.leader_commit > t.commit_index) then
              Int64.(min msg.leader_commit last_entry)
            else t.commit_index
          in
          let t =
            match t.node_state with
            | Follower _s ->
                {t with node_state= Follower {heartbeat= true}}
            | _ ->
                t
          in
          ({t with log; commit_index}, [send t (Ok match_index)]) )
  | `RAppendEntiresResponse (src, msg), Leader s
    when Int64.(msg.term = t.current_term.t) -> (
    match msg.success with
    | Ok match_index ->
        let updated_match_index =
          Int64.(max match_index @@ Map.find_exn s.match_index src)
        in
        let match_index =
          Map.set s.match_index ~key:src ~data:updated_match_index
        in
        let next_index =
          Map.set s.next_index ~key:src ~data:Int64.(updated_match_index + one)
        in
        let node_state = Leader {match_index; next_index; heartbeat= false} in
        let t = {t with node_state} in
        let commit_index, actions = check_commit_index t in
        ({t with commit_index}, actions)
    | Error prev_log_index ->
        let next_index = Map.set s.next_index ~key:src ~data:prev_log_index in
        let actions =
          [ `SendAppendEntries
              (src, make_append_entries t Int64.(prev_log_index + one)) ]
        in
        ({t with node_state= Leader {s with next_index}}, actions) )
  | `RAppendEntiresResponse _, _ ->
      (t, [])
  | `LogAddition, Leader s ->
      let highest_index = L.get_max_index t.log in
      let fold actions node_id =
        let next_index = Map.find_exn s.next_index node_id in
        if Int64.(highest_index >= next_index) then
          `SendAppendEntries (node_id, make_append_entries t next_index)
          :: actions
        else actions
      in
      let actions = List.fold t.config.other_nodes ~f:fold ~init:[] in
      let commit_index, actions' = check_commit_index t in
      ({t with commit_index}, actions' @ actions)
  | `LogAddition, _ ->
      (t, [])

let create_node config log current_term =
  Log.info (fun m -> m "Creating new node with id %a" Fmt.int64 config.node_id) ;
  { config
  ; log
  ; current_term
  ; commit_index= Int64.zero
  ; node_state= Follower {heartbeat= false} }

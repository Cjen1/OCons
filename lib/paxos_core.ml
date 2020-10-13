open! Core
open! Types
open Types.MessageTypes
module L = Types.Log
module T = Types.Term
module U = Utils
module IdMap = Map.Make (Int)

let src = Logs.Src.create "Paxos" ~doc:"Paxos core algorithm"

module Log = (val Logs.src_log src : Logs.LOG)

(* R before means receiving *)
type event =
  [ `Tick
  | `RRequestVote of node_id * request_vote
  | `RRequestVoteResponse of node_id * request_vote_response
  | `RAppendEntries of node_id * append_entries
  | `RAppendEntiresResponse of node_id * append_entries_response
  | `Commands of command list ]

type persistant_change = [`Log of L.op | `Term of T.op]

type action =
  [ `SendRequestVote of node_id * request_vote
  | `SendRequestVoteResponse of node_id * request_vote_response
  | `SendAppendEntries of node_id * append_entries
  | `SendAppendEntriesResponse of node_id * append_entries_response
  | `CommitIndexUpdate of log_index
  | `PersistantChange of persistant_change
  | `Sync 
  | `Unapplied of command list]

let compare_apply_order a b =
  match (a, b) with
  | `PersistantChange _, _ ->
      -1
  | _, `PersistantChange _ ->
      1
  | `Sync, _ ->
      -1
  | _, `Sync ->
      1
  | `CommitIndexUpdate _, _ ->
      -1
  | _, `CommitIndexUpdate _ ->
      1
  | _ ->
      0

module Comp = struct
  type 'a t = 'a * action list

  let return x = (x, [])
end

module CompRes = struct
  type ('a, 'b) t = ('a Comp.t, 'b) Result.t

  let bind : ('a, 'b) t -> ('a -> ('c, 'b) t) -> ('c, 'b) t =
   fun x f ->
    let ( let* ) x f = Result.bind x ~f in
    let* x, acts = x in
    match f x with Error e -> Error e | Ok (x, acts') -> Ok (x, acts' @ acts)

  let return v = Ok (Comp.return v)

  let error v = Error v

  let append action : (unit, 'b) t = Ok ((), [action])

  let appendv av : (unit, 'b) t = Ok ((), av)
end

let ( let+ ) x f = CompRes.(bind) x f

let ( let* ) x f = Result.bind x ~f

let log_op_to_action op : action = `PersistantChange (`Log op)

let term_op_to_action op : action = `PersistantChange (`Term op)

let pp_action f (x : action) =
  match x with
  | `SendRequestVote (id, _rv) ->
      Fmt.pf f "SendRequestVote to %d" id
  | `SendRequestVoteResponse (id, _) ->
      Fmt.pf f "SendRequestVoteResponse to %d" id
  | `SendAppendEntries (id, _) ->
      Fmt.pf f "SendAppendEntries to %d" id
  | `SendAppendEntriesResponse (id, _) ->
      Fmt.pf f "SendAppendEntriesResponse to %d" id
  | `CommitIndexUpdate i ->
      Fmt.pf f "CommitIndexUpdate to %a" Fmt.int64 i
  | `PersistantChange (`Log _) ->
      Fmt.pf f "PersistantChange to Log"
  | `PersistantChange (`Term _) ->
      Fmt.pf f "PersistantChange to Term"
  | `Sync ->
      Fmt.pf f "Sync"
  | `Unapplied cmds ->
    Fmt.pf f "Unapplied %s" ([%sexp_of: command list] cmds |> Sexp.to_string)

type config =
  { phase1majority: int
  ; phase2majority: int
  ; other_nodes: node_id list
  ; num_nodes: int
  ; node_id: node_id
  ; election_timeout: int }
[@@deriving sexp]

type node_state =
  | Follower of {heartbeat: int}
  | Candidate of
      { quorum: node_id U.Quorum.t
      ; entries: log_entry list
      ; start_index: log_index }
  | Leader of
      { match_index: log_index IdMap.t
      ; next_index: log_index IdMap.t
      ; heartbeat: int }
[@@deriving sexp_of]

let pp_node_state f (x : node_state) =
  match x with
  | Follower {heartbeat} ->
      Fmt.pf f "Follower(%d)" heartbeat
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
[@@deriving sexp_of]

let make_append_entries (t : t) next_index =
  let term = t.current_term in
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
      let+ () =
        if Int64.(commit_index <> t.commit_index) then
          CompRes.append @@ `CommitIndexUpdate commit_index
        else CompRes.return ()
      in
      CompRes.return {t with commit_index}
  | _ ->
      CompRes.return t

let transition_to_leader t =
  match t.node_state with
  | Candidate s ->
      Log.info (fun m -> m "Transition to leader") ;
      let entries =
        List.map s.entries ~f:(fun entry -> {entry with term= t.current_term})
      in
      (* If the node has the highest node_id then it will write its entire log back to disk with a higher term ... This feels like a bug but is in the protocol *)
      let log, actions =
        L.add_entries_remove_conflicts t.log ~start_index:s.start_index entries
      in
      let+ () = CompRes.appendv @@ List.map ~f:log_op_to_action actions in
      let match_index, next_index =
        List.fold t.config.other_nodes ~init:(IdMap.empty, IdMap.empty)
          ~f:(fun (mi, ni) id ->
            let mi = Map.set mi ~key:id ~data:Int64.zero in
            let ni = Map.set ni ~key:id ~data:Int64.(t.commit_index + one) in
            (mi, ni))
      in
      let node_state = Leader {match_index; next_index; heartbeat= 0} in
      let t = {t with node_state; log} in
      let fold actions node_id =
        let next_index = Map.find_exn next_index node_id in
        `SendAppendEntries (node_id, make_append_entries t next_index)
        :: actions
      in
      let+ t = check_commit_index t in
      let+ () =
        CompRes.appendv @@ List.fold t.config.other_nodes ~init:[] ~f:fold
      in
      CompRes.return t
  | _ ->
      CompRes.error
      @@ `Msg "Cannot transition to leader from states other than candidate"

let update_current_term term t =
  let current_term, ops = T.update_term t.current_term term in
  let actions = List.map ~f:term_op_to_action ops in
  let+ () = CompRes.appendv actions in
  CompRes.return {t with current_term}

let transition_to_candidate t =
  let updated_term : term =
    let id_in_current_epoch =
      t.current_term - (t.current_term % t.config.num_nodes) + t.config.node_id
    in
    if id_in_current_epoch <= t.current_term then
      id_in_current_epoch + t.config.num_nodes
    else id_in_current_epoch
  in
  Log.info (fun m -> m "Transition to candidate term = %d" updated_term) ;
  let quorum = U.Quorum.empty t.config.phase1majority Int.equal in
  let* quorum =
    match U.Quorum.add t.config.node_id quorum with
    | Ok quorum ->
        Ok quorum
    | Error _ ->
        CompRes.error @@ `Msg "Tried to add element to empty quorum and failed"
  in
  let entries = L.entries_after_inc t.log Int64.(t.commit_index + one) in
  let node_state =
    Candidate {quorum; entries; start_index= Int64.(t.commit_index + one)}
  in
  let t = {t with node_state} in
  let+ t = update_current_term updated_term t in
  if U.Quorum.satisified quorum then transition_to_leader t
  else
    let actions =
      let fold actions node_id =
        `SendRequestVote
          (node_id, {term= t.current_term; leader_commit= t.commit_index})
        :: actions
      in
      List.fold t.config.other_nodes ~init:[] ~f:fold
    in
    let+ () = CompRes.appendv actions in
    CompRes.return t

let transition_to_follower t =
  Log.info (fun m -> m "Transition to Follower") ;
  CompRes.return {t with node_state= Follower {heartbeat= 0}}

let rec advance_raw t (event : event) : (t, 'b) CompRes.t =
  match (event, t.node_state) with
  | `Tick, Follower {heartbeat} when heartbeat >= t.config.election_timeout ->
      Log.debug (fun m -> m "transition to candidate") ;
      transition_to_candidate t
  | `Tick, Follower {heartbeat} ->
      Log.debug (fun m ->
          m "Tick: increment %d to %d" heartbeat (heartbeat + 1)) ;
      CompRes.return {t with node_state= Follower {heartbeat= heartbeat + 1}}
  | `Tick, Leader ({heartbeat; _} as s) when heartbeat > 0 ->
      let highest_index = L.get_max_index t.log in
      let fold actions node_id =
        let next_index = Map.find_exn s.next_index node_id in
        if Int64.(next_index >= highest_index) then
          `SendAppendEntries (node_id, make_append_entries t next_index)
          :: actions
        else actions
      in
      let+ () =
        CompRes.appendv @@ List.fold t.config.other_nodes ~f:fold ~init:[]
      in
      CompRes.return {t with node_state= Leader {s with heartbeat= 0}}
  | `Tick, Leader ({heartbeat; _} as s) ->
      CompRes.return
        {t with node_state= Leader {s with heartbeat= heartbeat + 1}}
  | `Tick, _ ->
      CompRes.return t
  | (`RRequestVote (_, {term; _}) as event), _
  | (`RRequestVoteResponse (_, {term; _}) as event), _
  | (`RAppendEntries (_, {term; _}) as event), _
  | (`RAppendEntiresResponse (_, {term; _}) as event), _
    when Int.(t.current_term < term) ->
      let+ t = update_current_term term t in
      let+ t = transition_to_follower t in
      advance_raw t event
  | `RRequestVote (src, msg), _ when Int.(msg.term < t.current_term) ->
      let+ () =
        CompRes.append
        @@ `SendRequestVoteResponse
             ( src
             , { term= t.current_term
               ; vote_granted= false
               ; entries= []
               ; start_index= Int64.(msg.leader_commit + one) } )
      in
      CompRes.return t
  | `RRequestVote (src, msg), _ ->
      let+ () =
        CompRes.append
        @@
        let entries =
          L.entries_after_inc t.log Int64.(msg.leader_commit + one)
        in
        `SendRequestVoteResponse
          ( src
          , { term= t.current_term
            ; vote_granted= true
            ; entries
            ; start_index= Int64.(msg.leader_commit + one) } )
      in
      let t =
        match t.node_state with
        | Follower _s ->
            {t with node_state= Follower {heartbeat= 0}}
        | _ ->
            t
      in
      CompRes.return t
  | `RRequestVoteResponse (src_id, msg), Candidate s
    when Int.(msg.term = t.current_term) && msg.vote_granted -> (
    match U.Quorum.add src_id s.quorum with
    | Error `AlreadyInList ->
        CompRes.return t
    | Ok (quorum : node_id U.Quorum.t) ->
        let merge x sx y sy =
          if not Int64.(sx = sy) then (
            Log.err (fun m ->
                m "RRequestVoteResponse %a != %a" Fmt.int64 sx Fmt.int64 sy) ;
            CompRes.error
            @@ `Msg "Can't merge entries not starting at same point" )
          else
            let rec loop acc : 'a -> log_entry list = function
              | xs, [] ->
                  xs
              | [], ys ->
                  ys
              | x :: xs, y :: ys ->
                  let res = if Int.(x.term < y.term) then y else x in
                  loop (res :: acc) (xs, ys)
            in
            loop [] (x, y) |> List.rev |> fun v -> Ok v
        in
        let* entries =
          merge s.entries s.start_index msg.entries msg.start_index
        in
        let node_state = Candidate {s with quorum; entries} in
        let t = {t with node_state} in
        if U.Quorum.satisified quorum then transition_to_leader t
        else CompRes.return t )
  | `RRequestVoteResponse _, _ ->
      CompRes.return t
  | `RAppendEntries (src, msg), _ -> (
      let send t res =
        CompRes.append
        @@ `SendAppendEntriesResponse (src, {term= t.current_term; success= res})
      in
      match L.get_term t.log msg.prev_log_index with
      | _ when Int.(msg.term < t.current_term) ->
          let+ () = send t (Error msg.prev_log_index) in
          CompRes.return t
      | Error _ ->
          let+ () = send t (Error msg.prev_log_index) in
          CompRes.return t
      | Ok log_term when Int.(log_term <> msg.prev_log_term) ->
          let+ () = send t (Error msg.prev_log_index) in
          CompRes.return t
      | Ok _ ->
          let log, log_ops =
            L.add_entries_remove_conflicts t.log
              ~start_index:Int64.(msg.prev_log_index + one)
              msg.entries
          in
          let t = {t with log} in
          let+ () = CompRes.appendv @@ List.map ~f:log_op_to_action log_ops in
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
          let t = {t with commit_index} in
          let t =
            match t.node_state with
            | Follower _s ->
                {t with node_state= Follower {heartbeat= 0}}
            | _ ->
                t
          in
          let+ () = send t (Ok match_index) in
          CompRes.return t )
  | `RAppendEntiresResponse (src, msg), Leader s
    when Int.(msg.term = t.current_term) -> (
    match msg.success with
    | Ok match_index ->
        let updated_match_index =
          Int64.(max match_index @@ IdMap.find_exn s.match_index src)
        in
        let match_index =
          Map.set s.match_index ~key:src ~data:updated_match_index
        in
        let next_index =
          Map.set s.next_index ~key:src ~data:Int64.(updated_match_index + one)
        in
        let node_state = Leader {s with match_index; next_index} in
        let t = {t with node_state} in
        check_commit_index t
    | Error prev_log_index ->
        let next_index = Map.set s.next_index ~key:src ~data:prev_log_index in
        let+ () =
          CompRes.append
          @@ `SendAppendEntries
               (src, make_append_entries t Int64.(prev_log_index + one))
        in
        CompRes.return {t with node_state= Leader {s with next_index}} )
  | `RAppendEntiresResponse _, _ ->
      CompRes.return t
  | `Commands cs, Leader s ->
      let cs = List.filter cs ~f:(fun cmd -> not @@ L.id_in_log t.log cmd.id) in
      let+ t =
        List.fold cs ~init:(CompRes.return t) ~f:(fun t command ->
            let+ t = t in
            let log, ops = L.add {term= t.current_term; command} t.log in
            let+ () = CompRes.appendv @@ List.map ~f:log_op_to_action ops in
            CompRes.return {t with log})
      in
      let+ t = check_commit_index t in
      let highest_index = L.get_max_index t.log in
      let fold t_comp node_id =
        let+ t = t_comp in
        let next_index = Map.find_exn s.next_index node_id in
        if Int64.(highest_index >= next_index) then
          let+ () =
            CompRes.append
            @@ `SendAppendEntries (node_id, make_append_entries t next_index)
          in
          CompRes.return t
        else CompRes.return t
      in
      List.fold t.config.other_nodes ~f:fold ~init:(CompRes.return t)
  | `Commands cs, _ ->
    let+ () = CompRes.append (`Unapplied cs) in
    CompRes.return t

let advance t event : (t, 'b) CompRes.t =
  let* t, actions = advance_raw t event in
  let sync_required =
    List.exists actions ~f:(function
      | `SendAppendEntriesResponse _
      | `SendRequestVoteResponse _
      | `CommitIndexUpdate _ ->
          true
      | _ ->
          false)
  in
  let actions = if sync_required then `Sync :: actions else actions in
  Ok (t, actions)

let is_leader t = match t.node_state with Leader _ -> true | _ -> false

let get_log (t : t) = t.log
let get_term (t : t) = t.current_term

let create_node config log current_term =
  Log.info (fun m -> m "Creating new node with id %d" config.node_id) ;
  { config
  ; log
  ; current_term
  ; commit_index= Int64.zero
  ; node_state= Follower {heartbeat= config.election_timeout} }

module Test = struct
  module Comp = Comp
  module CompRes = CompRes
  let transition_to_leader = transition_to_leader
  let transition_to_candidate = transition_to_candidate
  let transition_to_follower = transition_to_follower
  let advance = advance
  let get_node_state t = t.node_state
  let get_commit_index t = t.commit_index
end

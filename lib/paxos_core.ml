open Types
open Base
module L = Log
module T = Term
open Utils

let src = Logs.Src.create "Paxos" ~doc:"Paxos core algorithm"

module Log = (val Logs.src_log src : Logs.LOG)

(* R before means receiving *)
type event =
  [ `Tick
  | `RRequestVote of node_id * request_vote
  | `RRequestVoteResponse of node_id * request_vote_response
  | `RAppendEntries of node_id * append_entries
  | `RAppendEntiresResponse of node_id * append_entries_response
  | `LogAddition of command list ]

type persistant_change = [`Log of L.op | `Term of T.op]

type action =
  [ `SendRequestVote of node_id * request_vote
  | `SendRequestVoteResponse of node_id * request_vote_response
  | `SendAppendEntries of node_id * append_entries
  | `SendAppendEntriesResponse of node_id * append_entries_response
  | `CommitIndexUpdate of log_index
  | `PersistantChange of persistant_change ]

module Comp = struct
  type 'a t = action list -> 'a * action list

  let ( >>= ) (x : 'a t) (f : 'a -> 'b t) : 'b t =
   fun ls ->
    let v, ls = x ls in
    (f v) ls

  let get : action list t = fun ls -> (List.rev ls, ls)

  let append action : unit t = fun ls -> ((), action :: ls)

  let appendv ls' : unit t = fun ls -> ((), List.rev ls' @ ls)

  let return (x : 'a) : 'a t = fun ls -> (x, ls)

  let run (comp : 'a t) : 'a * action list =
    comp [] |> fun (v, ls) -> (v, List.rev ls)
end

module C = Comp

let ( let+ ) x f = C.( >>= ) x f

let log_op_to_action op : action = `PersistantChange (`Log op)

let term_op_to_action op : action = `PersistantChange (`Term op)

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
  | `PersistantChange (`Log _) ->
      Fmt.pf f "PersistantChange to Log"
  | `PersistantChange (`Term _) ->
      Fmt.pf f "PersistantChange to Term"

type config =
  { phase1majority: int
  ; phase2majority: int
  ; other_nodes: node_id list
  ; num_nodes: int
  ; node_id: node_id
  ; election_timeout: int }

type node_state =
  | Follower of {heartbeat: int}
  | Candidate of
      {quorum: node_id Quorum.t; entries: log_entry list; start_index: log_index}
  | Leader of
      { match_index: (node_id, log_index, Int64.comparator_witness) Map.t
      ; next_index: (node_id, log_index, Int64.comparator_witness) Map.t
      ; heartbeat: int }

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
          C.append @@ `CommitIndexUpdate commit_index
        else C.return ()
      in
      C.return {t with commit_index}
  | _ ->
      C.return t

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
      let+ () = C.appendv @@ List.map ~f:log_op_to_action actions in
      let match_index, next_index =
        List.fold t.config.other_nodes
          ~init:(Map.empty (module Int64), Map.empty (module Int64))
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
      let+ () = C.appendv @@ List.fold t.config.other_nodes ~init:[] ~f:fold in
      C.return t
  | _ ->
      raise
      @@ Invalid_argument
           "Cannot transition to leader from states other than candidate"

let update_current_term term t =
  let current_term, ops = T.update t.current_term term in
  let actions = List.map ~f:term_op_to_action ops in
  let+ () = C.appendv actions in
  C.return {t with current_term}

let transition_to_candidate t =
  let updated_term : term =
    let open Int64 in
    let id_in_current_epoch =
      t.current_term
      - (t.current_term % of_int t.config.num_nodes)
      + t.config.node_id
    in
    if id_in_current_epoch <= t.current_term then
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
        raise
        @@ Invalid_argument "Tried to add element to empty quorum and failed"
  in
  let entries = L.entries_after_inc t.log Int64.(t.commit_index + one) in
  let node_state =
    Candidate {quorum; entries; start_index= Int64.(t.commit_index + one)}
  in
  let t = {t with node_state} in
  let+ t = update_current_term updated_term t in
  if Quorum.satisified quorum then transition_to_leader t
  else
    let actions =
      let fold actions node_id =
        `SendRequestVote
          (node_id, {term= t.current_term; leader_commit= t.commit_index})
        :: actions
      in
      List.fold t.config.other_nodes ~init:[] ~f:fold
    in
    let+ () = C.appendv actions in
    C.return t

let transition_to_follower t =
  Log.info (fun m -> m "Transition to Follower") ;
  C.return {t with node_state= Follower {heartbeat= 0}}

let rec advance_raw t : event -> t C.t =
 fun event ->
  match (event, t.node_state) with
  | `Tick, Follower {heartbeat} when heartbeat >= t.config.election_timeout ->
      Log.debug (fun m -> m "transition to candidate") ;
      transition_to_candidate t
  | `Tick, Follower {heartbeat} ->
      Log.debug (fun m ->
          m "Tick: increment %d to %d" heartbeat (heartbeat + 1)) ;
      C.return {t with node_state= Follower {heartbeat= heartbeat + 1}}
  | `Tick, Leader ({heartbeat; _} as s) when heartbeat > 0 ->
      let highest_index = L.get_max_index t.log in
      let fold actions node_id =
        let next_index = Map.find_exn s.next_index node_id in
        if Int64.(next_index >= highest_index) then
          `SendAppendEntries (node_id, make_append_entries t next_index)
          :: actions
        else actions
      in
      let+ () = C.appendv @@ List.fold t.config.other_nodes ~f:fold ~init:[] in
      C.return {t with node_state= Leader {s with heartbeat= 0}}
  | `Tick, Leader ({heartbeat; _} as s) ->
      C.return {t with node_state= Leader {s with heartbeat= heartbeat + 1}}
  | `Tick, _ ->
      C.return t
  | (`RRequestVote (_, {term; _}) as event), _
  | (`RRequestVoteResponse (_, {term; _}) as event), _
  | (`RAppendEntries (_, {term; _}) as event), _
  | (`RAppendEntiresResponse (_, {term; _}) as event), _
    when Int64.(t.current_term < term) ->
      let+ t = update_current_term term t in
      let+ t = transition_to_follower t in
      advance_raw t event
  | `RRequestVote (src, msg), _ when Int64.(msg.term < t.current_term) ->
      let+ () =
        C.append
        @@ `SendRequestVoteResponse
             ( src
             , { term= t.current_term
               ; vote_granted= false
               ; entries= []
               ; start_index= Int64.(msg.leader_commit + one) } )
      in
      C.return t
  | `RRequestVote (src, msg), _ ->
      let+ () =
        C.append
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
      C.return t
  | `RRequestVoteResponse (src_id, msg), Candidate s
    when Int64.(msg.term = t.current_term) && msg.vote_granted -> (
    match Quorum.add src_id s.quorum with
    | Error `AlreadyInList ->
        C.return t
    | Ok (quorum : node_id Quorum.t) ->
      (* TODO check correctness regarding ordering *)
        let merge x sx y sy =
          if not Int64.(sx = sy) then (
            Log.err (fun m ->
                m "RRequestVoteResponse %a != %a" Fmt.int64 sx Fmt.int64 sy) ;
            raise
            @@ Invalid_argument "Not correct entry to request vote response" ) ;
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
        if Quorum.satisified quorum then transition_to_leader t else C.return t
    )
  | `RRequestVoteResponse _, _ ->
      C.return t
  | `RAppendEntries (src, msg), _ -> (
      let send t res =
        C.append
        @@ `SendAppendEntriesResponse (src, {term= t.current_term; success= res})
      in
      match L.get_term t.log msg.prev_log_index with
      | _ when Int64.(msg.term < t.current_term) ->
          let+ () = send t (Error msg.prev_log_index) in
          C.return t
      | Error _ ->
          let+ () = send t (Error msg.prev_log_index) in
          C.return t
      | Ok log_term when Int64.(log_term <> msg.prev_log_term) ->
          let+ () = send t (Error msg.prev_log_index) in
          C.return t
      | Ok _ ->
          let log, log_ops =
            L.add_entries_remove_conflicts t.log
              ~start_index:Int64.(msg.prev_log_index + one)
              msg.entries
          in
          let t = {t with log} in
          let+ () = C.appendv @@ List.map ~f:log_op_to_action log_ops in
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
          C.return t )
  | `RAppendEntiresResponse (src, msg), Leader s
    when Int64.(msg.term = t.current_term) -> (
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
        let node_state = Leader {s with match_index; next_index} in
        let t = {t with node_state} in
        check_commit_index t
    | Error prev_log_index ->
        let next_index = Map.set s.next_index ~key:src ~data:prev_log_index in
        let+ () =
          C.append
          @@ `SendAppendEntries
               (src, make_append_entries t Int64.(prev_log_index + one))
        in
        C.return {t with node_state= Leader {s with next_index}} )
  | `RAppendEntiresResponse _, _ ->
    C.return t
  | `LogAddition rs, Leader s ->
      let+ t =
        List.fold rs ~init:(C.return t) ~f:(fun t command ->
            let+ t = t in
            let log, ops = L.add {term= t.current_term; command} t.log in
            let+ () = C.appendv @@ List.map ~f:log_op_to_action ops in
            C.return {t with log}
          )
      in
      let+ t = check_commit_index t in
      let highest_index = L.get_max_index t.log in
      let fold t_comp node_id =
        let+ t = t_comp in
        let next_index = Map.find_exn s.next_index node_id in
        if Int64.(highest_index >= next_index) then
          let+ () = C.append @@ `SendAppendEntries (node_id, make_append_entries t next_index)
          in C.return t
        else C.return t
      in
      List.fold t.config.other_nodes ~f:fold ~init:(C.return t)
  | `LogAddition _, _ ->
      C.return t

let advance t event : t * [>action] list = 
  let t, actions = C.run @@ advance_raw t event in
  let split ls f =
    let xs,ys = List.fold_left ls ~init:([],[]) ~f:(fun (xs, ys) v ->
        if f v 
        then (v::xs, ys)
        else (xs, v::ys) 
      )
    in List.rev xs, List.rev ys
  in
  let persistant, otherwise = split actions (fun v -> match v with
      | `PersistantChange _ -> false
      | _ -> true)
  in 
  t, (persistant @ otherwise)

let is_leader t = match t.node_state with Leader _ -> true | _ -> false

let create_node config log current_term =
  Log.info (fun m -> m "Creating new node with id %a" Fmt.int64 config.node_id) ;
  { config
  ; log
  ; current_term
  ; commit_index= Int64.zero
  ; node_state= Follower {heartbeat= config.election_timeout} }

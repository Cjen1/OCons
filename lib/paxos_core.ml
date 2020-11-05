open! Core
open! Types
open Ppx_log_async
open Types.MessageTypes
module L = Types.Wal.Log
module T = Types.Wal.Term
module U = Utils
module IdMap = Map.Make (Int)

let logger =
  let open Async_unix.Log in
  create ~level:`Info ~output:[] ~on_error:`Raise
    ~transform:(fun m -> Message.add_tags m [("src", "Paxos_core")])
    ()

(* R before means receiving *)
type event =
  [ `Tick
  | `RRequestVote of request_vote
  | `RRequestVoteResponse of request_vote_response
  | `RAppendEntries of append_entries
  | `RAppendEntiresResponse of append_entries_response
  | `Commands of command list ]

type persistant_change = [`Log of L.op | `Term of T.op]

type pre_sync_action =
  [ `PersistantChange of persistant_change
  | `SendRequestVote of node_id * request_vote
  | `SendAppendEntries of node_id * append_entries
  | `Unapplied of command list ]

type post_sync_action =
  [ `SendRequestVoteResponse of node_id * request_vote_response
  | `SendAppendEntriesResponse of node_id * append_entries_response
  | `CommitIndexUpdate of log_index ]

type do_sync = bool

type action_sequence = pre_sync_action list * do_sync * post_sync_action list

module Comp = struct
  type 'a t = 'a * action_sequence

  let return x : 'a t = (x, ([], false, []))
end

module CompRes = struct
  type ('a, 'b) t = ('a Comp.t, 'b) Result.t

  let bind : ('a, 'b) t -> ('a -> ('c, 'b) t) -> ('c, 'b) t =
   fun x f ->
    let ( let* ) x f = Result.bind x ~f in
    let* x, (pre, sync, post) = x in
    match f x with
    | Error e ->
        Error e
    | Ok (x, (pre', sync', post')) ->
        Ok (x, (pre @ pre', sync || sync', post @ post'))

  let return v = Ok (Comp.return v)

  let error v = Error v

  let append_pre action : (unit, 'b) t = Ok ((), ([action], false, []))

  let append_post action : (unit, 'b) t = Ok ((), ([], false, [action]))

  let appendv_pre av : (unit, 'b) t = Ok ((), (av, false, []))
end

let ( let+ ) x f = CompRes.(bind) x f

let ( let* ) x f = Result.bind x ~f

let log_op_to_action op : pre_sync_action = `PersistantChange (`Log op)

let term_op_to_action op : pre_sync_action = `PersistantChange (`Term op)

let pp_action f (x : [< pre_sync_action | post_sync_action]) =
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
  | `Unapplied cmds ->
      Fmt.pf f "Unapplied %s" ([%sexp_of: command list] cmds |> Sexp.to_string)

let pp_event f (e : event) =
  match e with
  | `Tick ->
      Fmt.pf f "Tick"
  | `RRequestVote _ ->
      Fmt.pf f "RRequestVote"
  | `RRequestVoteResponse _ ->
      Fmt.pf f "RRequestVoteResponse"
  | `RAppendEntries _ ->
      Fmt.pf f "RAppendEntries"
  | `RAppendEntiresResponse _ ->
      Fmt.pf f "RAppendEntiresResponse"
  | `Commands _ ->
      Fmt.pf f "Commands"

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
  ; current_term: term
  ; log: L.t
  ; commit_index: log_index
  ; node_state: node_state }
[@@deriving sexp_of]

let make_append_entries (t : t) next_index =
  let term = t.current_term in
  let prev_log_index = Int64.(next_index - one) in
  let prev_log_term = L.get_term_exn t.log prev_log_index in
  let entries, entries_length = L.entries_after_inc_size t.log next_index in
  let leader_commit = t.commit_index in
  { src= t.config.node_id
  ; term
  ; prev_log_index
  ; prev_log_term
  ; entries
  ; entries_length
  ; leader_commit }

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
          CompRes.append_post @@ `CommitIndexUpdate commit_index
        else CompRes.return ()
      in
      CompRes.return {t with commit_index}
  | _ ->
      CompRes.return t

let transition_to_leader t =
  match t.node_state with
  | Candidate s ->
      [%log.info logger "Transition to leader"] ;
      let entries =
        List.map s.entries ~f:(fun entry -> {entry with term= t.current_term})
      in
      (* If the node has the highest node_id then it will write its entire log back to disk with a higher term ... This feels like a bug but is in the protocol *)
      let log, actions =
        L.add_entries_remove_conflicts t.log ~start_index:s.start_index entries
      in
      let+ () = CompRes.appendv_pre @@ List.map ~f:log_op_to_action actions in
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
        CompRes.appendv_pre @@ List.fold t.config.other_nodes ~init:[] ~f:fold
      in
      CompRes.return t
  | _ ->
      CompRes.error
      @@ `Msg "Cannot transition to leader from states other than candidate"

let update_current_term term t =
  let current_term, ops = T.update_term t.current_term term in
  let actions = List.map ~f:term_op_to_action ops in
  let+ () = CompRes.appendv_pre actions in
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
  [%log.info logger "Transition to candidate" (updated_term : term)] ;
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
          ( node_id
          , { src= t.config.node_id
            ; term= t.current_term
            ; leader_commit= t.commit_index } )
        :: actions
      in
      List.fold t.config.other_nodes ~init:[] ~f:fold
    in
    let+ () = CompRes.appendv_pre actions in
    CompRes.return t

let transition_to_follower t =
  [%log.info logger "Transition to Follower"] ;
  CompRes.return {t with node_state= Follower {heartbeat= 0}}

let rec advance_raw t (event : event) : (t, 'b) CompRes.t =
  match (event, t.node_state) with
  | `Tick, Follower {heartbeat} when heartbeat >= t.config.election_timeout ->
      [%log.debug logger "Election timeout"] ; transition_to_candidate t
  | `Tick, Follower {heartbeat} ->
      let new_heartbeat = heartbeat + 1 in
      [%log.debug
        logger "Tick" ~pre:(heartbeat : int) ~post:(new_heartbeat : int)] ;
      CompRes.return {t with node_state= Follower {heartbeat= new_heartbeat}}
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
        CompRes.appendv_pre @@ List.fold t.config.other_nodes ~f:fold ~init:[]
      in
      CompRes.return {t with node_state= Leader {s with heartbeat= 0}}
  | `Tick, Leader ({heartbeat; _} as s) ->
      CompRes.return
        {t with node_state= Leader {s with heartbeat= heartbeat + 1}}
  | `Tick, _ ->
      CompRes.return t
  | (`RRequestVote {term; _} as event), _
  | (`RRequestVoteResponse {term; _} as event), _
  | (`RAppendEntries {term; _} as event), _
  | (`RAppendEntiresResponse {term; _} as event), _
    when Int.(t.current_term < term) ->
      let+ t = update_current_term term t in
      let+ t = transition_to_follower t in
      advance_raw t event
  | `RRequestVote msg, _ when Int.(msg.term < t.current_term) ->
      let+ () =
        CompRes.append_post
        @@ `SendRequestVoteResponse
             ( msg.src
             , { src= t.config.node_id
               ; term= t.current_term
               ; vote_granted= false
               ; entries= []
               ; start_index= Int64.(msg.leader_commit + one) } )
      in
      CompRes.return t
  | `RRequestVote msg, _ ->
      let+ () =
        CompRes.append_post
        @@
        let entries =
          L.entries_after_inc t.log Int64.(msg.leader_commit + one)
        in
        `SendRequestVoteResponse
          ( msg.src
          , { src= t.config.node_id
            ; term= t.current_term
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
  | `RRequestVoteResponse msg, Candidate s
    when Int.(msg.term = t.current_term) && msg.vote_granted -> (
    match U.Quorum.add msg.src s.quorum with
    | Error `AlreadyInList ->
        CompRes.return t
    | Ok (quorum : node_id U.Quorum.t) ->
        let merge x sx y sy =
          if not Int64.(sx = sy) then (
            [%log.error
              logger "Error while merging"
                ~start_x:(sx : log_index)
                ~start_y:(sy : log_index)] ;
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
  | `RAppendEntries msg, _ -> (
      let send t res =
        CompRes.append_post
        @@ `SendAppendEntriesResponse
             ( msg.src
             , {src= t.config.node_id; term= t.current_term; success= res} )
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
          let+ () =
            CompRes.appendv_pre @@ List.map ~f:log_op_to_action log_ops
          in
          let match_index = Int64.(msg.prev_log_index + msg.entries_length) in
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
  | `RAppendEntiresResponse msg, Leader s when Int.(msg.term = t.current_term)
    -> (
    match msg.success with
    | Ok match_index ->
        let updated_match_index =
          Int64.(max match_index @@ IdMap.find_exn s.match_index msg.src)
        in
        let match_index =
          Map.set s.match_index ~key:msg.src ~data:updated_match_index
        in
        let next_index =
          Map.set s.next_index ~key:msg.src
            ~data:Int64.(updated_match_index + one)
        in
        let node_state = Leader {s with match_index; next_index} in
        let t = {t with node_state} in
        check_commit_index t
    | Error prev_log_index ->
        let next_index =
          Map.set s.next_index ~key:msg.src ~data:prev_log_index
        in
        let+ () =
          CompRes.append_pre
          @@ `SendAppendEntries
               (msg.src, make_append_entries t Int64.(prev_log_index + one))
        in
        CompRes.return {t with node_state= Leader {s with next_index}} )
  | `RAppendEntiresResponse _, _ ->
      CompRes.return t
  | `Commands cs, Leader s ->
      let cs = List.filter cs ~f:(fun cmd -> not @@ L.id_in_log t.log cmd.id) in
      let log, ops = L.addv cs t.log t.current_term in
      let+ t =
        Ok
          ( {t with log}
          , (List.map ops ~f:(fun op -> log_op_to_action op), false, []) )
      in
      let+ t = check_commit_index t in
      let highest_index = L.get_max_index t.log in
      let fold t_comp node_id =
        let+ t = t_comp in
        let next_index = Map.find_exn s.next_index node_id in
        if Int64.(highest_index >= next_index) then
          let+ () =
            CompRes.append_pre
            @@ `SendAppendEntries (node_id, make_append_entries t next_index)
          in
          CompRes.return t
        else CompRes.return t
      in
      List.fold t.config.other_nodes ~f:fold ~init:(CompRes.return t)
  | `Commands cs, _ ->
      let+ () = CompRes.append_pre (`Unapplied cs) in
      CompRes.return t

let advance t event : (t, 'b) CompRes.t =
  [%log.debug logger "Advancing" ~event:(Fmt.str "%a" pp_event event)] ;
  let* t, (pre, sync, post) = advance_raw t event in
  let actions =
    if not @@ List.is_empty post then (pre, true, post) else (pre, sync, post)
  in
  let pre, do_sync, post = actions in
  [%log.debug
    logger "Returning actions:"
      ~pre:(Fmt.str "%a" (Fmt.list pp_action) pre : string)
      (do_sync : bool)
      ~post:(Fmt.str "%a" (Fmt.list pp_action) post : string)] ;
  Ok (t, actions)

let is_leader t =
  match t.node_state with Leader _ -> Some t.current_term | _ -> None

let get_log (t : t) = t.log

let get_term (t : t) = t.current_term

let create_node config log current_term =
  [%log.info
    logger "Creating new node" ~config:(config : config) (current_term : term)] ;
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

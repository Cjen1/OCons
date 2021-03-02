open! Core
open! Types
open Ppx_log_async
open Types.MessageTypes
module S = IStorage
module U = Utils
module IdMap = Map.Make (Int)

let logger =
  let open Async_unix.Log in
  create ~level:`Info ~output:[] ~on_error:`Raise
    ~transform:(fun m -> Message.add_tags m [("src", "Paxos_core")])
    ()

(* R means receiving *)
type event =
  [ `Tick
  | `Syncd of log_index
  | `RRequestVote of request_vote
  | `RRequestVoteResponse of request_vote_response
  | `RAppendEntries of append_entries
  | `RAppendEntiresResponse of append_entries_response
  | `Commands of command list ]

type action =
  [ `Unapplied of command list
  | `SendRequestVote of node_id * request_vote
  | `SendAppendEntries of node_id * append_entries
  | `SendRequestVoteResponse of node_id * request_vote_response
  | `SendAppendEntriesResponse of node_id * append_entries_response
  | `CommitIndexUpdate of log_index ]
[@@deriving sexp]

type actions = {acts: action list; nonblock_sync: bool}
[@@deriving sexp, accessors]

let empty = {acts= []; nonblock_sync= false}

module Comp = struct
  type 'a t = 'a * actions

  let return x : 'a t = (x, empty)
end

module CompRes = struct
  type ('a, 'b) t = ('a Comp.t, 'b) Result.t

  let bind : ('a, 'b) t -> ('a -> ('c, 'b) t) -> ('c, 'b) t =
   fun x f ->
    let ( let* ) x f = Result.bind x ~f in
    let* x, t = x in
    match f x with
    | Error e ->
        Error e
    | Ok (x, t') ->
        Ok
          ( x
          , { acts= t.acts @ t'.acts
            ; nonblock_sync= t.nonblock_sync || t'.nonblock_sync } )

  let return v = Ok (Comp.return v)

  let error v = Error v

  let append action : (unit, 'b) t = Ok ((), {empty with acts= [action]})

  let appendv acts : (unit, 'b) t = Ok ((), {empty with acts})

  let ( let+ ) x f = bind x f

  let list_fold ls ~init ~f : ('a, 'b) t =
    List.fold ls ~init:(return init) ~f:(fun acc v ->
        let+ acc = acc in
        f acc v)

  let list_iter ls ~f : (unit, 'a) t =
    list_fold ls ~init:() ~f:(fun () v -> f v)
end

let ( let+ ) x f = CompRes.(bind) x f

let ( let* ) x f = Result.bind x ~f

let pp_action f (x : action) =
  match x with
  | `Unapplied cmds ->
      Fmt.pf f "Unapplied %s" ([%sexp_of: command list] cmds |> Sexp.to_string)
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

let pp_event f (e : event) =
  match e with
  | `Tick ->
      Fmt.pf f "Tick"
  | `Syncd i ->
      Fmt.pf f "Sync'd up to %a" Fmt.int64 i
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
  { phase1quorum: int
  ; phase2quorum: int
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
  {config: config; store: S.t; commit_index: log_index; node_state: node_state}
[@@deriving sexp_of, accessors]

let check_commit_index t =
  match t.node_state with
  | Leader s ->
      let commit_index =
        Map.fold s.match_index ~init:[S.get_max_index t.store]
          ~f:(fun ~key:_ ~data acc -> data :: acc)
        |> List.sort ~compare:Int64.compare
        |> List.rev
        |> fun ls -> List.nth_exn ls t.config.phase2quorum
      in
      let+ () =
        if Int64.(commit_index <> t.commit_index) then
          CompRes.append @@ `CommitIndexUpdate commit_index
        else CompRes.return ()
      in
      CompRes.return {t with commit_index}
  | _ ->
      CompRes.return t

module ReplicationSM = struct
  (* We use the next_index to figure out what to send.
     When we receive a append_entries_response case switch on success:
      - Success -> Update match_index
      - Failure (error_point) -> update next_index to error_point
     In both cases try to send directly after

     When we try to send an append_entries message we send a window of
     the entries available, or nothing if nothing needs to be sent

     ==> In case of missing log entry a failure will be sent
  *)

  let recv_append_entries (t : t) (msg : append_entries) =
    (* If we fail we return the latest point an error occurred (that we can tell) *)
    let send t success =
      CompRes.append
      @@ `SendAppendEntriesResponse
           ( msg.src
           , {src= t.config.node_id; term= S.get_current_term t.store; success}
           )
    in
    match S.get_term t.store msg.prev_log_index with
    | _ when Int.(msg.term < S.get_current_term t.store) ->
        (* May want better error reporting here *)
        let+ () = send t (Error msg.prev_log_index) in
        CompRes.return t
    | Error _ ->
        (* The entries that got sent were greater than the entirety of the log *)
        let+ () = send t (Error (S.get_max_index t.store)) in
        CompRes.return t
    | Ok term when Int.(term <> msg.prev_log_term) ->
        let+ () = send t (Error msg.prev_log_index) in
        CompRes.return t
    | Ok _ ->
        let t =
          A.map store t
            ~f:
              (S.add_entries_remove_conflicts
                 ~start_index:Int64.(msg.prev_log_index + one)
                 ~entries:msg.entries)
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
        let+ () =
          if Int64.(t.commit_index <> commit_index) then
            CompRes.append @@ `CommitIndexUpdate commit_index
          else CompRes.return ()
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
        CompRes.return t

  let send_append_entries ?(force = false) (t : t) dst =
    match t.node_state with
    | Leader s -> (
        let next_index = Map.find_exn s.next_index dst in
        let prev_log_index = Int64.(next_index - one) in
        let entries, entries_length =
          S.entries_after_inc_size t.store next_index
        in
        match entries with
        | [] when not force ->
            CompRes.return ()
        | _ ->
            let term = S.get_current_term t.store in
            let prev_log_term = S.get_term_exn t.store prev_log_index in
            let leader_commit = t.commit_index in
            let res =
              { src= t.config.node_id
              ; term
              ; prev_log_index
              ; prev_log_term
              ; entries
              ; entries_length
              ; leader_commit }
            in
            CompRes.append (`SendAppendEntries (dst, res)) )
    | _ ->
        CompRes.return ()

  let recv_append_entries_response (t : t) msg =
    let+ t =
      match (t.node_state, msg.success) with
      | Leader s, Ok remote_match_index
        when Int.(msg.term = S.get_current_term t.store) ->
          let match_index =
            Map.update s.match_index msg.src ~f:(function
              | None ->
                  remote_match_index
              | Some mi ->
                  Int64.max mi remote_match_index)
          in
          let updated_match_index = Map.find_exn match_index msg.src in
          let next_index =
            Map.update s.next_index msg.src ~f:(function
              | None ->
                  Int64.(updated_match_index + one)
              | Some ni ->
                  Int64.(max ni (updated_match_index + one)))
          in
          let node_state = Leader {s with match_index; next_index} in
          CompRes.return {t with node_state}
      | Leader s, Error prev_log_index ->
          [%log.error
            logger "RappendEntiresResponse: not valid append entries"
              (prev_log_index : log_index)
              ~next_index:(Map.find_exn s.next_index msg.src : log_index)] ;
          let next_index =
            Map.set s.next_index ~key:msg.src ~data:prev_log_index
          in
          let node_state = Leader {s with next_index} in
          CompRes.return {t with node_state}
      | _ ->
          CompRes.return t
    in
    (* Send any remaining entries / Fixed entries *)
    let+ () = send_append_entries t msg.src in
    CompRes.return t

  let recv_syncd (t : t) index =
    match t.node_state with
    | Leader s ->
        let match_index =
          Map.update s.match_index t.config.node_id ~f:(function
            | None ->
                index
            | Some mi ->
                Int64.max mi index)
        in
        let node_state = Leader {s with match_index} in
        CompRes.return {t with node_state}
    | _ ->
        CompRes.return t
end

let transition_to_leader t =
  match t.node_state with
  | Candidate s ->
      [%log.info logger "Transition to leader"] ;
      let current_term = S.get_current_term t.store in
      let entries =
        List.map s.entries ~f:(fun entry -> {entry with term= current_term})
      in
      let t =
        A.map store t
          ~f:
            (S.add_entries_remove_conflicts ~start_index:s.start_index ~entries)
      in
      let match_index, next_index =
        List.fold (t.config.node_id :: t.config.other_nodes)
          ~init:(IdMap.empty, IdMap.empty) ~f:(fun (mi, ni) id ->
            let mi = Map.set mi ~key:id ~data:Int64.zero in
            let ni = Map.set ni ~key:id ~data:Int64.(t.commit_index + one) in
            (mi, ni))
      in
      let node_state = Leader {match_index; next_index; heartbeat= 0} in
      let t = {t with node_state} in
      let iter node_id =
        ReplicationSM.send_append_entries ~force:true t node_id
      in
      let+ t = check_commit_index t in
      let+ () = CompRes.list_iter t.config.other_nodes ~f:iter in
      CompRes.return t
  | _ ->
      CompRes.error
      @@ `Msg "Cannot transition to leader from states other than candidate"

let transition_to_candidate t =
  let updated_term : term =
    let current_term = S.get_current_term t.store in
    let id_in_current_epoch =
      current_term - (current_term % t.config.num_nodes) + t.config.node_id
    in
    if id_in_current_epoch <= current_term then
      id_in_current_epoch + t.config.num_nodes
    else id_in_current_epoch
  in
  [%log.info logger "Transition to candidate" (updated_term : term)] ;
  let quorum = U.Quorum.empty t.config.phase1quorum Int.equal in
  let* quorum =
    match U.Quorum.add t.config.node_id quorum with
    | Ok quorum ->
        Ok quorum
    | Error _ ->
        CompRes.error @@ `Msg "Tried to add element to empty quorum and failed"
  in
  let entries = S.entries_after_inc t.store Int64.(t.commit_index + one) in
  let node_state =
    Candidate {quorum; entries; start_index= Int64.(t.commit_index + one)}
  in
  let t = {t with node_state} in
  let t = A.map store t ~f:(S.update_term ~term:updated_term) in
  if U.Quorum.satisified quorum then transition_to_leader t
  else
    let actions =
      List.map t.config.other_nodes ~f:(fun node_id ->
          `SendRequestVote
            ( node_id
            , { src= t.config.node_id
              ; term= S.get_current_term t.store
              ; leader_commit= t.commit_index } ))
    in
    let+ () = CompRes.appendv actions in
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
      let+ () =
        CompRes.list_iter t.config.other_nodes
          ~f:(ReplicationSM.send_append_entries ~force:true t)
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
    when Int.(S.get_current_term t.store < term) ->
      let t = A.map store t ~f:(S.update_term ~term) in
      let+ t = transition_to_follower t in
      advance_raw t event
  | `RRequestVote msg, _ when Int.(msg.term < S.get_current_term t.store) ->
      let+ () =
        CompRes.append
        @@ `SendRequestVoteResponse
             ( msg.src
             , { src= t.config.node_id
               ; term= S.get_current_term t.store
               ; vote_granted= false
               ; entries= []
               ; start_index= Int64.(msg.leader_commit + one) } )
      in
      CompRes.return t
  | `RRequestVote msg, _ ->
      let+ () =
        CompRes.append
        @@
        let entries =
          S.entries_after_inc t.store Int64.(msg.leader_commit + one)
        in
        `SendRequestVoteResponse
          ( msg.src
          , { src= t.config.node_id
            ; term= S.get_current_term t.store
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
    when Int.(msg.term = S.get_current_term t.store) && msg.vote_granted -> (
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
  | `RAppendEntries msg, _ ->
      ReplicationSM.recv_append_entries t msg
  | `RAppendEntiresResponse msg, _ ->
      let+ t = ReplicationSM.recv_append_entries_response t msg in
      check_commit_index t
  | `Commands cs, Leader _ ->
      let cmds =
        List.filter cs ~f:(fun cmd -> not @@ S.mem_id t.store cmd.id)
      in
      let t =
        A.map store t ~f:(S.add_cmds ~cmds ~term:(S.get_current_term t.store))
      in
      let+ t = check_commit_index t in
      let+ () =
        CompRes.list_iter t.config.other_nodes
          ~f:(ReplicationSM.send_append_entries t)
      in
      CompRes.return t
  | `Commands cs, _ ->
      let+ () = CompRes.append (`Unapplied cs) in
      CompRes.return t
  | `Syncd index, _ ->
      let+ t = ReplicationSM.recv_syncd t index in
      check_commit_index t

let is_leader (t : t) =
  match t.node_state with
  | Leader _ ->
      Some (S.get_current_term t.store)
  | _ ->
      None

let get_max_index (t : t) = S.get_max_index t.store

let get_term (t : t) = S.get_current_term t.store

let pop_store (t : t) = ({t with store= S.reset_ops t.store}, t.store)

let advance t event : (t, 'b) CompRes.t =
  [%log.debug logger "Advancing" ~event:(Fmt.str "%a" pp_event event)] ;
  let* t, actions = advance_raw t event in
  [%log.debug logger "Returning actions:" (actions : actions)] ;
  let is_leader = Option.is_some (is_leader t) in
  Ok (t, {actions with nonblock_sync= actions.nonblock_sync || is_leader})

let create_node config store =
  [%log.info
    logger "Creating new node"
      ~config:(config : config)
      (S.get_current_term store : term)] ;
  { config
  ; store
  ; commit_index= Int64.zero
  ; node_state= Follower {heartbeat= config.election_timeout} }

module Test = struct
  module Comp = Comp
  module CompRes = CompRes

  let transition_to_leader = transition_to_leader

  let transition_to_candidate = transition_to_candidate

  let transition_to_follower = transition_to_follower

  let get_node_state t = t.node_state

  let get_commit_index t = t.commit_index

  let get_store t = t.store
end

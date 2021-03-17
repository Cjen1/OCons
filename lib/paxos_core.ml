open! Core
open! Types
open Accessor.O
module A = Accessor
open Ppx_log_async
open Types.MessageTypes
module S = IStorage
module U = Utils
module IdMap = Map.Make (Int)
open Core_profiler_disabled.Std

let logger =
  let open Async_unix.Log in
  create ~level:`Info ~output:[] ~on_error:`Raise
    ~transform:(fun m -> Message.add_tags m [("src", "Paxos_core")])
    ()

let io_logger =
  let open Async_unix.Log in
  create ~level:`Info ~output:[] ~on_error:`Raise
    ~transform:(fun m -> Message.add_tags m [("src", "Paxos_core_io")])
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
[@@deriving sexp]

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
  | Follower of {timeout: int}
  | Candidate of
      { quorum: node_id U.Quorum.t
      ; entries: log_entry list
      ; start_index: log_index
      ; timeout: int }
  | Leader of
      { match_index: log_index IdMap.t
      ; next_index: log_index IdMap.t
      ; heartbeat: int }
[@@deriving sexp_of, accessors]

let pp_node_state f (x : node_state) =
  match x with
  | Follower {timeout} ->
      Fmt.pf f "Follower(%d)" timeout
  | Candidate _ ->
      Fmt.pf f "Candidate"
  | Leader _ ->
      Fmt.pf f "Leader"

type t =
  {config: config; store: S.t; commit_index: log_index; node_state: node_state}
[@@deriving sexp_of, accessors]

module State = struct
  type state = {t: t; a: actions} [@@deriving accessors]

  let empty t = {t; a= empty}

  type 'a sm = state -> 'a * state

  let return x : 'a sm = fun state -> (x, state)

  let eval (x : 'a sm) (s : state) : 'a * state = x s

  let bind : 'a sm -> f:('a -> 'b sm) -> 'b sm =
   fun p ~f:k s0 ->
    let v, s1 = eval p s0 in
    eval (k v) s1

  module Let_syntax = struct
    module Let_syntax = struct
      let bind = bind
    end
  end

  open Let_syntax

  let get_t () : t sm = fun state -> (state.t, state)

  let put_t t : unit sm = fun state -> ((), {state with t})

  let map_t f : unit sm =
    let%bind t = get_t () in
    put_t (f t)

  let append action : unit sm =
   fun state -> ((), A.map (a @> acts) ~f:(List.cons action) state)

  let appendv actions state =
    ((), A.map (a @> acts) ~f:(List.append actions) state)
end

module StateR = struct
  type ('a, 'b) t = ('a, 'b) Result.t State.sm

  let return v : ('a, 'b) t = State.return @@ Result.return v

  let fail v : ('a, 'b) t = State.return @@ Result.fail v

  let bind : ('a, 'b) t -> ('a -> ('c, 'b) t) -> ('c, 'b) t =
   fun v f ->
    let%bind.State v = v in
    match v with Error _ as v -> State.return v | Ok v -> f v

  module Let_syntax = struct
    module Let_syntax = struct
      let bind v ~f = bind v f
    end
  end

  open Let_syntax

  let map_statef f v : ('a, 'b) t =
    let%bind.State v = f v in
    return v

  let append = map_statef State.append

  let appendv = map_statef State.appendv

  let get_t = map_statef State.get_t

  let put_t = map_statef State.put_t

  let map_t = map_statef State.map_t

  let eval (v : ('a, 'b) t) (state : State.state) :
      ('a * State.state, 'b) Result.t =
    match State.eval v state with
    | (Error _ as e), _ ->
        e
    | Ok v, state ->
        Ok (v, state)

  let list_fold ls ~init ~f : ('a, 'b) t =
    List.fold_left ls ~init:(return init) ~f:(fun prev v ->
        let%bind prev = prev in
        f prev v )

  let list_iter ls ~f : (unit, 'b) t =
    list_fold ls ~init:() ~f:(fun () v -> f v)
end

open StateR.Let_syntax

let check_commit_index () =
  let%bind t = StateR.get_t () in
  match t.node_state with
  | Leader s ->
      let commit_index =
        Map.fold s.match_index ~init:[S.get_max_index t.store]
          ~f:(fun ~key:_ ~data acc -> data :: acc)
        |> List.sort ~compare:Int64.compare
        |> List.rev
        |> fun ls -> List.nth_exn ls t.config.phase2quorum
      in
      let%bind () =
        if Int64.(commit_index <> t.commit_index) then
          StateR.append @@ `CommitIndexUpdate commit_index
        else StateR.return ()
      in
      StateR.put_t {t with commit_index}
  | _ ->
      StateR.return ()

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

  let probe_send_size =
    Probe.create ~name:"send_entries_length" ~units:Profiler_units.Int

  let send_append_entries ?(force = false) dst =
    let%bind t = StateR.get_t () in
    match t.node_state with
    | Leader s -> (
        let next_index = Map.find_exn s.next_index dst in
        let prev_log_index = Int64.(next_index - one) in
        let entries = S.entries_after_inc t.store next_index in
        let entries_length = List.length entries |> Int64.of_int in
        let%bind () =
          StateR.map_t
          @@ A.map
               (node_state @> Leader.next_index)
               ~f:(Map.set ~key:dst ~data:Int64.(next_index + entries_length))
        in
        let%bind t = StateR.get_t () in
        Probe.record probe_send_size (Int64.to_int_exn entries_length) ;
        [%log.debug
          logger (dst : int) (next_index : int64) (entries_length : int64)] ;
        match entries with
        | [] when not force ->
            StateR.return ()
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
            StateR.append (`SendAppendEntries (dst, res)) )
    | _ ->
        StateR.return ()

  let probe_recv_size =
    Probe.create ~name:"recv_entries_size" ~units:Profiler_units.Int

  let recv_append_entries (msg : append_entries) =
    let%bind t = StateR.get_t () in
    (* If we fail we return the latest point an error occurred (that we can tell) *)
    let send t success =
      StateR.append
      @@ `SendAppendEntriesResponse
           ( msg.src
           , {src= t.config.node_id; term= S.get_current_term t.store; success}
           )
    in
    match S.get_term t.store msg.prev_log_index with
    | _ when Int.(msg.term < S.get_current_term t.store) ->
        (* May want better error reporting here *)
        let%bind () = send t (Error msg.prev_log_index) in
        StateR.put_t t
    | Error _ ->
        (* The entries that got sent were greater than the entirety of the log *)
        let%bind () = send t (Error (S.get_max_index t.store)) in
        StateR.put_t t
    | Ok term when Int.(term <> msg.prev_log_term) ->
        let%bind () = send t (Error msg.prev_log_index) in
        StateR.put_t t
    | Ok _ ->
        Probe.record probe_recv_size (Int64.to_int_exn msg.entries_length) ;
        let t =
          A.map store t
            ~f:
              (S.add_entries_remove_conflicts
                 ~start_index:Int64.(msg.prev_log_index + one)
                 ~entries:msg.entries )
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
        let%bind () =
          if Int64.(t.commit_index <> commit_index) then
            StateR.append @@ `CommitIndexUpdate commit_index
          else StateR.return ()
        in
        let t = {t with commit_index} in
        let t =
          match t.node_state with
          | Follower _s ->
              {t with node_state= Follower {timeout= 0}}
          | _ ->
              t
        in
        let%bind () = send t (Ok match_index) in
        StateR.put_t t

  let recv_append_entries_response msg =
    let%bind t = StateR.get_t () in
    let%bind () =
      match (t.node_state, msg.success) with
      | Leader s, Ok remote_match_index
        when Int.(msg.term = S.get_current_term t.store) ->
          let match_index =
            Map.update s.match_index msg.src ~f:(function
              | None ->
                  remote_match_index
              | Some mi ->
                  Int64.max mi remote_match_index )
          in
          let updated_match_index = Map.find_exn match_index msg.src in
          let next_index =
            Map.update s.next_index msg.src ~f:(function
              | None ->
                  Int64.(updated_match_index + one)
              | Some ni ->
                  Int64.(max ni (updated_match_index + one)) )
          in
          let node_state = Leader {s with match_index; next_index} in
          StateR.map_t (fun t -> {t with node_state})
      | Leader s, Error prev_log_index ->
          [%log.error
            logger "RappendEntiresResponse: not valid append entries"
              (prev_log_index : log_index)
              ~next_index:(Map.find_exn s.next_index msg.src : log_index)] ;
          let next_index =
            Map.set s.next_index ~key:msg.src ~data:prev_log_index
          in
          let node_state = Leader {s with next_index} in
          StateR.map_t (fun t -> {t with node_state})
      | _ ->
          StateR.put_t t
    in
    (* Send any remaining entries / Fixed entries *)
    send_append_entries msg.src
end

let transition_to_leader () =
  let%bind t = StateR.get_t () in
  match t.node_state with
  | Candidate s ->
      [%log.info logger "Transition to leader"] ;
      let current_term = S.get_current_term t.store in
      let entries =
        List.map s.entries ~f:(fun entry -> {entry with term= current_term})
      in
      let%bind () =
        StateR.map_t
        @@ A.map store
             ~f:
               (S.add_entries_remove_conflicts ~start_index:s.start_index
                  ~entries )
      in
      let match_index, next_index =
        List.fold (t.config.node_id :: t.config.other_nodes)
          ~init:(IdMap.empty, IdMap.empty) ~f:(fun (mi, ni) id ->
            let mi = Map.set mi ~key:id ~data:Int64.zero in
            let ni = Map.set ni ~key:id ~data:Int64.(t.commit_index + one) in
            (mi, ni) )
      in
      let%bind () =
        StateR.map_t
          (A.set node_state
             ~to_:(Leader {match_index; next_index; heartbeat= 0}) )
      in
      let iter node_id =
        ReplicationSM.send_append_entries ~force:true node_id
      in
      let%bind () = check_commit_index () in
      StateR.list_iter t.config.other_nodes ~f:iter
  | _ ->
      StateR.fail
      @@ `Msg "Cannot transition to leader from states other than candidate"

let transition_to_candidate () =
  let%bind t = StateR.get_t () in
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
  let%bind.StateR quorum =
    match U.Quorum.add t.config.node_id quorum with
    | Ok quorum ->
        StateR.return quorum
    | Error _ ->
        StateR.fail @@ `Msg "Tried to add element to empty quorum and failed"
  in
  let entries = S.entries_after_inc t.store Int64.(t.commit_index + one) in
  let%bind () =
    StateR.map_t
    @@ A.set node_state
         ~to_:
           (Candidate
              { quorum
              ; entries
              ; start_index= Int64.(t.commit_index + one)
              ; timeout= 0 } )
  in
  let%bind () =
    StateR.map_t @@ A.map store ~f:(S.update_term ~term:updated_term)
  in
  let%bind t = StateR.get_t () in
  match U.Quorum.satisified quorum with
  | true ->
      transition_to_leader ()
  | false ->
      let actions =
        List.map t.config.other_nodes ~f:(fun node_id ->
            `SendRequestVote
              ( node_id
              , { src= t.config.node_id
                ; term= S.get_current_term t.store
                ; leader_commit= t.commit_index } ) )
      in
      let%bind () = StateR.appendv actions in
      StateR.return ()

let transition_to_follower () =
  [%log.info logger "Transition to Follower"] ;
  StateR.map_t @@ A.set node_state ~to_:(Follower {timeout= 0})

let recv_syncd index =
  let%bind t = StateR.get_t () in
  match t.node_state with
  | Leader s ->
      let match_index =
        Map.update s.match_index t.config.node_id ~f:(function
          | None ->
              index
          | Some mi ->
              Int64.max mi index )
      in
      StateR.map_t @@ A.set (node_state @> Leader.match_index) ~to_:match_index
  | _ ->
      StateR.return ()

let command_size_probe =
  Probe.create ~name:"cs_batch_size" ~units:Profiler_units.Int

let rec advance_raw (event : event) : (unit, 'b) StateR.t =
  let%bind t = StateR.get_t () in
  match (event, t.node_state) with
  | `Tick, Follower {timeout} when timeout >= t.config.election_timeout ->
      [%log.info logger "Election timeout"] ; transition_to_candidate ()
  | `Tick, Follower {timeout} ->
      let new_timeout = timeout + 1 in
      [%log.debug logger "Tick" ~pre:(timeout : int) ~post:(new_timeout : int)] ;
      StateR.map_t @@ A.set (node_state @> Follower.timeout) ~to_:new_timeout
  | `Tick, Candidate {timeout; _} when timeout >= t.config.election_timeout ->
      [%log.info logger "Election timeout"] ; transition_to_candidate ()
  | `Tick, Candidate state ->
      let new_timeout = state.timeout + 1 in
      [%log.debug
        logger "Tick" ~pre:(state.timeout : int) ~post:(new_timeout : int)] ;
      StateR.map_t @@ A.set (node_state @> Candidate.timeout) ~to_:new_timeout
  | `Tick, Leader {heartbeat; _} when heartbeat > 0 ->
      let%bind () =
        StateR.list_iter t.config.other_nodes
          ~f:(ReplicationSM.send_append_entries ~force:true)
      in
      StateR.map_t @@ A.set (node_state @> Leader.heartbeat) ~to_:0
  | `Tick, Leader _ ->
      StateR.map_t @@ A.map (node_state @> Leader.heartbeat) ~f:(fun h -> h + 1)
  | (`RRequestVote {term; _} as event), _
  | (`RRequestVoteResponse {term; _} as event), _
  | (`RAppendEntries {term; _} as event), _
  | (`RAppendEntiresResponse {term; _} as event), _
    when Int.(S.get_current_term t.store < term) ->
      let%bind () = StateR.map_t @@ A.map store ~f:(S.update_term ~term) in
      let%bind () = transition_to_follower () in
      advance_raw event
  | `RRequestVote msg, _ when Int.(msg.term < S.get_current_term t.store) ->
      StateR.append
      @@ `SendRequestVoteResponse
           ( msg.src
           , { src= t.config.node_id
             ; term= S.get_current_term t.store
             ; vote_granted= false
             ; entries= []
             ; start_index= Int64.(msg.leader_commit + one) } )
  | `RRequestVote msg, _ ->
      let%bind () =
        StateR.append
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
      StateR.map_t @@ A.set (node_state @> Follower.timeout) ~to_:0
  | `RRequestVoteResponse msg, Candidate s
    when Int.(msg.term = S.get_current_term t.store) && msg.vote_granted -> (
    match U.Quorum.add msg.src s.quorum with
    | Error `AlreadyInList ->
        StateR.return ()
    | Ok (quorum : node_id U.Quorum.t) ->
        let merge x sx y sy =
          if not Int64.(sx = sy) then (
            [%log.error
              logger "Error while merging"
                ~start_x:(sx : log_index)
                ~start_y:(sy : log_index)] ;
            StateR.fail @@ `Msg "Can't merge entries not starting at same point"
            )
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
            loop [] (x, y) |> List.rev |> StateR.return
        in
        let%bind entries =
          merge s.entries s.start_index msg.entries msg.start_index
        in
        let%bind () =
          StateR.map_t
          @@ A.set node_state ~to_:(Candidate {s with quorum; entries})
        in
        if U.Quorum.satisified quorum then transition_to_leader ()
        else StateR.return () )
  | `RRequestVoteResponse _, _ ->
      StateR.return ()
  | `RAppendEntries msg, _ ->
      ReplicationSM.recv_append_entries msg
  | `RAppendEntiresResponse msg, _ ->
      let%bind () = ReplicationSM.recv_append_entries_response msg in
      check_commit_index ()
  | `Commands cs, Leader _ ->
      let cmds =
        List.filter cs ~f:(fun cmd -> not @@ S.mem_id t.store cmd.id)
      in
      Probe.record command_size_probe (List.length cmds) ;
      let%bind () =
        StateR.map_t
        @@ A.map store ~f:(S.add_cmds ~cmds ~term:(S.get_current_term t.store))
      in
      let%bind () = check_commit_index () in
      StateR.list_iter t.config.other_nodes ~f:ReplicationSM.send_append_entries
  | `Commands cs, _ ->
      StateR.append (`Unapplied cs)
  | `Syncd index, _ ->
      let%bind () = recv_syncd index in
      check_commit_index ()

let is_leader (t : t) =
  match t.node_state with
  | Leader _ ->
      Some (S.get_current_term t.store)
  | _ ->
      None

let get_max_index (t : t) = S.get_max_index t.store

let get_term (t : t) = S.get_current_term t.store

let pop_store (t : t) = ({t with store= S.reset_ops t.store}, t.store)

let advance t event =
  [%log.debug io_logger "Entry" (event : event)] ;
  let prog = advance_raw event in
  let%bind.Result (), State.{t; a= actions} =
    StateR.eval prog (State.empty t)
  in
  let is_leader = Option.is_some (is_leader t) in
  let actions =
    {actions with nonblock_sync= actions.nonblock_sync || is_leader}
  in
  [%log.debug io_logger "Exit" (actions : actions)] ;
  Ok (t, actions)

let create_node config store =
  [%log.info logger "Creating new node" ~config:(config : config)] ;
  { config
  ; store
  ; commit_index= Int64.zero
  ; node_state= Follower {timeout= config.election_timeout} }

module Test = struct
  module State = State
  module StateR = StateR

  let transition_to_leader = transition_to_leader

  let transition_to_candidate = transition_to_candidate

  let transition_to_follower = transition_to_follower

  let get_node_state t = t.node_state

  let get_commit_index t = t.commit_index

  let get_store t = t.store
end

open! Core
open Accessor.O
module A = Accessor
open Ppx_log_async
open! Ocons_core
open! Types
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

let log_logger =
  let open Async_unix.Log in
  create ~level:`Info ~output:[] ~on_error:`Raise
    ~transform:(fun m -> Message.add_tags m [("src", "Paxos_core_log")])
    ()

type config =
  { phase1quorum: int
  ; phase2quorum: int
  ; other_nodes: node_id list
  ; num_nodes: int
  ; node_id: node_id
  ; election_timeout: int }
[@@deriving sexp]

let make_config ~node_id ~node_list ~election_timeout =
  let length = List.length node_list in
  let phase1quorum = (length + 1) / 2 in
  let phase2quorum = (length + 1) / 2 in
  let other_nodes =
    List.filter node_list ~f:(fun id -> not @@ Int.equal node_id id)
  in
  { phase1quorum
  ; phase2quorum
  ; other_nodes
  ; num_nodes= length
  ; node_id
  ; election_timeout }

let sexp_of_entries entries =
  List.map entries ~f:(fun v ->
      [%message (v.command.op : Types.sm_op) (v.term : term)] )
  |> [%sexp_of: Sexp.t list]

module MessageTypes = struct
  type request_vote = {src: node_id; term: term; leader_commit: log_index}
  [@@deriving bin_io, sexp]

  type request_vote_response =
    { src: node_id
    ; term: term
    ; vote_granted: bool
    ; entries: log_entry list
    ; start_index: log_index }
  [@@deriving bin_io, sexp]

  type append_entries =
    { src: node_id
    ; term: term
    ; prev_log_index: log_index
    ; prev_log_term: term
    ; entries: log_entry list
    ; entries_length: log_index
    ; leader_commit: log_index }
  [@@deriving bin_io, sexp]

  (* success is either the highest replicated term (match index) or prev_log_index *)
  type append_entries_response =
    {src: node_id; term: term; success: (log_index, log_index) Result.t}
  [@@deriving bin_io, sexp]
end

module Make (S : Immutable_store_intf.S) = struct
  open MessageTypes
  module Store = S

  type store = Store.t

  type nonrec config = config [@@deriving sexp_of]

  type message =
    | RequestVote of request_vote
    | RequestVoteResponse of request_vote_response
    | AppendEntries of append_entries
    | AppendEntriesResponse of append_entries_response
  [@@deriving sexp_of, bin_io]

  type event =
    [`Tick | `Syncd of log_index | `Recv of message | `Commands of command list]
  [@@deriving sexp_of, bin_io]

  type action =
    [ `Unapplied of command list
    | `Send of node_id * message
    | `CommitIndexUpdate of log_index ]
  [@@deriving sexp_of, bin_io]

  let send d m = `Send (d, m)

  type actions = {acts: action list; nonblock_sync: bool}
  [@@deriving sexp_of, accessors]

  let empty = {acts= []; nonblock_sync= false}

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

  type t =
    {config: config; store: S.t; commit_index: log_index; node_state: node_state}
  [@@deriving sexp_of, accessors]

  module State = struct
    type state = {t: t; a: actions} [@@deriving sexp_of, accessors]

    let empty t = {t; a= empty}

    type 'a sm = state -> 'a * state [@@deriving sexp_of]

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
    type ('a, 'b) t = ('a, 'b) Result.t State.sm [@@deriving sexp_of]

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

    let get_a acc =
      let%bind t = get_t () in
      return (A.get acc t)

    (*
    let put_t = map_statef State.put_t
       *)

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

  let update_commit_index new_commit_index =
    let%bind old_commit_index = StateR.get_a commit_index in
    if Int64.(old_commit_index <> new_commit_index) then
      let%bind () = StateR.map_t @@ A.set commit_index ~to_:new_commit_index in
      StateR.append @@ `CommitIndexUpdate new_commit_index
    else StateR.return ()

  let check_commit_index () =
    match%bind StateR.get_a node_state with
    | Leader s ->
        let%bind current_store = StateR.get_a store in
        let%bind config = StateR.get_a config in
        let calculated_commit_index =
          Map.fold s.match_index ~init:[S.get_max_index current_store]
            ~f:(fun ~key:_ ~data acc -> data :: acc)
          |> List.sort ~compare:Int64.compare
          |> List.rev
          |> fun ls -> List.nth_exn ls config.phase2quorum
        in
        let%bind new_commit_index =
          let%bind prev_commit_index = StateR.get_a commit_index in
          Int64.max calculated_commit_index prev_commit_index |> StateR.return
        in
        update_commit_index new_commit_index
    | _ ->
        StateR.return ()

  (* Takes two lists of entries lowest index first
       iterates through the lists until there is a conflict
       at which point it returns the conflict index and the entries to add
  *)
  let rec merge_y_into_x idx :
      log_entry list * log_entry list -> int64 option * log_entry list =
    function
    | [], [] ->
        (None, [])
    | _ :: _, [] ->
        (Some idx, [])
    | [], ys ->
        (None, ys)
    | x :: _, (y :: _ as ys) when not @@ [%compare.equal: term] x.term y.term ->
        [%log.debug
          logger "Mismatch while merging" (x : log_entry) (y : log_entry)] ;
        (Some idx, ys)
    | _ :: xs, _ :: ys ->
        merge_y_into_x Int64.(succ idx) (xs, ys)

  let add_entries_remove_conflicts ~start_index new_entries =
    let%bind t = StateR.get_t () in
    let relevant_entries = S.entries_after_inc t.store start_index in
    (* entries_to_add is in oldest first order *)
    let removeGEQ_o, entries_to_add =
      merge_y_into_x start_index
        (List.rev relevant_entries, List.rev new_entries)
    in
    [%log.debug
      logger "add_entries_remove_conflicts merge_y_into_x result"
        (removeGEQ_o : log_index option)
        ~adding:(sexp_of_entries entries_to_add : Sexp.t)] ;
    let%bind () =
      StateR.map_t
      @@ A.map store ~f:(fun s ->
             match removeGEQ_o with
             | Some i ->
                 S.remove_geq s ~index:i
             | None ->
                 s )
    in
    let%bind () =
      StateR.map_t
      @@ A.map store ~f:(fun s ->
             List.fold_left entries_to_add ~init:s ~f:(fun s entry ->
                 S.add_entry s ~entry ) )
    in
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
          [%log.debug
            io_logger "Sending append entries"
              (next_index : log_index)
              (prev_log_index : log_index)
              (dst : node_id)] ;
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
            logger "send_append_entries"
              (dst : int)
              (prev_log_index : int64)
              (entries_length : int64)] ;
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
              StateR.append @@ send dst @@ AppendEntries res )
      | _ ->
          StateR.return ()

    let probe_recv_size =
      Probe.create ~name:"recv_entries_size" ~units:Profiler_units.Int

    let recv_append_entries (msg : append_entries) =
      (* If we fail we return the latest point an error occurred (that we can tell) *)
      let send success =
        let%bind t = StateR.get_t () in
        StateR.append @@ send msg.src
        @@ AppendEntriesResponse
             {src= t.config.node_id; term= S.get_current_term t.store; success}
      in
      let%bind prev_store = StateR.get_a store in
      match S.get_term prev_store msg.prev_log_index with
      | _ when Int.(msg.term < S.get_current_term prev_store) ->
          [%log.debug
            logger "Received message from old leader"
              (msg.term : term)
              (msg.src : node_id)] ;
          (* May want better error reporting here *)
          send (Error msg.prev_log_index)
      | Error _ ->
          (* The entries that got sent were greater than the entirety of the log 
           * So the error is the missing entry one higher than the highest index *)
          [%log.debug
            logger "Entries received were larger than entirety of log"
              (msg.prev_log_index : log_index)
              (S.get_max_index prev_store : log_index)] ;
          send (Error Int64.(S.get_max_index prev_store + one))
      | Ok term when Int.(term <> msg.prev_log_term) ->
          [%log.debug
            logger "Log divergence, difference terms for same entry"
              (term : term)
              (msg.prev_log_term : term)] ;
          send (Error msg.prev_log_index)
      | Ok _ ->
          Probe.record probe_recv_size (Int64.to_int_exn msg.entries_length) ;
          let%bind () =
            add_entries_remove_conflicts
              ~start_index:Int64.(msg.prev_log_index + one)
              msg.entries
          in
          let%bind () =
            StateR.map_t @@ A.set (node_state @> Follower.timeout) ~to_:0
          in
          let match_index = Int64.(msg.prev_log_index + msg.entries_length) in
          let%bind prev_commit_index = StateR.get_a commit_index in
          let new_commit_index =
            let last_entry =
              match List.hd msg.entries with
              | None ->
                  msg.prev_log_index
              | Some _ ->
                  match_index
            in
            Int64.(max prev_commit_index (min msg.leader_commit last_entry))
          in
          let%bind () = update_commit_index new_commit_index in
          let%bind () = send (Ok match_index) in
          StateR.return ()

    let recv_append_entries_response msg =
      let%bind () =
        let%bind prev_node_state = StateR.get_a node_state in
        let%bind prev_store = StateR.get_a store in
        match (prev_node_state, msg.success) with
        | Leader s, Ok remote_match_index
        (* Update commit index, send more messages *)
          when Int.(msg.term = S.get_current_term prev_store) ->
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
            let new_node_state = Leader {s with match_index; next_index} in
            StateR.map_t @@ A.set node_state ~to_:new_node_state
        | Leader s, Error error_index ->
            (* Error occurred, it occurred at an index which is at most prev_log_index *)
            [%log.debug
              logger "RappendEntiresResponse: Invalid append_entries"
                (error_index : log_index)
                ~next_index:(Map.find_exn s.next_index msg.src : log_index)] ;
            let next_index =
              Map.set s.next_index ~key:msg.src ~data:error_index
            in
            let node_state = Leader {s with next_index} in
            StateR.map_t (fun t -> {t with node_state})
        | _ ->
            StateR.return ()
      in
      (* Send any remaining entries / Fixed entries *)
      send_append_entries msg.src
  end

  let transition_to_leader () =
    let%bind t = StateR.get_t () in
    match t.node_state with
    | Candidate s ->
        let current_term = S.get_current_term t.store in
        [%log.info
          logger "Won leader election"
            ~of_term:(current_term : term)
            ~commited:(t.commit_index : log_index)] ;
        [%log.debug
          logger "Leader transition starting"
            (Candidate s : node_state)
            ~log:(t.store : Store.t)] ;
        let entries =
          List.map s.entries ~f:(fun entry -> {entry with term= current_term})
        in
        let%bind () =
          add_entries_remove_conflicts ~start_index:s.start_index entries
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
        [%log.debug
          logger "Leader transition complete" ~log:(t.store : Store.t)] ;
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
              send node_id
              @@ RequestVote
                   { src= t.config.node_id
                   ; term= S.get_current_term t.store
                   ; leader_commit= t.commit_index } )
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
        StateR.map_t
        @@ A.set (node_state @> Leader.match_index) ~to_:match_index
    | _ ->
        StateR.return ()

  (**
   * Takes two lists of log entries
   * merges them.
   * They should start at the same point
   *
   * If they don't match it takes the entry with the highest term
   *)
  let merge_entries lx sx ly sy =
    if not Int64.(sx = sy) then (
      [%log.error
        logger "Error while merging"
          ~start_x:(sx : log_index)
          ~start_y:(sy : log_index)] ;
      StateR.fail @@ `Msg "Can't merge entries not starting at same point" )
    else
      let rec loop acc : 'a -> log_entry list = function
        | [], [] ->
            acc
        | x :: xs, ([] as ys) ->
            loop (x :: acc) (xs, ys)
        | ([] as xs), y :: ys ->
            loop (y :: acc) (xs, ys)
        | x :: xs, y :: ys when x.term < y.term ->
            loop (y :: acc) (xs, ys)
        | x :: xs, y :: ys when x.term > y.term ->
            loop (x :: acc) (xs, ys)
        | x :: xs, y :: ys
          when [%compare: Types.command] x.command y.command = 0 ->
            loop (x :: acc) (xs, ys)
        | x :: _xs, y :: _ys ->
            [%log.error
              logger "Commands for same term and index are different..."
                ~start:(sx : log_index)
                (x : Types.log_entry)
                (y : Types.log_entry)] ;
            assert false
      in
      loop [] (lx, ly) |> List.rev |> StateR.return

  let command_size_probe =
    Probe.create ~name:"cs_batch_size" ~units:Profiler_units.Int

  let rec advance_raw (event : event) : (unit, 'b) StateR.t =
    let%bind t = StateR.get_t () in
    match (event, t.node_state) with
    | `Tick, Follower {timeout} when timeout >= t.config.election_timeout ->
        [%log.info logger "Election timeout"] ; transition_to_candidate ()
    | `Tick, Follower {timeout} ->
        let new_timeout = timeout + 1 in
        [%log.debug
          logger "Tick" ~pre:(timeout : int) ~post:(new_timeout : int)] ;
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
        StateR.map_t
        @@ A.map (node_state @> Leader.heartbeat) ~f:(fun h -> h + 1)
    | (`Recv (RequestVote {term; _}) as event), _
    | (`Recv (RequestVoteResponse {term; _}) as event), _
    | (`Recv (AppendEntries {term; _}) as event), _
    | (`Recv (AppendEntriesResponse {term; _}) as event), _
      when Int.(S.get_current_term t.store < term) ->
        let%bind () = StateR.map_t @@ A.map store ~f:(S.update_term ~term) in
        let%bind () = transition_to_follower () in
        advance_raw event
    | `Recv (RequestVote msg), _
      when Int.(msg.term < S.get_current_term t.store) ->
        StateR.append @@ send msg.src
        @@ RequestVoteResponse
             { src= t.config.node_id
             ; term= S.get_current_term t.store
             ; vote_granted= false
             ; entries= []
             ; start_index= Int64.(msg.leader_commit + one) }
    | `Recv (RequestVote msg), _ ->
        let%bind () =
          let entries =
            S.entries_after_inc t.store Int64.(msg.leader_commit + one)
          in
          StateR.append @@ send msg.src
          @@ RequestVoteResponse
               { src= t.config.node_id
               ; term= S.get_current_term t.store
               ; vote_granted= true
               ; entries
               ; start_index= Int64.(msg.leader_commit + one) }
        in
        StateR.map_t @@ A.set (node_state @> Follower.timeout) ~to_:0
    | `Recv (RequestVoteResponse msg), Candidate s
      when Int.(msg.term = S.get_current_term t.store) && msg.vote_granted -> (
      match U.Quorum.add msg.src s.quorum with
      | Error `AlreadyInList ->
          StateR.return ()
      | Ok (quorum : node_id U.Quorum.t) ->
          let%bind entries =
            merge_entries s.entries s.start_index msg.entries msg.start_index
          in
          let%bind () =
            StateR.map_t
            @@ A.set node_state ~to_:(Candidate {s with quorum; entries})
          in
          if U.Quorum.satisified quorum then transition_to_leader ()
          else StateR.return () )
    | `Recv (RequestVoteResponse _), _ ->
        StateR.return ()
    | `Recv (AppendEntries msg), _ ->
        ReplicationSM.recv_append_entries msg
    | `Recv (AppendEntriesResponse msg), _ ->
        let%bind () = ReplicationSM.recv_append_entries_response msg in
        check_commit_index ()
    | `Commands cs, Leader _ ->
        let cmds =
          List.filter cs ~f:(fun cmd -> not @@ S.mem_id t.store cmd.id)
        in
        Probe.record command_size_probe (List.length cmds) ;
        let current_term = S.get_current_term t.store in
        let%bind () =
          StateR.map_t @@ A.map store ~f:(S.add_cmds ~cmds ~term:current_term)
        in
        let%bind () = check_commit_index () in
        StateR.list_iter t.config.other_nodes
          ~f:ReplicationSM.send_append_entries
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

  let pop_store (t : t) = ({t with store= S.reset_ops t.store}, t.store)

  let advance t event =
    [%log.debug io_logger "NEW EVENT" (event : event)] ;
    [%log.debug log_logger (t.store : S.t)] ;
    let prog = advance_raw event in
    let%bind.Result (), State.{t; a= actions} =
      StateR.eval prog (State.empty t)
    in
    let is_leader = Option.is_some (is_leader t) in
    let actions =
      {actions with nonblock_sync= actions.nonblock_sync || is_leader}
    in
    Ok (t, actions)

  let create_node config store =
    [%log.info logger "Creating new node" ~config:(config : config)] ;
    { config
    ; store
    ; commit_index= Int64.zero
    ; node_state= Follower {timeout= config.election_timeout} }
end

module Test = struct
  module S = Immutable_store
  module P = Make (S)
  open! P.StateR.Let_syntax

  let cmd_of_int i =
    Types.Command.
      { op= Read (Int.to_string i)
      ; id= Uuid.create_random (Random.State.make [|i|]) }

  let single_config =
    { phase1quorum= 1
    ; phase2quorum= 1
    ; other_nodes= []
    ; num_nodes= 1
    ; node_id= 1
    ; election_timeout= 1 }

  let three_config =
    { phase1quorum= 2
    ; phase2quorum= 2
    ; other_nodes= [2; 3]
    ; num_nodes= 3
    ; node_id= 1
    ; election_timeout= 1 }

  let%expect_test "merge_entries" =
    let empty_state =
      let store = S.init () in
      let t = P.create_node single_config store in
      P.State.empty t
    in
    let open Types in
    let open P in
    let mk_entry i term =
      let cmd = cmd_of_int i in
      {command= cmd; term}
    in
    let mk_entries ls = List.map ls ~f:(fun (i, term) -> mk_entry i term) in
    let print_result res =
      res
      |> (fun v -> StateR.eval v empty_state)
      |> Result.map ~f:fst
      |> Result.map ~f:(fun v ->
             List.map v ~f:(fun v ->
                 [%message (v.command.op : Types.sm_op) (v.term : term)] ) )
      |> [%sexp_of: (Sexp.t list, [> `Msg of key]) Result.t]
      |> Sexp.to_string_hum |> print_endline
    in
    merge_entries [] Int64.(of_int 1) [] Int64.(of_int 2) |> print_result ;
    [%expect
      {| (Error (Msg "Can't merge entries not starting at same point")) |}] ;
    let x = [(1, 1); (2, 1); (3, 1)] |> mk_entries in
    let y = [(1, 1)] |> mk_entries in
    merge_entries x Int64.zero y Int64.zero |> print_result ;
    [%expect
      {|
      (Ok
       ((("(v.command).op" (Read 1)) (v.term 1))
        (("(v.command).op" (Read 2)) (v.term 1))
        (("(v.command).op" (Read 3)) (v.term 1)))) |}] ;
    let x = [(1, 1)] |> mk_entries in
    let y = [(1, 1); (2, 1); (3, 1)] |> mk_entries in
    merge_entries x Int64.zero y Int64.zero |> print_result ;
    [%expect
      {|
      (Ok
       ((("(v.command).op" (Read 1)) (v.term 1))
        (("(v.command).op" (Read 2)) (v.term 1))
        (("(v.command).op" (Read 3)) (v.term 1)))) |}] ;
    let x = [(1, 1); (2, 1); (3, 1)] |> mk_entries in
    let y = [(1, 1); (2, 1); (3, 1)] |> mk_entries in
    merge_entries x Int64.zero y Int64.zero |> print_result ;
    [%expect
      {|
      (Ok
       ((("(v.command).op" (Read 1)) (v.term 1))
        (("(v.command).op" (Read 2)) (v.term 1))
        (("(v.command).op" (Read 3)) (v.term 1)))) |}] ;
    let x = [(1, 1); (4, 2)] |> mk_entries in
    let y = [(1, 1); (2, 1); (3, 2)] |> mk_entries in
    merge_entries x Int64.zero y Int64.zero |> print_result ;
    [%expect
      {|
      (Ok
       ((("(v.command).op" (Read 1)) (v.term 1))
        (("(v.command).op" (Read 4)) (v.term 2))
        (("(v.command).op" (Read 3)) (v.term 2)))) |}] ;
    let x = [(1, 1); (2, 1); (3, 2)] |> mk_entries in
    let y = [(1, 1); (4, 2)] |> mk_entries in
    merge_entries x Int64.zero y Int64.zero |> print_result ;
    [%expect
      {|
      (Ok
       ((("(v.command).op" (Read 1)) (v.term 1))
        (("(v.command).op" (Read 4)) (v.term 2))
        (("(v.command).op" (Read 3)) (v.term 2)))) |}] ;
    ()

  let pr_err s p =
    match P.StateR.eval p s with
    | Error (`Msg s) ->
        print_endline @@ Fmt.str "Error: %s" s
    | Ok _ ->
        ()

  let get_result s p =
    match P.StateR.eval p s with
    | Error (`Msg s) ->
        raise @@ Invalid_argument s
    | Ok ((), {t; a= actions}) ->
        (t, actions)

  let get_ok = function
    | Error (`Msg s) ->
        raise @@ Invalid_argument s
    | Ok v ->
        v

  let print_state (t : P.t) actions =
    let t, store = P.pop_store t in
    [%message
      (t.P.node_state : P.node_state)
        (t.P.commit_index : log_index)
        (store : S.t)
        (actions : P.actions)]
    |> Sexp.to_string_hum |> print_endline ;
    t

  let make_empty t = P.State.empty t

  let%expect_test "transitions" =
    let store = S.init () in
    let t = P.create_node three_config store in
    let () = pr_err (make_empty t) @@ P.transition_to_leader () in
    [%expect
      {| Error: Cannot transition to leader from states other than candidate |}] ;
    let t, actions =
      P.transition_to_candidate () |> get_result (make_empty t)
    in
    let t = print_state t actions in
    [%expect
      {|
    ((t.P.node_state
      (Candidate (quorum ((elts (1)) (n 1) (threshold 2) (eq <fun>)))
       (entries ()) (start_index 1) (timeout 0)))
     (t.P.commit_index 0)
     (store
      ((data ((current_term 1) (log ((store ()) (command_set ()) (length 0)))))
       (ops ((Term 1)))))
     (actions
      ((acts
        ((Send (2 (RequestVote ((src 1) (term 1) (leader_commit 0)))))
         (Send (3 (RequestVote ((src 1) (term 1) (leader_commit 0)))))))
       (nonblock_sync false)))) |}] ;
    let t, actions = P.transition_to_leader () |> get_result (make_empty t) in
    let t = print_state t actions in
    let _ = t in
    [%expect
      {|
    ((t.P.node_state
      (Leader (match_index ((1 0) (2 0) (3 0))) (next_index ((1 1) (2 1) (3 1)))
       (heartbeat 0)))
     (t.P.commit_index 0)
     (store
      ((data ((current_term 1) (log ((store ()) (command_set ()) (length 0)))))
       (ops ())))
     (actions
      ((acts
        ((Send
          (3
           (AppendEntries
            ((src 1) (term 1) (prev_log_index 0) (prev_log_term 0) (entries ())
             (entries_length 0) (leader_commit 0)))))
         (Send
          (2
           (AppendEntries
            ((src 1) (term 1) (prev_log_index 0) (prev_log_term 0) (entries ())
             (entries_length 0) (leader_commit 0)))))))
       (nonblock_sync false)))) |}]

  let%expect_test "tick" =
    let store = S.init () in
    let t = P.create_node three_config store in
    let t, actions = P.advance t `Tick |> get_ok in
    let t = print_state t actions in
    [%expect
      {|
    ((t.P.node_state
      (Candidate (quorum ((elts (1)) (n 1) (threshold 2) (eq <fun>)))
       (entries ()) (start_index 1) (timeout 0)))
     (t.P.commit_index 0)
     (store
      ((data ((current_term 1) (log ((store ()) (command_set ()) (length 0)))))
       (ops ((Term 1)))))
     (actions
      ((acts
        ((Send (2 (RequestVote ((src 1) (term 1) (leader_commit 0)))))
         (Send (3 (RequestVote ((src 1) (term 1) (leader_commit 0)))))))
       (nonblock_sync false)))) |}] ;
    let t, _ = P.transition_to_follower () |> get_result (make_empty t) in
    let t, actions = P.advance t `Tick |> get_ok in
    let _t = print_state t actions in
    [%expect
      {|
    ((t.P.node_state (Follower (timeout 1))) (t.P.commit_index 0)
     (store
      ((data ((current_term 1) (log ((store ()) (command_set ()) (length 0)))))
       (ops ())))
     (actions ((acts ()) (nonblock_sync false)))) |}]

  let%expect_test "loop single" =
    let store = S.init () in
    let t = P.create_node single_config store in
    let t, actions =
      P.advance t (`Commands [cmd_of_int 1; cmd_of_int 2]) |> get_ok
    in
    let t = print_state t actions in
    [%expect
      {|
    ((t.P.node_state (Follower (timeout 1))) (t.P.commit_index 0)
     (store
      ((data ((current_term 0) (log ((store ()) (command_set ()) (length 0)))))
       (ops ())))
     (actions
      ((acts
        ((Unapplied
          (((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0))
           ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a))))))
       (nonblock_sync false)))) |}] ;
    let t, actions = P.advance t `Tick |> get_ok in
    let t = print_state t actions in
    [%expect
      {|
    ((t.P.node_state
      (Leader (match_index ((1 0))) (next_index ((1 1))) (heartbeat 0)))
     (t.P.commit_index 0)
     (store
      ((data ((current_term 1) (log ((store ()) (command_set ()) (length 0)))))
       (ops ((Term 1)))))
     (actions ((acts ()) (nonblock_sync true)))) |}] ;
    let t, actions =
      P.advance t (`Commands [cmd_of_int 1; cmd_of_int 2]) |> get_ok
    in
    let t = print_state t actions in
    [%expect
      {|
    ((t.P.node_state
      (Leader (match_index ((1 0))) (next_index ((1 1))) (heartbeat 0)))
     (t.P.commit_index 0)
     (store
      ((data
        ((current_term 1)
         (log
          ((store
            (((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
              (term 1))
             ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
              (term 1))))
           (command_set
            (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
             da9280e5-0845-4466-e4bb-94e2f401c14a))
           (length 2)))))
       (ops
        ((Log
          (Add
           ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
            (term 1))))
         (Log
          (Add
           ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
            (term 1))))))))
     (actions ((acts ()) (nonblock_sync true)))) |}] ;
    let t, actions = P.advance t Int64.(`Syncd (of_int 2)) |> get_ok in
    let t = print_state t actions in
    let _ = t in
    [%expect
      {|
    ((t.P.node_state
      (Leader (match_index ((1 2))) (next_index ((1 1))) (heartbeat 0)))
     (t.P.commit_index 2)
     (store
      ((data
        ((current_term 1)
         (log
          ((store
            (((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
              (term 1))
             ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
              (term 1))))
           (command_set
            (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
             da9280e5-0845-4466-e4bb-94e2f401c14a))
           (length 2)))))
       (ops ())))
     (actions ((acts ((CommitIndexUpdate 2))) (nonblock_sync true)))) |}]

  let%expect_test "loop triple" =
    let s1 = S.init () in
    let t1 = P.create_node three_config s1 in
    let s2 = S.init () in
    let t2 =
      P.create_node {three_config with other_nodes= [1; 3]; node_id= 2} s2
    in
    let t2, actions = P.advance t2 `Tick |> get_ok in
    let t2 = print_state t2 actions in
    [%expect
      {|
    ((t.P.node_state
      (Candidate (quorum ((elts (2)) (n 1) (threshold 2) (eq <fun>)))
       (entries ()) (start_index 1) (timeout 0)))
     (t.P.commit_index 0)
     (store
      ((data ((current_term 2) (log ((store ()) (command_set ()) (length 0)))))
       (ops ((Term 2)))))
     (actions
      ((acts
        ((Send (1 (RequestVote ((src 2) (term 2) (leader_commit 0)))))
         (Send (3 (RequestVote ((src 2) (term 2) (leader_commit 0)))))))
       (nonblock_sync false)))) |}] ;
    let rv =
      List.find_map_exn actions.acts ~f:(function
        | `Send (dst, rv) when dst = 1 ->
            Some rv
        | _ ->
            None )
    in
    let t1, actions = P.advance t1 (`Recv rv) |> get_ok in
    let t1 = print_state t1 actions in
    [%expect
      {|
    ((t.P.node_state (Follower (timeout 0))) (t.P.commit_index 0)
     (store
      ((data ((current_term 2) (log ((store ()) (command_set ()) (length 0)))))
       (ops ((Term 2)))))
     (actions
      ((acts
        ((Send
          (2
           (RequestVoteResponse
            ((src 1) (term 2) (vote_granted true) (entries ()) (start_index 1)))))))
       (nonblock_sync false)))) |}] ;
    let rvr =
      List.find_map_exn actions.acts ~f:(function
        | `Send (id, rvr) when id = 2 ->
            Some rvr
        | _ ->
            None )
    in
    let t2, actions = P.advance t2 (`Recv rvr) |> get_ok in
    let t2 = print_state t2 actions in
    [%expect
      {|
    ((t.P.node_state
      (Leader (match_index ((1 0) (2 0) (3 0))) (next_index ((1 1) (2 1) (3 1)))
       (heartbeat 0)))
     (t.P.commit_index 0)
     (store
      ((data ((current_term 2) (log ((store ()) (command_set ()) (length 0)))))
       (ops ())))
     (actions
      ((acts
        ((Send
          (3
           (AppendEntries
            ((src 2) (term 2) (prev_log_index 0) (prev_log_term 0) (entries ())
             (entries_length 0) (leader_commit 0)))))
         (Send
          (1
           (AppendEntries
            ((src 2) (term 2) (prev_log_index 0) (prev_log_term 0) (entries ())
             (entries_length 0) (leader_commit 0)))))))
       (nonblock_sync true)))) |}] ;
    let t2, actions =
      P.advance t2 (`Commands [cmd_of_int 1; cmd_of_int 2]) |> get_ok
    in
    let t2 = print_state t2 actions in
    [%expect
      {|
    ((t.P.node_state
      (Leader (match_index ((1 0) (2 0) (3 0))) (next_index ((1 3) (2 1) (3 3)))
       (heartbeat 0)))
     (t.P.commit_index 0)
     (store
      ((data
        ((current_term 2)
         (log
          ((store
            (((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
              (term 2))
             ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
              (term 2))))
           (command_set
            (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
             da9280e5-0845-4466-e4bb-94e2f401c14a))
           (length 2)))))
       (ops
        ((Log
          (Add
           ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
            (term 2))))
         (Log
          (Add
           ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
            (term 2))))))))
     (actions
      ((acts
        ((Send
          (3
           (AppendEntries
            ((src 2) (term 2) (prev_log_index 0) (prev_log_term 0)
             (entries
              (((command
                 ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                (term 2))
               ((command
                 ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                (term 2))))
             (entries_length 2) (leader_commit 0)))))
         (Send
          (1
           (AppendEntries
            ((src 2) (term 2) (prev_log_index 0) (prev_log_term 0)
             (entries
              (((command
                 ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                (term 2))
               ((command
                 ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                (term 2))))
             (entries_length 2) (leader_commit 0)))))))
       (nonblock_sync true)))) |}] ;
    let ae =
      List.find_map_exn actions.acts ~f:(function
        | `Send (id, ae) when id = 1 ->
            Some ae
        | _ ->
            None )
    in
    let t2, actions = P.advance t2 (`Syncd (Int64.of_int 2)) |> get_ok in
    let t2 = print_state t2 actions in
    [%expect
      {|
    ((t.P.node_state
      (Leader (match_index ((1 0) (2 2) (3 0))) (next_index ((1 3) (2 1) (3 3)))
       (heartbeat 0)))
     (t.P.commit_index 0)
     (store
      ((data
        ((current_term 2)
         (log
          ((store
            (((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
              (term 2))
             ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
              (term 2))))
           (command_set
            (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
             da9280e5-0845-4466-e4bb-94e2f401c14a))
           (length 2)))))
       (ops ())))
     (actions ((acts ()) (nonblock_sync true)))) |}] ;
    let t1, actions = P.advance t1 (`Recv ae) |> get_ok in
    let t1 = print_state t1 actions in
    let _ = t1 in
    [%expect
      {|
    ((t.P.node_state (Follower (timeout 0))) (t.P.commit_index 0)
     (store
      ((data
        ((current_term 2)
         (log
          ((store
            (((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
              (term 2))
             ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
              (term 2))))
           (command_set
            (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
             da9280e5-0845-4466-e4bb-94e2f401c14a))
           (length 2)))))
       (ops
        ((Log
          (Add
           ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
            (term 2))))
         (Log
          (Add
           ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
            (term 2))))))))
     (actions
      ((acts
        ((Send (2 (AppendEntriesResponse ((src 1) (term 2) (success (Ok 2))))))))
       (nonblock_sync false)))) |}] ;
    let aer =
      List.find_map_exn actions.acts ~f:(function
        | `Send (id, aer) when id = 2 ->
            Some aer
        | _ ->
            None )
    in
    (* In case of full update *)
    let () =
      let t2, actions = P.advance t2 (`Recv aer) |> get_ok in
      let _t2 = print_state t2 actions in
      [%expect
        {|
      ((t.P.node_state
        (Leader (match_index ((1 2) (2 2) (3 0))) (next_index ((1 3) (2 1) (3 3)))
         (heartbeat 0)))
       (t.P.commit_index 2)
       (store
        ((data
          ((current_term 2)
           (log
            ((store
              (((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                (term 2))
               ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                (term 2))))
             (command_set
              (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
               da9280e5-0845-4466-e4bb-94e2f401c14a))
             (length 2)))))
         (ops ())))
       (actions ((acts ((CommitIndexUpdate 2))) (nonblock_sync true)))) |}]
    in
    ()

  let%expect_test "loop triple, double leader election" =
    let s1 = S.init () in
    let t1 = P.create_node three_config s1 in
    let s2 = S.init () in
    let t2 =
      P.create_node {three_config with other_nodes= [1; 3]; node_id= 2} s2
    in
    let s3 = S.init () in
    let t3 =
      P.create_node {three_config with other_nodes= [1; 2]; node_id= 3} s3
    in
    let t2, actions = P.advance t2 `Tick |> get_ok in
    let t2, _ = P.pop_store t2 in
    let rv =
      List.find_map_exn actions.acts ~f:(function
        | `Send (dst, rv) when dst = 1 ->
            Some rv
        | _ ->
            None )
    in
    let t1, actions = P.advance t1 (`Recv rv) |> get_ok in
    let t1, _ = P.pop_store t1 in
    let rvr =
      List.find_map_exn actions.acts ~f:(function
        | `Send (id, rvr) when id = 2 ->
            Some rvr
        | _ ->
            None )
    in
    let t2, _actions = P.advance t2 (`Recv rvr) |> get_ok in
    let t2, _ = P.pop_store t2 in
    let t2, actions =
      P.advance t2 (`Commands [cmd_of_int 1; cmd_of_int 2]) |> get_ok
    in
    let t2 = print_state t2 actions in
    [%expect
      {|
      ((t.P.node_state
        (Leader (match_index ((1 0) (2 0) (3 0))) (next_index ((1 3) (2 1) (3 3)))
         (heartbeat 0)))
       (t.P.commit_index 0)
       (store
        ((data
          ((current_term 2)
           (log
            ((store
              (((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                (term 2))
               ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                (term 2))))
             (command_set
              (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
               da9280e5-0845-4466-e4bb-94e2f401c14a))
             (length 2)))))
         (ops
          ((Log
            (Add
             ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
              (term 2))))
           (Log
            (Add
             ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
              (term 2))))))))
       (actions
        ((acts
          ((Send
            (3
             (AppendEntries
              ((src 2) (term 2) (prev_log_index 0) (prev_log_term 0)
               (entries
                (((command
                   ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                  (term 2))
                 ((command
                   ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                  (term 2))))
               (entries_length 2) (leader_commit 0)))))
           (Send
            (1
             (AppendEntries
              ((src 2) (term 2) (prev_log_index 0) (prev_log_term 0)
               (entries
                (((command
                   ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                  (term 2))
                 ((command
                   ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                  (term 2))))
               (entries_length 2) (leader_commit 0)))))))
         (nonblock_sync true)))) |}] ;
    let ae =
      List.find_map_exn actions.acts ~f:(function
        | `Send (id, ae) when id = 1 ->
            Some ae
        | _ ->
            None )
    in
    let t2, _actions = P.advance t2 (`Syncd (Int64.of_int 2)) |> get_ok in
    let t2, _ = P.pop_store t2 in
    let t1, actions = P.advance t1 (`Recv ae) |> get_ok in
    let t1 = print_state t1 actions in
    [%expect
      {|
      ((t.P.node_state (Follower (timeout 0))) (t.P.commit_index 0)
       (store
        ((data
          ((current_term 2)
           (log
            ((store
              (((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                (term 2))
               ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                (term 2))))
             (command_set
              (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
               da9280e5-0845-4466-e4bb-94e2f401c14a))
             (length 2)))))
         (ops
          ((Log
            (Add
             ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
              (term 2))))
           (Log
            (Add
             ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
              (term 2))))))))
       (actions
        ((acts
          ((Send (2 (AppendEntriesResponse ((src 1) (term 2) (success (Ok 2))))))))
         (nonblock_sync false)))) |}] ;
    let aer =
      List.find_map_exn actions.acts ~f:(function
        | `Send (id, aer) when id = 2 ->
            Some aer
        | _ ->
            None )
    in
    let t2, actions = P.advance t2 (`Recv aer) |> get_ok in
    let t2 = print_state t2 actions in
    [%expect
      {|
      ((t.P.node_state
        (Leader (match_index ((1 2) (2 2) (3 0))) (next_index ((1 3) (2 1) (3 3)))
         (heartbeat 0)))
       (t.P.commit_index 2)
       (store
        ((data
          ((current_term 2)
           (log
            ((store
              (((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                (term 2))
               ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                (term 2))))
             (command_set
              (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
               da9280e5-0845-4466-e4bb-94e2f401c14a))
             (length 2)))))
         (ops ())))
       (actions ((acts ((CommitIndexUpdate 2))) (nonblock_sync true)))) |}] ;
    let t2, actions =
      P.advance t2 (`Commands [cmd_of_int 3; cmd_of_int 4]) |> get_ok
    in
    let t2 = print_state t2 actions in
    [%expect
      {|
      ((t.P.node_state
        (Leader (match_index ((1 2) (2 2) (3 0))) (next_index ((1 5) (2 1) (3 5)))
         (heartbeat 0)))
       (t.P.commit_index 2)
       (store
        ((data
          ((current_term 2)
           (log
            ((store
              (((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
                (term 2))
               ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
                (term 2))
               ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                (term 2))
               ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                (term 2))))
             (command_set
              (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
               6c4ac624-e62a-45ee-c2f5-f327ad1a21f6
               da9280e5-0845-4466-e4bb-94e2f401c14a
               eed8f731-aab8-4baf-f515-521eff34be65))
             (length 4)))))
         (ops
          ((Log
            (Add
             ((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
              (term 2))))
           (Log
            (Add
             ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
              (term 2))))))))
       (actions
        ((acts
          ((Send
            (3
             (AppendEntries
              ((src 2) (term 2) (prev_log_index 2) (prev_log_term 2)
               (entries
                (((command
                   ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
                  (term 2))
                 ((command
                   ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
                  (term 2))))
               (entries_length 2) (leader_commit 2)))))
           (Send
            (1
             (AppendEntries
              ((src 2) (term 2) (prev_log_index 2) (prev_log_term 2)
               (entries
                (((command
                   ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
                  (term 2))
                 ((command
                   ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
                  (term 2))))
               (entries_length 2) (leader_commit 2)))))))
         (nonblock_sync true)))) |}] ;
    let t1, _actions = P.advance t1 `Tick |> get_ok in
    let t1, _ = P.pop_store t1 in
    let t1, actions = P.advance t1 `Tick |> get_ok in
    let t1 = print_state t1 actions in
    [%expect
      {|
      ((t.P.node_state
        (Candidate (quorum ((elts (1)) (n 1) (threshold 2) (eq <fun>)))
         (entries
          (((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
            (term 2))
           ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
            (term 2))))
         (start_index 1) (timeout 0)))
       (t.P.commit_index 0)
       (store
        ((data
          ((current_term 4)
           (log
            ((store
              (((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                (term 2))
               ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                (term 2))))
             (command_set
              (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
               da9280e5-0845-4466-e4bb-94e2f401c14a))
             (length 2)))))
         (ops ((Term 4)))))
       (actions
        ((acts
          ((Send (2 (RequestVote ((src 1) (term 4) (leader_commit 0)))))
           (Send (3 (RequestVote ((src 1) (term 4) (leader_commit 0)))))))
         (nonblock_sync false)))) |}] ;
    let rv =
      List.find_map_exn actions.acts ~f:(function
        | `Send (id, rv) when id = 3 ->
            Some rv
        | _ ->
            None )
    in
    let t3, actions = P.advance t3 (`Recv rv) |> get_ok in
    let t3 = print_state t3 actions in
    [%expect
      {|
      ((t.P.node_state (Follower (timeout 0))) (t.P.commit_index 0)
       (store
        ((data ((current_term 4) (log ((store ()) (command_set ()) (length 0)))))
         (ops ((Term 4)))))
       (actions
        ((acts
          ((Send
            (1
             (RequestVoteResponse
              ((src 3) (term 4) (vote_granted true) (entries ()) (start_index 1)))))))
         (nonblock_sync false)))) |}] ;
    let rvr =
      List.find_map_exn actions.acts ~f:(function
        | `Send (id, rvr) when id = 1 ->
            Some rvr
        | _ ->
            None )
    in
    let t1, actions = P.advance t1 (`Recv rvr) |> get_ok in
    let t1 = print_state t1 actions in
    [%expect
      {|
      ((t.P.node_state
        (Leader (match_index ((1 0) (2 0) (3 0))) (next_index ((1 1) (2 3) (3 3)))
         (heartbeat 0)))
       (t.P.commit_index 0)
       (store
        ((data
          ((current_term 4)
           (log
            ((store
              (((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                (term 4))
               ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                (term 4))))
             (command_set
              (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
               da9280e5-0845-4466-e4bb-94e2f401c14a))
             (length 2)))))
         (ops
          ((Log
            (Add
             ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
              (term 4))))
           (Log
            (Add
             ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
              (term 4))))
           (Log (RemoveGEQ 1))))))
       (actions
        ((acts
          ((Send
            (3
             (AppendEntries
              ((src 1) (term 4) (prev_log_index 0) (prev_log_term 0)
               (entries
                (((command
                   ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                  (term 4))
                 ((command
                   ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                  (term 4))))
               (entries_length 2) (leader_commit 0)))))
           (Send
            (2
             (AppendEntries
              ((src 1) (term 4) (prev_log_index 0) (prev_log_term 0)
               (entries
                (((command
                   ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                  (term 4))
                 ((command
                   ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                  (term 4))))
               (entries_length 2) (leader_commit 0)))))))
         (nonblock_sync true)))) |}] ;
    let ae =
      List.find_map_exn actions.acts ~f:(function
        | `Send (id, ae) when id = 2 ->
            Some ae
        | _ ->
            None )
    in
    let t2, actions = P.advance t2 (`Recv ae) |> get_ok in
    let t2 = print_state t2 actions in
    [%expect
      {|
      ((t.P.node_state (Follower (timeout 0))) (t.P.commit_index 2)
       (store
        ((data
          ((current_term 4)
           (log
            ((store
              (((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                (term 4))
               ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                (term 4))))
             (command_set
              (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
               da9280e5-0845-4466-e4bb-94e2f401c14a))
             (length 2)))))
         (ops
          ((Log
            (Add
             ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
              (term 4))))
           (Log
            (Add
             ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
              (term 4))))
           (Log (RemoveGEQ 1)) (Term 4)))))
       (actions
        ((acts
          ((Send (1 (AppendEntriesResponse ((src 2) (term 4) (success (Ok 2))))))))
         (nonblock_sync false)))) |}] ;
    let _ts = (t1, t2, t3) in
    ()

  let%expect_test "loop triple, double leader election, asymetrical replication"
      =
    (* Create nodes *)
    let s1 = S.init () in
    let t1 = P.create_node three_config s1 in
    let s2 = S.init () in
    let t2 =
      P.create_node {three_config with other_nodes= [1; 3]; node_id= 2} s2
    in
    let s3 = S.init () in
    let t3 =
      P.create_node {three_config with other_nodes= [1; 2]; node_id= 3} s3
    in
    (* Elect t2 *)
    let t2, actions = P.advance t2 `Tick |> get_ok in
    let t2, _ = P.pop_store t2 in
    let rv =
      List.find_map_exn actions.acts ~f:(function
        | `Send (dst, rv) when dst = 1 ->
            Some rv
        | _ ->
            None )
    in
    let t1, actions = P.advance t1 (`Recv rv) |> get_ok in
    let t1, _ = P.pop_store t1 in
    let rvr =
      List.find_map_exn actions.acts ~f:(function
        | `Send (id, rvr) when id = 2 ->
            Some rvr
        | _ ->
            None )
    in
    let t2, _actions = P.advance t2 (`Recv rvr) |> get_ok in
    let t2, _ = P.pop_store t2 in
    (* Adding commands to system *)
    let t2, actions =
      P.advance t2 (`Commands [cmd_of_int 1; cmd_of_int 2]) |> get_ok
    in
    let t2, _ = P.pop_store t2 in
    let ae1 =
      List.find_map_exn actions.acts ~f:(function
        | `Send (id, ae) when id = 1 ->
            Some ae
        | _ ->
            None )
    in
    let ae3 =
      List.find_map_exn actions.acts ~f:(function
        | `Send (id, ae) when id = 3 ->
            Some ae
        | _ ->
            None )
    in
    let t3 =
      (* t3 recv append entries from t2 *)
      let t3, actions = P.advance t3 (`Recv ae3) |> get_ok in
      let t3 = print_state t3 actions in
      [%expect
        {|
         ((t.P.node_state (Follower (timeout 0))) (t.P.commit_index 0)
          (store
           ((data
             ((current_term 2)
              (log
               ((store
                 (((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                   (term 2))
                  ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                   (term 2))))
                (command_set
                 (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
                  da9280e5-0845-4466-e4bb-94e2f401c14a))
                (length 2)))))
            (ops
             ((Log
               (Add
                ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                 (term 2))))
              (Log
               (Add
                ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                 (term 2))))
              (Term 2)))))
          (actions
           ((acts
             ((Send (2 (AppendEntriesResponse ((src 3) (term 2) (success (Ok 2))))))))
            (nonblock_sync false)))) |}] ;
      t3
    in
    let t1, aer =
      (* t1 recv append entries from t2 *)
      let t1, actions = P.advance t1 (`Recv ae1) |> get_ok in
      let t1 = print_state t1 actions in
      [%expect
        {|
      ((t.P.node_state (Follower (timeout 0))) (t.P.commit_index 0)
       (store
        ((data
          ((current_term 2)
           (log
            ((store
              (((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                (term 2))
               ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                (term 2))))
             (command_set
              (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
               da9280e5-0845-4466-e4bb-94e2f401c14a))
             (length 2)))))
         (ops
          ((Log
            (Add
             ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
              (term 2))))
           (Log
            (Add
             ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
              (term 2))))))))
       (actions
        ((acts
          ((Send (2 (AppendEntriesResponse ((src 1) (term 2) (success (Ok 2))))))))
         (nonblock_sync false)))) |}] ;
      let aer =
        List.find_map_exn actions.acts ~f:(function
          | `Send (id, aer) when id = 2 ->
              Some aer
          | _ ->
              None )
      in
      (t1, aer)
    in
    let t2, _actions = P.advance t2 (`Syncd (Int64.of_int 2)) |> get_ok in
    let t2, _ = P.pop_store t2 in
    (* Recv aer *)
    let t2, _actions = P.advance t2 (`Recv aer) |> get_ok in
    let t2, _ = P.pop_store t2 in
    (* add more commands which are only replicated to t3 *)
    let t2, actions =
      P.advance t2 (`Commands [cmd_of_int 3; cmd_of_int 4]) |> get_ok
    in
    let t2 = print_state t2 actions in
    [%expect
      {|
      ((t.P.node_state
        (Leader (match_index ((1 2) (2 2) (3 0))) (next_index ((1 5) (2 1) (3 5)))
         (heartbeat 0)))
       (t.P.commit_index 2)
       (store
        ((data
          ((current_term 2)
           (log
            ((store
              (((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
                (term 2))
               ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
                (term 2))
               ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                (term 2))
               ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                (term 2))))
             (command_set
              (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
               6c4ac624-e62a-45ee-c2f5-f327ad1a21f6
               da9280e5-0845-4466-e4bb-94e2f401c14a
               eed8f731-aab8-4baf-f515-521eff34be65))
             (length 4)))))
         (ops
          ((Log
            (Add
             ((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
              (term 2))))
           (Log
            (Add
             ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
              (term 2))))))))
       (actions
        ((acts
          ((Send
            (3
             (AppendEntries
              ((src 2) (term 2) (prev_log_index 2) (prev_log_term 2)
               (entries
                (((command
                   ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
                  (term 2))
                 ((command
                   ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
                  (term 2))))
               (entries_length 2) (leader_commit 2)))))
           (Send
            (1
             (AppendEntries
              ((src 2) (term 2) (prev_log_index 2) (prev_log_term 2)
               (entries
                (((command
                   ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
                  (term 2))
                 ((command
                   ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
                  (term 2))))
               (entries_length 2) (leader_commit 2)))))))
         (nonblock_sync true)))) |}] ;
    let ae3 =
      List.find_map_exn actions.acts ~f:(function
        | `Send (id, ae) when id = 3 ->
            Some ae
        | _ ->
            None )
    in
    (* Replicate to t3 but not to t1 *)
    let t3, actions = P.advance t3 (`Recv ae3) |> get_ok in
    let t3 = print_state t3 actions in
    [%expect
      {|
      ((t.P.node_state (Follower (timeout 0))) (t.P.commit_index 2)
       (store
        ((data
          ((current_term 2)
           (log
            ((store
              (((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
                (term 2))
               ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
                (term 2))
               ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                (term 2))
               ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                (term 2))))
             (command_set
              (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
               6c4ac624-e62a-45ee-c2f5-f327ad1a21f6
               da9280e5-0845-4466-e4bb-94e2f401c14a
               eed8f731-aab8-4baf-f515-521eff34be65))
             (length 4)))))
         (ops
          ((Log
            (Add
             ((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
              (term 2))))
           (Log
            (Add
             ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
              (term 2))))))))
       (actions
        ((acts
          ((Send (2 (AppendEntriesResponse ((src 3) (term 2) (success (Ok 4))))))
           (CommitIndexUpdate 2)))
         (nonblock_sync false)))) |}] ;
    (* t2 has now crashed so no further updates should occur *)
    (* start election of t1 *)
    let t1, _actions = P.advance t1 `Tick |> get_ok in
    let t1, _ = P.pop_store t1 in
    let t1, actions = P.advance t1 `Tick |> get_ok in
    let t1 = print_state t1 actions in
    [%expect
      {|
      ((t.P.node_state
        (Candidate (quorum ((elts (1)) (n 1) (threshold 2) (eq <fun>)))
         (entries
          (((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
            (term 2))
           ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
            (term 2))))
         (start_index 1) (timeout 0)))
       (t.P.commit_index 0)
       (store
        ((data
          ((current_term 4)
           (log
            ((store
              (((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                (term 2))
               ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                (term 2))))
             (command_set
              (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
               da9280e5-0845-4466-e4bb-94e2f401c14a))
             (length 2)))))
         (ops ((Term 4)))))
       (actions
        ((acts
          ((Send (2 (RequestVote ((src 1) (term 4) (leader_commit 0)))))
           (Send (3 (RequestVote ((src 1) (term 4) (leader_commit 0)))))))
         (nonblock_sync false)))) |}] ;
    let rv =
      List.find_map_exn actions.acts ~f:(function
        | `Send (id, rv) when id = 3 ->
            Some rv
        | _ ->
            None )
    in
    let t3, actions = P.advance t3 (`Recv rv) |> get_ok in
    let t3 = print_state t3 actions in
    [%expect
      {|
      ((t.P.node_state (Follower (timeout 0))) (t.P.commit_index 2)
       (store
        ((data
          ((current_term 4)
           (log
            ((store
              (((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
                (term 2))
               ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
                (term 2))
               ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                (term 2))
               ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                (term 2))))
             (command_set
              (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
               6c4ac624-e62a-45ee-c2f5-f327ad1a21f6
               da9280e5-0845-4466-e4bb-94e2f401c14a
               eed8f731-aab8-4baf-f515-521eff34be65))
             (length 4)))))
         (ops ((Term 4)))))
       (actions
        ((acts
          ((Send
            (1
             (RequestVoteResponse
              ((src 3) (term 4) (vote_granted true)
               (entries
                (((command
                   ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
                  (term 2))
                 ((command
                   ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
                  (term 2))
                 ((command
                   ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                  (term 2))
                 ((command
                   ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                  (term 2))))
               (start_index 1)))))))
         (nonblock_sync false)))) |}] ;
    let rvr =
      List.find_map_exn actions.acts ~f:(function
        | `Send (id, rvr) when id = 1 ->
            Some rvr
        | _ ->
            None )
    in
    let t1, actions = P.advance t1 (`Recv rvr) |> get_ok in
    let t1 = print_state t1 actions in
    [%expect.unreachable] ;
    let _ts = (t1, t2, t3) in
    ()
    [@@expect.uncaught_exn
      {|
    (* CR expect_test_collector: This test expectation appears to contain a backtrace.
       This is strongly discouraged as backtraces are fragile.
       Please change this test to not include a backtrace. *)

    "Assert_failure paxos/paxos.ml:625:12"
    Raised at Paxos_lib__Paxos.Make.merge_entries.loop in file "paxos/paxos.ml", line 625, characters 12-24
    Called from Paxos_lib__Paxos.Make.merge_entries in file "paxos/paxos.ml", line 627, characters 6-22
    Called from Paxos_lib__Paxos.Make.advance_raw.(fun) in file "paxos/paxos.ml", line 697, characters 12-77
    Called from Paxos_lib__Paxos.Make.State.bind in file "paxos/paxos.ml", line 148, characters 11-16
    Called from Paxos_lib__Paxos.Make.State.eval in file "paxos/paxos.ml" (inlined), line 143, characters 52-55
    Called from Paxos_lib__Paxos.Make.StateR.eval in file "paxos/paxos.ml", line 215, characters 12-30
    Called from Paxos_lib__Paxos.Make.advance in file "paxos/paxos.ml", line 744, characters 6-38
    Called from Paxos_lib__Paxos.Test.(fun) in file "paxos/paxos.ml", line 2021, characters 22-46
    Called from Expect_test_collector.Make.Instance.exec in file "collector/expect_test_collector.ml", line 244, characters 12-19 |}]
end

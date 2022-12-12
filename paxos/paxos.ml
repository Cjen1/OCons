module C = Ocons_core
open! C.Types
open! Utils
module A = Accessor
module Log = Utils.SegmentLog

let ( @> ) = A.( @> )

type message =
  | RequestVote of {term: term; leader_commit: log_index}
  | RequestVoteResponse of
      {term: term; start_index: log_index; entries: log_entry Iter.t * int}
  | AppendEntries of
      { term: term
      ; leader_commit: log_index
      ; prev_log_index: log_index
      ; prev_log_term: term
      ; entries_length: log_index
      ; entries: log_entry Iter.t }
  | AppendEntriesResponse of
      {term: term; success: (log_index, log_index) Result.t}

type event = Tick | Recv of (message * node_id) | Commands of command iter

type action =
  | Send of node_id * message
  | Broadcast of message
  | CommitCommands of command Iter.t

type config =
  { phase1quorum: int
  ; phase2quorum: int
  ; other_nodes: node_id list
  ; num_nodes: int
  ; node_id: node_id
  ; election_timeout: int }

let config_pp : config Fmt.t =
 fun ppf v ->
  let open Fmt in
  pf ppf "{P1Q:%d,P2Q:%d,#Nodes:%d,Id:%d,eT:%d,%a}" v.phase1quorum
    v.phase2quorum v.num_nodes v.node_id v.election_timeout
    (braces @@ list ~sep:comma int)
    v.other_nodes

let make_config ~node_id ~node_list ~election_timeout =
  let length = List.length node_list in
  let phase1quorum = (length + 1) / 2 in
  let phase2quorum = (length + 1) / 2 in
  let other_nodes =
    node_list |> List.filter (fun id -> not @@ Int.equal node_id id)
  in
  { phase1quorum
  ; phase2quorum
  ; other_nodes
  ; num_nodes= length
  ; node_id
  ; election_timeout }

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

type t =
  { log: log_entry SegmentLog.t
  ; commit_index: log_index (* Guarantee that [commit_index] is >= log.vlo *)
  ; config: config
  ; node_state: node_state
  ; current_term: term }
[@@deriving accessors]

module type ActionSig = sig
  val send : node_id -> message -> unit

  val broadcast : message -> unit

  val commit : upto:int -> unit
  (* if upto is greater than t commit index then update it and mark to emit action *)

  val t : ('i -> t -> t, 'i -> unit -> unit, [< A.field]) A.General.t

  val run : (event -> unit) -> t -> event -> t * action list
end

module Make (Act : ActionSig) = struct
  type nonrec config = config

  type nonrec message = message

  type nonrec event = event

  type nonrec t = t

  open Act

  let send_append_entries ?(force = false) () =
    let ct = A.get t () in
    match ct.node_state with
    | Leader s ->
        let highest = Log.highest ct.log in
        s.rep_sent <-
          s.rep_sent
          |> IntMap.mapi (fun id start ->
                 ( if start < highest || force then
                   (* May want to limit max msg sent *)
                   let upper = highest in
                   send id
                   @@ AppendEntries
                        { term= ct.current_term
                        ; leader_commit= A.get (t @> commit_index) ()
                        ; prev_log_index= start - 1
                        ; prev_log_term= (Log.get ct.log (start - 1)).term
                        ; entries_length= upper - start
                        ; entries= Log.iter ct.log ~lo:start ~hi:upper } ) ;
                 highest )
    | _ ->
        assert false

  let transit_follower term =
    A.set (t @> node_state) () ~to_:(Follower {timeout= 0}) ;
    A.set (t @> current_term) () ~to_:term

  let transit_candidate () =
    let cterm = A.get (t @> current_term) () in
    let num_nodes = (A.get (t @> config) ()).num_nodes in
    let quot, rem = (Int.div cterm num_nodes, cterm mod num_nodes) in
    let new_term = ((quot + 1) * num_nodes) + rem in
    A.set (t @> node_state) ()
      ~to_:(Candidate {quorum= Utils.Quorum.empty (num_nodes - 1); timeout= 0}) ;
    A.set (t @> current_term) () ~to_:new_term ;
    broadcast
    @@ RequestVote {term= new_term; leader_commit= A.get (t @> commit_index) ()}

  let transit_leader () =
    let ct = A.get t () in
    match ct.node_state with
    | Candidate {quorum; _} ->
        let resps = quorum.Quorum.elts |> IntMap.to_seq in
        resps
        |> Seq.iter (fun (_, entry_seq) ->
               entry_seq
               |> Iter.iter (fun (idx, le) ->
                      if (Log.get ct.log idx).term < le.term then
                        Log.set ct.log idx le ) ) ;
        let rep_ackd =
          ct.config.other_nodes |> List.to_seq
          |> Seq.map (fun i -> (i, 0))
          |> IntMap.of_seq
        in
        let rep_sent =
          ct.config.other_nodes |> List.to_seq
          |> Seq.map (fun i -> (i, ct.commit_index + 1))
          |> IntMap.of_seq
        in
        A.set (t @> node_state) ()
          ~to_:(Leader {rep_ackd; rep_sent; heartbeat= 0}) ;
        send_append_entries ~force:true ()
    | _ ->
        assert false

  let if_recv_advance_term e =
    match e with
    | Recv
        ( ( AppendEntries {term; _}
          | AppendEntriesResponse {term; _}
          | RequestVote {term; _}
          | RequestVoteResponse {term; _} )
        , _ )
      when term > A.get A.(t @> current_term) () ->
        transit_follower term
    | _ ->
        ()

  let resolve_event e =
    if_recv_advance_term e ;
    match (e, A.get A.(t @> node_state) ()) with
    (* Increment ticks *)
    | Tick, Follower {timeout} ->
        A.set (t @> node_state @> Follower.timeout) ~to_:(timeout + 1) ()
    | Tick, Candidate {timeout; _} ->
        A.set (t @> node_state @> Candidate.timeout) ~to_:(timeout + 1) ()
    | Tick, Leader {heartbeat; _} ->
        A.set (t @> node_state @> Leader.heartbeat) ~to_:(heartbeat + 1) ()
    | Commands cs, Leader _ ->
        cs (fun c ->
            Log.add
              (A.get (t @> log) ())
              {command= c; term= A.get (t @> current_term) ()} )
    | Commands cs, _ ->
        cs |> assert false (*TODO*)
    (* Ignore msgs from lower terms *)
    | ( Recv
          ( ( RequestVote {term; _}
            | RequestVoteResponse {term; _}
            | AppendEntries {term; _}
            | AppendEntriesResponse {term; _} )
          , _ )
      , _ )
      when term < A.get A.(t @> current_term) () ->
        ()
    (* Recv msgs from this term*)
    (* Candidate*)
    | Recv (RequestVoteResponse m, src), Candidate _ ->
        assert (m.term = A.get (t @> current_term) ()) ;
        let entries, _ = m.entries in
        let q_entries =
          entries |> Iter.zip_i
          |> Iter.map (fun (i, e) -> (i + m.start_index, e))
        in
        A.map
          A.(t @> node_state @> Candidate.quorum)
          ~f:(Quorum.add src q_entries) ()
    (* Leader *)
    | Recv (AppendEntriesResponse ({success= Ok idx; _} as m), src), Leader _ ->
        assert (m.term = A.get (t @> current_term) ()) ;
        A.map A.(t @> node_state @> Leader.rep_ackd) () ~f:(IntMap.add src idx)
    | Recv (AppendEntriesResponse ({success= Error idx; _} as m), src), Leader _
      ->
        (* This case happens if a message is lost *)
        assert (m.term = A.get (t @> current_term) ()) ;
        A.map
          A.(t @> node_state @> Leader.rep_sent)
          ()
          ~f:(IntMap.add src (idx - 1))
    (* Follower *)
    | Recv (RequestVote m, cid), Follower _ ->
        let t = A.get t () in
        let entries = Log.iter_len t.log ~lo:m.leader_commit () in
        send cid
        @@ RequestVoteResponse
             {term= t.current_term; start_index= m.leader_commit; entries}
    | ( Recv
          ( AppendEntries
              {prev_log_term; prev_log_index; entries; leader_commit; _}
          , lid )
      , Follower _ ) ->
        let ct = A.get t () in
        if
          (not @@ Log.mem ct.log prev_log_index)
          && (Log.get ct.log prev_log_index).term = prev_log_term
        then
          (* Reply with the highest index known not to be replicated *)
          (* This will be the prev_log_index of the next msg *)
          send lid
          @@ AppendEntriesResponse
               { term= ct.current_term
               ; success=
                   Error (min (prev_log_index - 1) (Log.highest ct.log - 1)) }
        else
          entries |> Iter.zip_i
          |> Iter.map (fun (i, v) -> (i + prev_log_index + 1, v))
          |> Iter.iter (fun (idx, le) ->
                 if (Log.get ct.log idx).term < le.term then
                   Log.set ct.log idx le ) ;
        commit ~upto:leader_commit
    (*Invalid or already handled *)
    | _ ->
        ()

  let check_conditions () =
    let ct = A.get t () in
    match ct.node_state with
    (* check if can become leader *)
    | Candidate {quorum; _} when Quorum.satisified quorum ->
        transit_leader ()
    (* send msg if exists entries to send *)
    | Leader _ ->
        send_append_entries ()
    | _ ->
        ()

  let resolve_timeouts () =
    let ct = A.get t () in
    match ct.node_state with
    (* When should ticking result in an action? *)
    | Follower {timeout} when timeout >= ct.config.election_timeout ->
        transit_candidate ()
    | Candidate {timeout; _} when timeout >= ct.config.election_timeout ->
        transit_candidate ()
    | Leader {heartbeat; _} when heartbeat > 0 ->
        send_append_entries () ;
        A.set (t @> node_state @> Leader.heartbeat) ~to_:0 ()
    | _ ->
        ()

  let check_commit () =
    let ct = A.get t () in
    match ct.node_state with
    | Leader {rep_ackd; _} ->
        let acks =
          rep_ackd |> IntMap.to_seq
          |> Seq.map (fun (_, v) -> v)
          |> List.of_seq
          |> List.sort (fun a b -> Int.neg @@ Int.compare a b)
        in
        let majority_rep = List.nth acks (Int.div ct.config.num_nodes 2) in
        commit ~upto:majority_rep
    | _ ->
        ()

  let advance_raw e =
    resolve_event e ;
    resolve_timeouts () ;
    check_conditions () ;
    check_commit ()

  let rec advance t e = run advance_raw t e
end

module Actions = struct
  type s =
    { mutable action_acc: action list
    ; mutable commit_upto: int option
    ; mutable t: t }

  let s_init t = {action_acc= []; commit_upto= None; t}

  let s = ref (s_init @@ assert false)

  let send d m = !s.action_acc <- Send (d, m) :: !s.action_acc

  let broadcast m = !s.action_acc <- Broadcast m :: !s.action_acc

  let commit ~upto =
    match !s.commit_upto with
    | None ->
        !s.commit_upto <- Some upto
    | Some u ->
        !s.commit_upto <- Some (max u upto)

  let t =
    [%accessor A.field ~get:(fun () -> !s.t) ~set:(fun () t' -> !s.t <- t)]

  let run f (t : t) (e : event) =
    s := s_init t ;
    f e ;
    let t = !s.t in
    let actions =
      let command_iter upto =
        Utils.RBuf.pop_iter t.log ~hi:upto |> Iter.map (fun l -> l.command)
      in
      let open Iter in
      let commit_iter =
        of_opt !s.commit_upto >|= fun upto -> CommitCommands (command_iter upto)
      in
      append_l [of_list !s.action_acc; commit_iter] |> Iter.to_rev_list
    in
    (t, actions)
end

module Imperative = Make (Actions)

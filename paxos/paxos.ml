module C = Ocons_core
open! C.Types
open! Utils
module A = Accessor

let ( @> ) = A.( @> )

type message =
  | RequestVote of {term: term; leader_commit: log_index}
  | RequestVoteResponse of
      { term: term
      ; start_index: log_index
      ; commit_index: log_index
      ; entries: log_entry Iter.t * int }
  | AppendEntries of
      { term: term
      ; leader_commit: log_index
      ; prev_log_index: log_index
      ; prev_log_term: term
      ; entries_length: log_index
      ; entries: log_entry list }
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
  | Candidate of {mutable quorum: Quorum.t; mutable timeout: int}
  | Leader of
      { mutable rep_ackd: log_index IntMap.t
      ; mutable rep_sent: log_index IntMap.t
      ; heartbeat: int }
[@@deriving accessors]

type t =
  { log: log_entry Utils.RBuf.t
  ; commit_index: log_index (* Guarantee that [commit_index] is >= log.vlo *)
  ; config: config
  ; node_state: node_state
  ; current_term: term }
[@@deriving accessors]

module type ActionSig = sig
  val send : node_id -> message -> unit

  val broadcast : message -> unit

  val commit : upto:int -> unit

  val t : ('i -> t -> t, 'i -> unit -> unit, [< A.field]) A.General.t

  val run : (event -> unit) -> t -> event -> t * action list
end

module Make (Act : ActionSig) = struct
  type nonrec config = config

  type nonrec message = message

  type nonrec event = event

  type t = {config: config; node_state: node_state}

  open Act

  (*TODO*)
  let transit_follower () = assert false

  (*TODO*)
  let transit_candidate () = assert false

  (*TODO*)
  let make_append_entries () = assert false

  let if_recv_advance_term e =
    match e with
    | Recv ((AppendEntries {term; _} | AppendEntries {term; _} | RequestVote {term; _} | RequestVoteResponse {term; _}), _)
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
            Utils.RBuf.push
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
      when term < t.current_term ->
        ()
    (* Recv msgs from this term*)
    (* Leader *)
    | Recv (RequestVoteResponse _, _), Leader s ->
        () (*Already*)
    | Recv (AppendEntriesResponse _, _), Leader s ->
        assert false
    (* Follower *)
    | Recv (RequestVote, candidate), Follower s ->
        let t = A.get t () in
        let start_index = max t.commit_index (Utils.RBuf.lowest t.log) in
        let entries =
          let log = A.get (t @> log) () in
          Utils.RBuf.iter ~lo:start_index log
        in
        send
        @@ { term= t.current_term
           ; start_index
           ; entries
           ; commit_index= t.commit_index }
    | Recv (AppendEntries _, _), _ ->
        ()
    (*Invalid or already handled *)
    | Recv (RequestVotes, candidate), _ ->
        ()

  let resolve_timeouts () =
    match (get ()).node_state with
    (* When should ticking result in an action? *)
    | Follower {timeout} when timeout >= t.config.election_timeout ->
        transit_candidate ()
    | Candidate {timeout; _} when timeout >= t.config.election_timeout ->
        transit_candidate ()
    | Leader {heartbeat; _} when heartbeat > 0 ->
        broadcast (make_append_entries ()) ;
        update @@ A.set (node_state @> Leader.heartbeat) ~to_:0 (get ())
    | _ ->
        ()

  (*TODO*)
  let check_commit () = assert false

  let advance_raw e = resolve_event e ; resolve_timeouts () ; check_commit ()

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

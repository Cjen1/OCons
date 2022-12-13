module C = Ocons_core
open! C.Types
open! Utils
module A = Accessor
module Log = SegmentLog

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

let message_pp ppf v =
  let open Fmt in
  match v with
  | RequestVote {term; leader_commit} ->
      pf ppf "RequestVote {term:%d; leader_commit:%d}" term leader_commit
  | RequestVoteResponse {term; start_index; entries= _, len} ->
      pf ppf "RequestVoteResponse {term:%d; start_index:%d; entries_length:%d}"
        term start_index len
  | AppendEntries
      {term; leader_commit; prev_log_index; prev_log_term; entries_length; _} ->
      pf ppf
        "AppendEntries {term: %d; leader_commit: %d; prev_log_index: %d; \
         prev_log_term: %d; entries_length: %d}"
        term leader_commit prev_log_index prev_log_term entries_length
  | AppendEntriesResponse {term; success} ->
      pf ppf "AppendEntriesResponse {term: %d; success: %a}" term
        (result
           ~ok:(const string "Ok: " ++ int)
           ~error:(const string "Error: " ++ int) )
        success

type event = Tick | Recv of (message * node_id) | Commands of command iter

let event_pp ppf v =
  let open Fmt in
  match v with
  | Tick ->
      pf ppf "Tick"
  | Recv (m, src) ->
      pf ppf "Recv(%a, %d)" message_pp m src
  | Commands _ ->
      pf ppf "Commands"

type action =
  | Send of node_id * message
  | Broadcast of message
  | CommitCommands of command Iter.t

let action_pp ppf v =
  let open Fmt in
  match v with
  | Send (d, m) ->
      pf ppf "Send(%d, %a)" d message_pp m
  | Broadcast m ->
      pf ppf "Broadcast(%a)" message_pp m
  | CommitCommands _ ->
      pf ppf "CommitCommands"

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

let node_state_pp : node_state Fmt.t =
 fun ppf v ->
  let open Fmt in
  match v with
  | Follower {timeout} ->
      pf ppf "Follower(%d)" timeout
  | Candidate {quorum; timeout} ->
      pf ppf "Candidate{quorum:%a, timeout:%d}" Quorum.pp quorum timeout
  | Leader {rep_ackd; rep_sent; heartbeat} ->
      let format_rep_map : int IntMap.t Fmt.t =
       fun ppf v ->
        let open Fmt in
        let elts = v |> IntMap.bindings in
        pf ppf "%a"
          (brackets @@ list ~sep:comma @@ braces @@ pair ~sep:comma int int)
          elts
      in
      pf ppf "{heartbeat:%d; rep_ackd:%a; rep_sent:%a" heartbeat format_rep_map
        rep_ackd format_rep_map rep_sent

type t =
  { log: log_entry SegmentLog.t
  ; commit_index: log_index (* Guarantee that [commit_index] is >= log.vlo *)
  ; config: config
  ; node_state: node_state
  ; current_term: term }
[@@deriving accessors]

let t_pp : t Fmt.t =
 fun ppf t ->
  Fmt.pf ppf "{log: _; commit_index:%d; current_term: %d; node_state:%a}"
    t.commit_index t.current_term node_state_pp t.node_state

module type ActionSig = sig
  val send : node_id -> message -> unit

  val broadcast : message -> unit

  val commit : upto:int -> unit
  (* if upto is greater than t commit index then update it and mark to emit action *)

  val t : ('i -> t -> t, 'i -> unit -> unit, [< A.field]) A.General.t

  val run : (event -> unit) -> t -> event -> t * action list
end

module ImperativeActions = struct
  type s =
    { mutable action_acc: action list
    ; mutable commit_upto: int option
    ; mutable t: t }

  let s_init t = {action_acc= []; commit_upto= None; t}

  let s = ref (s_init @@ Obj.magic ())

  let send d m = !s.action_acc <- Send (d, m) :: !s.action_acc

  let broadcast m = !s.action_acc <- Broadcast m :: !s.action_acc

  let commit ~upto =
    match !s.commit_upto with
    | None ->
        !s.commit_upto <- Some upto
    | Some u ->
        !s.commit_upto <- Some (max u upto)

  let t =
    [%accessor A.field ~get:(fun () -> !s.t) ~set:(fun () t' -> !s.t <- t')]

  let run f (t : t) (e : event) =
    s := s_init t ;
    let start_upto = t.commit_index in
    f e ;
    let t = !s.t in
    let actions =
      let open Iter in
      let make_command_iter upto =
        Log.iter t.log ~lo:start_upto ~hi:upto |> Iter.map (fun l -> l.command)
      in
      append_l
        [ of_list !s.action_acc
        ; !s.commit_upto |> of_opt |> Iter.map make_command_iter
          |> Iter.map (fun i -> CommitCommands i) ]
      |> Iter.to_rev_list
    in
    (t, actions)
end

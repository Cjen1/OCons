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
      ; entries: log_entry Iter.t * int }
  | AppendEntriesResponse of
      {term: term; success: (log_index, log_index) Result.t}

let message_pp ppf v =
  let open Fmt in
  match v with
  | RequestVote {term; leader_commit} ->
      pf ppf "RequestVote {term:%d; leader_commit:%d}" term leader_commit
  | RequestVoteResponse {term; start_index; entries= entries, len} ->
      pf ppf
        "RequestVoteResponse {term:%d; start_index:%d; entries_length:%d; \
         entries: %a}"
        term start_index len
        (brackets @@ list ~sep:(const char ',') log_entry_pp)
        (entries |> Iter.to_list)
  | AppendEntries {term; leader_commit; prev_log_index; prev_log_term; entries}
    ->
      pf ppf
        "AppendEntries {term: %d; leader_commit: %d; prev_log_index: %d; \
         prev_log_term: %d; entries_length: %d; entries: %a}"
        term leader_commit prev_log_index prev_log_term (snd entries)
        (brackets @@ list ~sep:(const char ',') log_entry_pp)
        (fst entries |> Iter.to_list)
  | AppendEntriesResponse {term; success} ->
      pf ppf "AppendEntriesResponse {term: %d; success: %a}" term
        (result
           ~ok:(const string "Ok: " ++ int)
           ~error:(const string "Error: " ++ int) )
        success

type event = Tick | Recv of (message * node_id) | Commands of command Iter.t

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
  | CommitCommands i ->
      pf ppf "CommitCommands[%a]" (list ~sep:sp Command.pp) (i |> Iter.to_list)

type config =
  { phase1quorum: int
  ; phase2quorum: int
  ; other_nodes: node_id list
  ; num_nodes: int
  ; node_id: node_id
  ; election_timeout: int
  ; max_outstanding: int }
[@@deriving accessors]

let config_pp : config Fmt.t =
 fun ppf v ->
  let open Fmt in
  pf ppf "{P1Q:%d,P2Q:%d,#Nodes:%d,Id:%d,eT:%d,mO:%d,%a}" v.phase1quorum
    v.phase2quorum v.num_nodes v.node_id v.election_timeout v.max_outstanding
    (braces @@ list ~sep:comma int)
    v.other_nodes

let make_config ~node_id ~node_list ~election_timeout ?(max_outstanding = 8192)
    () =
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
  ; election_timeout
  ; max_outstanding }

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
      pf ppf "Leader{heartbeat:%d; rep_ackd:%a; rep_sent:%a" heartbeat
        format_rep_map rep_ackd format_rep_map rep_sent

type t =
  { log: log_entry SegmentLog.t
  ; commit_index: log_index (* Guarantee that [commit_index] is >= log.vlo *)
  ; config: config
  ; node_state: node_state
  ; current_term: term }
[@@deriving accessors]

let create config =
  let log = SegmentLog.create {term= -1; command= empty_command} in
  { log
  ; commit_index= -1
  ; config
  ; node_state= Follower {timeout= 0}
  ; current_term= 0 }

let t_pp : t Fmt.t =
 fun ppf t ->
  Fmt.pf ppf "{log: %a; commit_index:%d; current_term: %d; node_state:%a}"
    Fmt.(brackets @@ list ~sep:(const char ',') log_entry_pp)
    (t.log |> Log.iter |> Iter.to_list)
    t.commit_index t.current_term node_state_pp t.node_state

module type ActionSig = sig
  val send : node_id -> message -> unit

  val broadcast : message -> unit

  val t : ('i -> t -> t, 'i -> unit -> unit, [< A.field]) A.General.t

  val run_side_effects : (unit -> unit) -> t -> t * action list
end

module ImperativeActions : ActionSig = struct
  type s =
    {mutable action_acc: action list; mutable starting_cid: int; mutable t: t}

  let s = ref None

  let s_init t = {action_acc= []; starting_cid= t.commit_index; t}

  let send d m =
    (!s |> Option.get).action_acc <-
      Send (d, m) :: (!s |> Option.get).action_acc

  let broadcast m =
    (!s |> Option.get).action_acc <-
      Broadcast m :: (!s |> Option.get).action_acc

  let t =
    [%accessor
      A.field
        ~get:(fun () -> (!s |> Option.get).t)
        ~set:(fun () t' -> (!s |> Option.get).t <- t')]

  let get_actions init_commit_index =
    let open Iter in
    let commit_upto =
      let ct = (!s |> Option.get).t in
      if ct.commit_index > (!s |> Option.get).starting_cid then
        Some ct.commit_index
      else None
    in
    let make_command_iter upto =
      Log.iter (!s |> Option.get).t.log ~lo:init_commit_index ~hi:upto
      |> Iter.map (fun l -> l.command)
    in
    append_l
      [ of_list (!s |> Option.get).action_acc
      ; commit_upto |> of_opt |> Iter.map make_command_iter
        |> Iter.map (fun i -> CommitCommands i) ]
    |> Iter.to_rev_list

  let run_side_effects f t =
    s := Some (s_init t) ;
    let init_commit_index = t.commit_index in
    f () ;
    ((!s |> Option.get).t, get_actions init_commit_index)
end

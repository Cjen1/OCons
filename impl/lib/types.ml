module C = Ocons_core
open! Utils
include C.Types
module A = Accessor
module Log = SegmentLog

let ( @> ) = A.( @> )

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

module PaxosTypes = struct
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

  type event = Tick | Recv of (message * node_id) | Commands of command Iter.t

  type action =
    | Send of node_id * message
    | Broadcast of message
    | CommitCommands of command Iter.t

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

  let timeout_a =
    [%accessor
      A.field
        ~get:(function
          | Follower {timeout} ->
              timeout
          | Candidate {timeout; _} ->
              timeout
          | Leader {heartbeat; _} ->
              heartbeat )
        ~set:(fun ns v ->
          match ns with
          | Follower _ ->
              A.set Follower.timeout ~to_:v ns
          | Candidate _ ->
              A.set Candidate.timeout ~to_:v ns
          | Leader _ ->
              A.set Leader.heartbeat ~to_:v ns )]

  type t =
    { log: log_entry SegmentLog.t
    ; commit_index: log_index (* Guarantee that [commit_index] is >= log.vlo *)
    ; config: config
    ; node_state: node_state
    ; current_term: term
    ; append_entries_length: int Ocons_core.Utils.InternalReporter.reporter }
  [@@deriving accessors]

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

  let event_pp ppf v =
    let open Fmt in
    match v with
    | Tick ->
        pf ppf "Tick"
    | Recv (m, src) ->
        pf ppf "Recv(%a, %d)" message_pp m src
    | Commands _ ->
        pf ppf "Commands"

  let action_pp ppf v =
    let open Fmt in
    match v with
    | Send (d, m) ->
        pf ppf "Send(%d, %a)" d message_pp m
    | Broadcast m ->
        pf ppf "Broadcast(%a)" message_pp m
    | CommitCommands i ->
        pf ppf "CommitCommands[%a]" (list ~sep:sp Command.pp) (i |> Iter.to_list)

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

  let t_pp : t Fmt.t =
   fun ppf t ->
    Fmt.pf ppf "{log: %a; commit_index:%d; current_term: %d; node_state:%a}"
      Fmt.(brackets @@ list ~sep:(const char ',') log_entry_pp)
      (t.log |> Log.iter |> Iter.to_list)
      t.commit_index t.current_term node_state_pp t.node_state
end

module RaftTypes = struct
  type message =
    | RequestVote of {term: term; lastIndex: log_index; lastTerm: term}
    | RequestVoteResponse of {term: term; success: bool}
    | AppendEntries of
        { term: term
        ; leader_commit: log_index
        ; prev_log_index: log_index
        ; prev_log_term: term
        ; entries: log_entry Iter.t * int }
    | AppendEntriesResponse of
        {term: term; success: (log_index, log_index) Result.t}

  type event = Tick | Recv of (message * node_id) | Commands of command Iter.t

  type action =
    | Send of node_id * message
    | Broadcast of message
    | CommitCommands of command Iter.t

  type node_state =
    | Follower of {timeout: int; voted_for: node_id option}
    | Candidate of {mutable quorum: unit Quorum.t; mutable timeout: int}
    | Leader of
        { mutable rep_ackd: log_index IntMap.t (* MatchIdx *)
        ; mutable rep_sent: log_index IntMap.t (* NextIdx *)
        ; heartbeat: int }
  [@@deriving accessors]

  let timeout_a =
    [%accessor
      A.field
        ~get:(function
          | Follower {timeout; _} ->
              timeout
          | Candidate {timeout; _} ->
              timeout
          | Leader {heartbeat; _} ->
              heartbeat )
        ~set:(fun ns v ->
          match ns with
          | Follower _ ->
              A.set Follower.timeout ~to_:v ns
          | Candidate _ ->
              A.set Candidate.timeout ~to_:v ns
          | Leader _ ->
              A.set Leader.heartbeat ~to_:v ns )]

  type t =
    { log: log_entry SegmentLog.t
    ; commit_index: log_index (* Guarantee that [commit_index] is >= log.vlo *)
    ; config: config
    ; node_state: node_state
    ; current_term: term
    ; append_entries_length: int Ocons_core.Utils.InternalReporter.reporter }
  [@@deriving accessors]

  let message_pp ppf v =
    let open Fmt in
    match v with
    | RequestVote {term; lastIndex; lastTerm} ->
        pf ppf "RequestVote {term:%d; lastIndex:%d; lastTerm:%d}" term lastIndex
          lastTerm
    | RequestVoteResponse {term; success} ->
        pf ppf "RequestVoteResponse {term:%d; success:%b}" term success
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

  let event_pp ppf v =
    let open Fmt in
    match v with
    | Tick ->
        pf ppf "Tick"
    | Recv (m, src) ->
        pf ppf "Recv(%a, %d)" message_pp m src
    | Commands _ ->
        pf ppf "Commands"

  let action_pp ppf v =
    let open Fmt in
    match v with
    | Send (d, m) ->
        pf ppf "Send(%d, %a)" d message_pp m
    | Broadcast m ->
        pf ppf "Broadcast(%a)" message_pp m
    | CommitCommands i ->
        pf ppf "CommitCommands[%a]" (list ~sep:sp Command.pp) (i |> Iter.to_list)

  let node_state_pp : node_state Fmt.t =
   fun ppf v ->
    let open Fmt in
    match v with
    | Follower {timeout; voted_for} ->
        pf ppf "Follower{timeout:%d; voted_for:%a}" timeout
          Fmt.(option ~none:(const string "None") int)
          voted_for
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

  let t_pp : t Fmt.t =
   fun ppf t ->
    Fmt.pf ppf "{log: %a; commit_index:%d; current_term: %d; node_state:%a}"
      Fmt.(brackets @@ list ~sep:(const char ',') log_entry_pp)
      (t.log |> Log.iter |> Iter.to_list)
      t.commit_index t.current_term node_state_pp t.node_state
end

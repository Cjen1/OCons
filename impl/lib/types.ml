module C = Ocons_core
open! Utils
include C.Types
module A = Accessor
open A.O
module Log = SegmentLog

type config =
  { phase1quorum: int
  ; phase2quorum: int
  ; other_nodes: node_id list
  ; num_nodes: int
  ; node_id: node_id
  ; election_timeout: int
  ; max_outstanding: int
  ; max_append_entries: int }
[@@deriving accessors]

let config_pp : config Fmt.t =
 fun ppf v ->
  let open Fmt in
  pf ppf "{P1Q:%d,P2Q:%d,#Nodes:%d,Id:%d,eT:%d,mO:%d,%a}" v.phase1quorum
    v.phase2quorum v.num_nodes v.node_id v.election_timeout v.max_outstanding
    (braces @@ list ~sep:comma int)
    v.other_nodes

let make_config ~node_id ~node_list ~election_timeout ?(max_outstanding = 8192)
    ?(max_append_entries = 512) () =
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
  ; max_outstanding
  ; max_append_entries }

let get_log_term log idx = if idx < 0 then 0 else (Log.get log idx).term

module type StrategyTypes = sig
  type request_vote

  type request_vote_response

  type quorum_type

  module Utils : sig
    val request_vote_pp : request_vote Fmt.t

    val request_vote_response_pp : request_vote_response Fmt.t

    val term_rv :
      ( 'a -> int -> int
      , 'a -> request_vote -> request_vote
      , [< A.field] )
      A.General.t

    val term_rvr :
      ( 'a -> int -> int
      , 'a -> request_vote_response -> request_vote_response
      , [< A.field] )
      A.General.t

    val parse_request_vote : request_vote Eio.Buf_read.parser

    val serialise_request_vote : request_vote -> Eio.Buf_write.t -> unit

    val parse_request_vote_response : request_vote_response Eio.Buf_read.parser

    val serialise_request_vote_response :
      request_vote_response -> Eio.Buf_write.t -> unit
  end
end

module type ImplTypes = sig
  type request_vote

  type request_vote_response

  type quorum_type

  module Utils : sig
    val request_vote_pp : request_vote Fmt.t

    val request_vote_response_pp : request_vote_response Fmt.t

    val term_rv :
      ( 'a -> term -> term
      , 'a -> request_vote -> request_vote
      , [< A.field] )
      A.General.t

    val term_rvr :
      ( 'a -> term -> term
      , 'a -> request_vote_response -> request_vote_response
      , [< A.field] )
      A.General.t
  end

  type message =
    | RequestVote of request_vote
    | RequestVoteResponse of request_vote_response
    | AppendEntries of
        { term: term
        ; leader_commit: term
        ; prev_log_index: term
        ; prev_log_term: term
        ; entries: log_entry Iter.t * term }
    | AppendEntriesResponse of
        {term: term; success: (term, term) result; trace: time}

  val term_a : ('a -> term -> 'b, 'a -> message -> 'c, [< A.getter]) A.General.t

  type node_state =
    | Follower of {timeout: term; voted_for: term option}
    | Candidate of {mutable quorum: quorum_type Quorum.t; mutable timeout: term}
    | Leader of
        { mutable rep_ackd: term IntMap.t
        ; mutable rep_sent: term IntMap.t
        ; heartbeat: term }
  [@@deriving accessors]

  val timeout_a :
    ( 'a -> term -> term
    , 'a -> node_state -> node_state
    , [< A.field] )
    A.General.t

  type t =
    { log: log_entry Log.t
    ; commit_index: term
    ; config: config
    ; node_state: node_state
    ; current_term: term
    ; append_entries_length: term C.Utils.InternalReporter.reporter }
  [@@deriving accessors]

  val message_pp : Format.formatter -> message -> unit

  val node_state_pp : node_state Fmt.t

  val t_pp : t Fmt.t
end

module VarImplTypes (StrategyTypes : StrategyTypes) :
  ImplTypes
    with type request_vote = StrategyTypes.request_vote
     and type request_vote_response = StrategyTypes.request_vote_response
     and type quorum_type = StrategyTypes.quorum_type = struct
  include StrategyTypes

  type message =
    | RequestVote of StrategyTypes.request_vote
    | RequestVoteResponse of StrategyTypes.request_vote_response
    | AppendEntries of
        { term: term
        ; leader_commit: log_index
        ; prev_log_index: log_index
        ; prev_log_term: term
        ; entries: log_entry Iter.t * int }
    | AppendEntriesResponse of
        {term: term; success: (log_index, log_index) Result.t; trace: time}

  type node_state =
    | Follower of {timeout: int; voted_for: node_id option}
    | Candidate of {mutable quorum: quorum_type Quorum.t; mutable timeout: int}
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
    ; current_term: term
    ; append_entries_length: int Ocons_core.Utils.InternalReporter.reporter }
  [@@deriving accessors]

  let term_a =
    [%accessor
      A.getter (function
        | RequestVote s ->
            s.@(StrategyTypes.Utils.term_rv)
        | RequestVoteResponse s ->
            s.@(StrategyTypes.Utils.term_rvr)
        | AppendEntriesResponse {term; _} | AppendEntries {term; _} ->
            term )]

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
              ns.@(Follower.timeout) <- v
          | Candidate _ ->
              ns.@(Candidate.timeout) <- v
          | Leader _ ->
              ns.@(Leader.heartbeat) <- v )]

  let message_pp ppf v =
    let open Fmt in
    match v with
    | RequestVote s ->
        pf ppf "RequestVote %a" StrategyTypes.Utils.request_vote_pp s
    | RequestVoteResponse s ->
        pf ppf "RequestVoteResponse %a"
          StrategyTypes.Utils.request_vote_response_pp s
    | AppendEntries {term; leader_commit; prev_log_index; prev_log_term; entries}
      ->
        pf ppf
          "AppendEntries {term: %d; leader_commit: %d; prev_log_index: %d; \
           prev_log_term: %d; entries_length: %d; entries: %a}"
          term leader_commit prev_log_index prev_log_term (snd entries)
          (brackets @@ list ~sep:(const char ',') log_entry_pp)
          (fst entries |> Iter.to_list)
    | AppendEntriesResponse {term; success; _} ->
        pf ppf "AppendEntriesResponse {term: %d; success: %a}" term
          (result
             ~ok:(const string "Ok: " ++ int)
             ~error:(const string "Error: " ++ int) )
          success

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

module type Strategy = sig
  module StrategyTypes : StrategyTypes

  open StrategyTypes

  val increment_term : unit -> term

  val send_request_vote : unit -> unit

  val leader_transit_log_update : unit -> unit

  val candidate_tick : unit -> unit

  val recv_request_vote : request_vote * node_id -> unit

  val recv_request_vote_response : request_vote_response * node_id -> unit

  val rep_sent_default : unit -> log_index
end

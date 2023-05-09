open Types
open! Utils
module CommonPrim = Ocons_core.Line_prot.CommonPrim
module SerPrim = Ocons_core.Line_prot.SerPrim
module DeserPrim = Ocons_core.Line_prot.DeserPrim
module W = Eio.Buf_write
module R = Eio.Buf_read

module VarLineProt
    (ST : StrategyTypes)
    (IT : Types.ImplTypes
            with type request_vote = ST.request_vote
             and type request_vote_response = ST.request_vote_response) =
struct
  open IT

  let enum_msg = function
    | RequestVote _ ->
        0
    | RequestVoteResponse _ ->
        1
    | AppendEntries _ ->
        2
    | AppendEntriesResponse _ ->
        3

  let serialise m w =
    W.uint8 w (enum_msg m) ;
    let open Int64 in
    match m with
    | RequestVote s ->
        ST.Utils.serialise_request_vote s w
    | RequestVoteResponse s ->
        ST.Utils.serialise_request_vote_response s w
    | AppendEntries {term; leader_commit; prev_log_index; prev_log_term; entries}
      ->
        W.BE.uint64 w (of_int term) ;
        W.BE.uint64 w (of_int leader_commit) ;
        W.BE.uint64 w (of_int prev_log_index) ;
        W.BE.uint64 w (of_int prev_log_term) ;
        SerPrim.entries entries w
    | AppendEntriesResponse {term; success; trace} -> (
        W.BE.uint64 w (of_int term) ;
        W.BE.double w trace ;
        match success with
        | Ok i ->
            W.uint8 w 0 ;
            W.BE.uint64 w (of_int i)
        | Error i ->
            W.uint8 w 1 ;
            W.BE.uint64 w (of_int i) )

  let parse =
    let uint64 = R.map Int64.to_int R.BE.uint64 in
    let open R.Syntax in
    let* msg_code = R.map Char.code R.any_char in
    match msg_code with
    | 0 (* RequestVote *) ->
        let* s = ST.Utils.parse_request_vote in
        R.return @@ RequestVote s
    | 1 (* RequestVoteResponse *) ->
        let* s = ST.Utils.parse_request_vote_response in
        R.return @@ RequestVoteResponse s
    | 2 (* AppendEntires *) ->
        let* term = uint64
        and* leader_commit = uint64
        and* prev_log_index = uint64
        and* prev_log_term = uint64
        and* entries = DeserPrim.entries in
        R.return
        @@ AppendEntries
             {term; leader_commit; prev_log_index; prev_log_term; entries}
    | 3 (* AppendEntriesResponse *) ->
        let* term = uint64
        and* trace = R.BE.double
        and* success = R.map Char.code R.any_char
        and* index = uint64 in
        R.return
        @@ AppendEntriesResponse
             { term
             ; success= (if success = 0 then Ok index else Error index)
             ; trace }
    | _ ->
        raise
        @@ Invalid_argument
             (Fmt.str "Received %d which is not a valid msg_code" msg_code)
end

module Paxos = struct
  open Paxos.Types

  let enum_msg = function
    | RequestVote _ ->
        0
    | RequestVoteResponse _ ->
        1
    | AppendEntries _ ->
        2
    | AppendEntriesResponse _ ->
        3

  let serialise m w =
    W.uint8 w (enum_msg m) ;
    let open Int64 in
    match m with
    | RequestVote {term; leader_commit} ->
        W.BE.uint64 w (of_int term) ;
        W.BE.uint64 w (of_int leader_commit)
    | RequestVoteResponse {term; start_index; entries} ->
        W.BE.uint64 w (of_int term) ;
        W.BE.uint64 w (of_int start_index) ;
        SerPrim.entries entries w
    | AppendEntries {term; leader_commit; prev_log_index; prev_log_term; entries}
      ->
        W.BE.uint64 w (of_int term) ;
        W.BE.uint64 w (of_int leader_commit) ;
        W.BE.uint64 w (of_int prev_log_index) ;
        W.BE.uint64 w (of_int prev_log_term) ;
        SerPrim.entries entries w
    | AppendEntriesResponse {term; success; trace} -> (
        W.BE.uint64 w (of_int term) ;
        W.BE.double w trace ;
        match success with
        | Ok i ->
            W.uint8 w 0 ;
            W.BE.uint64 w (of_int i)
        | Error i ->
            W.uint8 w 1 ;
            W.BE.uint64 w (of_int i) )

  let parse =
    let uint64 = R.map Int64.to_int R.BE.uint64 in
    let open R.Syntax in
    let* msg_code = R.map Char.code R.any_char in
    match msg_code with
    | 0 (* RequestVote *) ->
        let* term = uint64 and* leader_commit = uint64 in
        R.return @@ RequestVote {term; leader_commit}
    | 1 (* RequestVoteResponse *) ->
        let* term = uint64
        and* start_index = uint64
        and* entries = DeserPrim.entries in
        R.return @@ RequestVoteResponse {term; start_index; entries}
    | 2 (* AppendEntires *) ->
        let* term = uint64
        and* leader_commit = uint64
        and* prev_log_index = uint64
        and* prev_log_term = uint64
        and* entries = DeserPrim.entries in
        R.return
        @@ AppendEntries
             {term; leader_commit; prev_log_index; prev_log_term; entries}
    | 3 (* AppendEntriesResponse *) ->
        let* term = uint64
        and* trace = R.BE.double
        and* success = R.map Char.code R.any_char
        and* index = uint64 in
        R.return
        @@ AppendEntriesResponse
             { term
             ; success= (if success = 0 then Ok index else Error index)
             ; trace }
    | _ ->
        raise
        @@ Invalid_argument
             (Fmt.str "Received %d which is not a valid msg_code" msg_code)
end

module Raft = struct
  open Raft.Types

  let enum_msg = function
    | RequestVote _ ->
        0
    | RequestVoteResponse _ ->
        1
    | AppendEntries _ ->
        2
    | AppendEntriesResponse _ ->
        3

  let serialise m w =
    W.uint8 w (enum_msg m) ;
    let open Int64 in
    match m with
    | RequestVote {term; lastIndex; lastTerm} ->
        W.BE.uint64 w (of_int term) ;
        W.BE.uint64 w (of_int lastIndex) ;
        W.BE.uint64 w (of_int lastTerm)
    | RequestVoteResponse {term; success} ->
        W.BE.uint64 w (of_int term) ;
        SerPrim.bool success w
    | AppendEntries {term; leader_commit; prev_log_index; prev_log_term; entries}
      ->
        W.BE.uint64 w (of_int term) ;
        W.BE.uint64 w (of_int leader_commit) ;
        W.BE.uint64 w (of_int prev_log_index) ;
        W.BE.uint64 w (of_int prev_log_term) ;
        SerPrim.entries entries w
    | AppendEntriesResponse {term; success} -> (
        W.BE.uint64 w (of_int term) ;
        match success with
        | Ok i ->
            W.uint8 w 0 ;
            W.BE.uint64 w (of_int i)
        | Error i ->
            W.uint8 w 1 ;
            W.BE.uint64 w (of_int i) )

  let parse =
    let uint64 = R.map Int64.to_int R.BE.uint64 in
    let open R.Syntax in
    let* msg_code = R.map Char.code R.any_char in
    match msg_code with
    | 0 (* RequestVote *) ->
        let* term = uint64 and* lastIndex = uint64 and* lastTerm = uint64 in
        R.return @@ RequestVote {term; lastIndex; lastTerm}
    | 1 (* RequestVoteResponse *) ->
        let* term = uint64 and* success = DeserPrim.bool in
        R.return @@ RequestVoteResponse {term; success}
    | 2 (* AppendEntires *) ->
        let* term = uint64
        and* leader_commit = uint64
        and* prev_log_index = uint64
        and* prev_log_term = uint64
        and* entries = DeserPrim.entries in
        R.return
        @@ AppendEntries
             {term; leader_commit; prev_log_index; prev_log_term; entries}
    | 3 (* AppendEntriesResponse *) ->
        let* term = uint64
        and* success = R.map Char.code R.any_char
        and* index = uint64 in
        R.return
        @@ AppendEntriesResponse
             {term; success= (if success = 0 then Ok index else Error index)}
    | _ ->
        raise
        @@ Invalid_argument
             (Fmt.str "Received %d which is not a valid msg_code" msg_code)
end

module PrevoteRaft = struct
  open Prevote.Types

  let enum_msg = function
    | RequestVote _ ->
        0
    | RequestVoteResponse _ ->
        1
    | AppendEntries _ ->
        2
    | AppendEntriesResponse _ ->
        3

  let serialise m w =
    W.uint8 w (enum_msg m) ;
    let open Int64 in
    match m with
    | RequestVote {term; lastIndex; lastTerm; prevote} ->
        W.BE.uint64 w (of_int term) ;
        W.BE.uint64 w (of_int lastIndex) ;
        W.BE.uint64 w (of_int lastTerm) ;
        SerPrim.bool prevote w
    | RequestVoteResponse {term; success; prevote} ->
        W.BE.uint64 w (of_int term) ;
        SerPrim.bool success w ;
        SerPrim.bool prevote w
    | AppendEntries {term; leader_commit; prev_log_index; prev_log_term; entries}
      ->
        W.BE.uint64 w (of_int term) ;
        W.BE.uint64 w (of_int leader_commit) ;
        W.BE.uint64 w (of_int prev_log_index) ;
        W.BE.uint64 w (of_int prev_log_term) ;
        SerPrim.entries entries w
    | AppendEntriesResponse {term; success} -> (
        W.BE.uint64 w (of_int term) ;
        match success with
        | Ok i ->
            W.uint8 w 0 ;
            W.BE.uint64 w (of_int i)
        | Error i ->
            W.uint8 w 1 ;
            W.BE.uint64 w (of_int i) )

  let parse =
    let uint64 = R.map Int64.to_int R.BE.uint64 in
    let open R.Syntax in
    let* msg_code = R.map Char.code R.any_char in
    match msg_code with
    | 0 (* RequestVote *) ->
        let* term = uint64
        and* lastIndex = uint64
        and* lastTerm = uint64
        and* prevote = DeserPrim.bool in
        R.return @@ RequestVote {term; lastIndex; lastTerm; prevote}
    | 1 (* RequestVoteResponse *) ->
        let* term = uint64
        and* success = DeserPrim.bool
        and* prevote = DeserPrim.bool in
        R.return @@ RequestVoteResponse {term; success; prevote}
    | 2 (* AppendEntires *) ->
        let* term = uint64
        and* leader_commit = uint64
        and* prev_log_index = uint64
        and* prev_log_term = uint64
        and* entries = DeserPrim.entries in
        R.return
        @@ AppendEntries
             {term; leader_commit; prev_log_index; prev_log_term; entries}
    | 3 (* AppendEntriesResponse *) ->
        let* term = uint64
        and* success = R.map Char.code R.any_char
        and* index = uint64 in
        R.return
        @@ AppendEntriesResponse
             {term; success= (if success = 0 then Ok index else Error index)}
    | _ ->
        raise
        @@ Invalid_argument
             (Fmt.str "Received %d which is not a valid msg_code" msg_code)
end

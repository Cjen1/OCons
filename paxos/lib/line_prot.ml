open Types
open! Utils
module W = Eio.Buf_write
module R = Eio.Buf_read

module SerPrim = struct
  let string s w =
    W.BE.uint64 w (s |> String.length |> Int64.of_int) ;
    W.string w s

  let enum_sm_op =
    let open C.Types in
    function Read _ -> 0 | Write _ -> 1 | CAS _ -> 2 | NoOp -> 3

  let sm_op op w =
    let open C.Types in
    W.uint8 w (enum_sm_op op) ;
    match op with
    | Read k ->
        string k w
    | Write (k, v) ->
        string k w ; string v w
    | CAS {key; value; value'} ->
        string key w ; string value w ; string value' w
    | NoOp ->
        ()

  let command C.Types.Command.{op; id} w =
    W.BE.uint64 w (Int64.of_int id) ;
    sm_op op w

  let serialise_entries (es, length) w =
    W.BE.uint64 w (Int64.of_int length) ;
    es
    |> Iter.iter (fun C.Types.{term; command= cmd} ->
           W.BE.uint64 w (Int64.of_int term) ;
           command cmd w )
end

module DeserPrim = struct
  open! R.Syntax
end

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
  | RequestVoteResponse {term; start_index; entries= entries, length} ->
      W.BE.uint64 w (of_int term) ;
      W.BE.uint64 w (of_int start_index) ;
      W.BE.uint64 w (of_int length) ;
      SerPrim.serialise_entries (entries, length) w
  | AppendEntries _ | AppendEntriesResponse _ ->
      assert false

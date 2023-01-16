open Types
open! Utils
module W = Eio.Buf_write
module R = Eio.Buf_read

module CommonPrim = struct
  let sm_op_to_uint8 =
    let open C.Types in
    function Read _ -> 0 | Write _ -> 1 | CAS _ -> 2 | NoOp -> 3
end

module SerPrim = struct
  let string s w =
    W.BE.uint64 w (s |> String.length |> Int64.of_int) ;
    W.string w s

  let sm_op op w =
    let open C.Types in
    W.uint8 w (CommonPrim.sm_op_to_uint8 op) ;
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

  let entries (es, length) w =
    W.BE.uint64 w (Int64.of_int length) ;
    es
    |> Iter.iter (fun C.Types.{term; command= cmd} ->
           W.BE.uint64 w (Int64.of_int term) ;
           command cmd w )
end

module DeserPrim = struct
  open! R.Syntax

  type 'a parser = 'a R.parser

  let string : string parser =
    let* len = R.BE.uint64 in
    R.take (Int64.to_int len)

  let sm_op : C.Types.sm_op parser =
    let* op_code = R.map Char.code R.any_char in
    match op_code with
    | 0 (* Read *) ->
        let* k = string in
        R.return @@ C.Types.Read k
    | 1 (* Write *) ->
        let* k = string and* v = string in
        R.return @@ C.Types.Write (k, v)
    | 2 (* CAS *) ->
        let* key = string and* value = string and* value' = string in
        R.return @@ C.Types.(CAS {key; value; value'})
    | 3 (* NoOp *) ->
        R.return @@ C.Types.NoOp
    | _ ->
        raise
          (Invalid_argument
             (Fmt.str "Received %d which is not a valid op_code" op_code) )

  let command =
    let* id = R.map Int64.to_int R.BE.uint64 in
    let* op = sm_op in
    R.return C.Types.Command.{op; id}

  let entries r =
    let len = R.BE.uint64 r |> Int64.to_int in
    let arr =
      Array.init len (fun _ -> C.Types.{term= -1; command= empty_command})
    in
    for i = 0 to len - 1 do
      let term = R.BE.uint64 r |> Int64.to_int in
      let command = command r in
      Array.set arr i C.Types.{term; command}
    done ;
    (Iter.of_array arr, len)
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
      let* term = uint64 and* leader_commit = uint64 in
      R.return @@ RequestVote {term; leader_commit}
  | 1 (* RequestVoteResponse *) ->
      let* term = uint64 
      and* start_index = uint64 
      and* entries = DeserPrim.entries
      in
      R.return @@ RequestVoteResponse {term;start_index;entries}
  | 2 (* AppendEntires *) ->
      let* term = uint64
      and* leader_commit = uint64
      and* prev_log_index = uint64
      and* prev_log_term = uint64
      and* entries = DeserPrim.entries
      in
      R.return @@ AppendEntries {term;leader_commit; prev_log_index; prev_log_term; entries}
  | 3 (* AppendEntriesResponse *) ->
      let* term = uint64
      and* success = R.map Char.code R.any_char
      and* index = uint64
      in R.return @@ AppendEntriesResponse {term; success = if success = 0 then Ok index else Error index}
  | _ -> raise @@ 
          (Invalid_argument
             (Fmt.str "Received %d which is not a valid msg_code" msg_code) )

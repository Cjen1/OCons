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

module Size = struct
  let string s = 8 + String.length s

  let sm_op op =
    let open C.Types in
    1
    +
    match op with
    | Read k ->
        string k
    | Write (k, v) ->
        string k + string v
    | CAS {key; value; value'} ->
        string key + string value + string value'
    | NoOp ->
        0

  let command C.Types.Command.{op; _} = 8 + sm_op op

  let entries (es, _) =
    8 + Iter.fold (fun s C.Types.{command= cmd; _} -> s + 8 + command cmd) 0 es
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
  | RequestVoteResponse {term; start_index; entries= entries, length} ->
      W.BE.uint64 w (of_int term) ;
      W.BE.uint64 w (of_int start_index) ;
      W.BE.uint64 w (of_int length) ;
      SerPrim.entries (entries, length) w
  | AppendEntries _ | AppendEntriesResponse _ ->
      assert false

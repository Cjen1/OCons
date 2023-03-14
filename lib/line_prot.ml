open! Types
module W = Eio.Buf_write
module R = Eio.Buf_read

module CommonPrim = struct
  let sm_op_to_uint8 =
    let open Types in
    function Read _ -> 0 | Write _ -> 1 | CAS _ -> 2 | NoOp -> 3
end

module SerPrim = struct
  let bool t w = match t with true -> W.uint8 w 1 | false -> W.uint8 w 0

  let string s w =
    W.BE.uint64 w (s |> String.length |> Int64.of_int) ;
    W.string w s

  let sm_op op w =
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

  let command Command.{op; id; trace_start} w =
    W.BE.uint64 w (Int64.of_int id) ;
    W.BE.uint64 w (Mtime.to_uint64_ns trace_start) ;
    sm_op op w

  let entries (es, length) w =
    W.BE.uint64 w (Int64.of_int length) ;
    es
    |> Iter.iter (fun {term; command= cmd} ->
           W.BE.uint64 w (Int64.of_int term) ;
           command cmd w )
end

module DeserPrim = struct
  open! R.Syntax

  type 'a parser = 'a R.parser

  let bool : bool parser =
    let+ i = R.uint8 in
    match i with
    | 0 ->
        false
    | 1 ->
        true
    | _ ->
        Fmt.failwith "Received %d which is not 0 or 1" i

  let string : string parser =
    let* len = R.BE.uint64 in
    R.take (Int64.to_int len)

  let sm_op : sm_op parser =
    let* op_code = R.map Char.code R.any_char in
    match op_code with
    | 0 (* Read *) ->
        let* k = string in
        R.return @@ Read k
    | 1 (* Write *) ->
        let* k = string and* v = string in
        R.return @@ Write (k, v)
    | 2 (* CAS *) ->
        let* key = string and* value = string and* value' = string in
        R.return @@ CAS {key; value; value'}
    | 3 (* NoOp *) ->
        R.return @@ NoOp
    | _ ->
        raise
          (Invalid_argument
             (Fmt.str "Received %d which is not a valid op_code" op_code) )

  let command =
    let* id = R.map Int64.to_int R.BE.uint64
    and* trace_start = R.map Mtime.of_uint64_ns R.BE.uint64
    and* op = sm_op in
    R.return Command.{op; id; trace_start}

  let entries r =
    let len = R.BE.uint64 r |> Int64.to_int in
    if len < 0 then
      raise
      @@ Fmt.invalid_arg "Len less than 0: %d\n %a" len Fmt.exn_backtrace
           (Invalid_argument "", Printexc.get_callstack 10) ;
    let arr = Array.init len (fun _ -> {term= -1; command= empty_command}) in
    for i = 0 to len - 1 do
      let term = R.BE.uint64 r |> Int64.to_int in
      let command = command r in
      Array.set arr i {term; command}
    done ;
    (Iter.of_array arr, len)
end

module External_infra = struct
  type request = command

  let serialise_request = SerPrim.command

  let parse_request = DeserPrim.command

  type response = command_id * op_result * Mtime.t

  let response_pp : response Fmt.t =
   fun ppf (id, res, _) ->
    Fmt.pf ppf "{id:%d; res:%a}" id Types.op_result_pp res

  let op_result_to_enum = function
    | Success ->
        0
    | Failure _ ->
        1
    | ReadSuccess _ ->
        2

  let serialise_response (id, res, time) w =
    W.BE.uint64 w (Int64.of_int id) ;
    W.BE.uint64 w (Mtime.to_uint64_ns time) ;
    W.uint8 w (op_result_to_enum res) ;
    match res with
    | Success ->
        ()
    | Failure k ->
        SerPrim.string k w
    | ReadSuccess v ->
        SerPrim.string v w

  let parse_response : response R.parser =
    let open R.Syntax in
    let* id = R.map Int64.to_int R.BE.uint64
    and* time = R.map Mtime.of_uint64_ns R.BE.uint64
    and* res_code = R.map Char.code R.any_char in
    match res_code with
    | 0 (*Success*) ->
        R.return (id, Success, time)
    | 1 (*Failure*) ->
        let* msg = DeserPrim.string in
        R.return (id, Failure msg, time)
    | 2 (*ReadSuccess*) ->
        let* v = DeserPrim.string in
        R.return (id, ReadSuccess v, time)
    | _ ->
        raise
        @@ Invalid_argument
             (Fmt.str "Received invalid response code: %d" res_code)
end

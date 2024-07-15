open! Types
module W = Eio.Buf_write
module R = Eio.Buf_read

let bin_io_read reader t =
  let open Bin_prot.Utils in
  let open R in
  ensure t size_header_length ;
  let cst = R.peek t in
  let pos_ref = ref cst.Cstruct.off in
  let len = bin_read_size_header cst.Cstruct.buffer ~pos_ref in
  ensure t (len + size_header_length) ;
  let cst = R.peek t in
  let init_pos = cst.Cstruct.off + size_header_length in
  pos_ref := init_pos ;
  let x = reader cst.Cstruct.buffer ~pos_ref in
  let len' = !pos_ref - init_pos in
  assert (len' = len) ;
  consume t (len + size_header_length) ;
  x

let bin_io_write write_buf writer sizer t =
  let open Bin_prot.Utils in
  let open W in
  let hlen = size_header_length in
  let vlen = sizer t in
  let tlen = vlen + hlen in
  let blit x ~src_off:_ (cstbuf : Cstruct.buffer) ~dst_off:pos ~len =
    let pos = bin_write_size_header cstbuf ~pos (len - hlen) in
    writer cstbuf ~pos x |> ignore
  in
  write_gen write_buf ~blit ~off:0 ~len:tlen t

module CommonPrim = struct
  let sm_op_to_uint8 =
    let open Types in
    function Read _ -> 0 | Write _ -> 1
end

module SerPrim = struct
  let bool t w = match t with true -> W.uint8 w 1 | false -> W.uint8 w 0

  let string s w =
    W.BE.uint64 w (s |> String.length |> Int64.of_int) ;
    W.string w s

  let iter ser (is, length) w =
    W.BE.uint64 w (Int64.of_int length) ;
    is |> Iter.iter (fun v -> ser v w)

  let array ser arr = iter ser (Iter.of_array arr, Array.length arr)

  let sm_op op w =
    W.uint8 w (CommonPrim.sm_op_to_uint8 op) ;
    match op with
    | Read k ->
        string k w
    | Write (k, v) ->
        string k w ; string v w

  let command Command.{op; id; submitted; trace_start} w =
    W.BE.uint64 w (Int64.of_int id) ;
    W.BE.double w submitted ;
    W.BE.double w trace_start ;
    array sm_op op w

  let log_entry {term; command= cmd} w =
    W.BE.uint64 w (Int64.of_int term) ;
    command cmd w

  let entries es w = iter log_entry es w
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

  let array parse r =
    let len = R.BE.uint64 r |> Int64.to_int in
    if len < 0 then
      raise
      @@ Fmt.invalid_arg "Len less than 0: %d\n %a" len Fmt.exn_backtrace
           (Invalid_argument "", Printexc.get_callstack 10) ;
    Array.init len (fun _ -> parse r)

  let iter parse =
    let* entries = array parse in
    R.return (Iter.of_array entries, Array.length entries)

  let sm_op : sm_op parser =
    let* op_code = R.map Char.code R.any_char in
    match op_code with
    | 0 (* Read *) ->
        let* k = string in
        R.return @@ Read k
    | 1 (* Write *) ->
        let* k = string and* v = string in
        R.return @@ Write (k, v)
    | _ ->
        raise
          (Invalid_argument
             (Fmt.str "Received %d which is not a valid op_code" op_code) )

  let command =
    let* id = R.map Int64.to_int R.BE.uint64
    and* submitted = R.BE.double
    and* trace_start = R.BE.double
    and* op = array sm_op in
    R.return Command.{op; id; submitted; trace_start}

  let log_entry =
    let* term = R.map Int64.to_int R.BE.uint64 and* command = command in
    R.return {term; command}

  let entries = iter log_entry
end

module External_infra = struct
  type request = command [@@deriving bin_io]

  let serialise_request c w =
    bin_io_write w bin_write_command bin_size_command c

  let parse_request r = bin_io_read bin_read_command r

  type response = command_id * op_result * Core.Float.t [@@deriving bin_io]

  let serialise_response resp w =
    bin_io_write w bin_write_response bin_size_response resp

  let parse_response r = bin_io_read bin_read_response r

  let response_pp : response Fmt.t =
   fun ppf (id, res, _) ->
    Fmt.pf ppf "{id:%d; res:%a}" id Types.op_result_pp res
end

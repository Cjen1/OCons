(* types.ml *)

open Base

type time = float

let time_now : unit -> time = fun () -> Unix.gettimeofday ()

let create_id () =
  Random.self_init () ;
  Random.int64 Int64.max_value

type node_id = int64

type node_addr = string

type client_id = node_id

type command_id = int64 
let len_command_id = 8

module StateMachine : sig
  type t

  type key = string 

  type value = string

  type op = Read of key [@key 1] | Write of key * value [@key 2]

  type command = {op: op; id: command_id} 

  val blit_command : bytes -> command -> offset:int -> unit  

  val get_encoded_length : command -> int

  val decode_command : bytes -> offset:int -> command

  type op_result =
    | Success [@key 1]
    | Failure [@key 2]
    | ReadSuccess of key [@key 3]

  val op_result_failure : unit -> op_result

  val update : t -> command -> op_result

  val create : unit -> t

  val command_equal : command -> command -> bool
end = struct
  type key = string 

  type value = string 

  type t = (key, value) Hashtbl.t

  type op = Read of key [@key 1] | Write of key * value [@key 2]

  type command = {op: op [@key 1]; id: command_id [@key 2]}

  let get_encoded_length = function
    | {op=Read k; id=_id} -> 1 + 8 + String.length k + 8   
    | {op=Write (k,v); id=_id} -> 1 + 8 + String.length k + 8 + String.length v + len_command_id

  let blit_command buf command ~offset =
    let total_size = get_encoded_length command in
    Logs.debug (fun m -> m "offset = %d, p_len = %d" offset  total_size);
    match command with
    | {op=Read key; id} -> 
      let offset =
        Logs.debug (fun m -> m "%d, %d" offset 1);
        EndianBytes.LittleEndian.set_int8 buf offset 0;
        offset + 1
      in
      let offset, key_len =
        let key_len = String.length key in
        Logs.debug (fun m -> m "%d, %d" offset 8);
        EndianBytes.LittleEndian.set_int64 buf offset (key_len|> Int64.of_int);
        offset + 8, key_len
      in 
      let offset = 
        Logs.debug (fun m -> m "%d, %d" offset key_len);
        Bytes.From_string.blito ~src:key ~dst:buf ~dst_pos:offset ();
        offset + key_len 
      in
      let _offset = 
        Logs.debug (fun m -> m "%d, %d" offset 8);
        EndianBytes.LittleEndian.set_int64 buf offset id;
        offset + 8
      in 
      ()
    | {op=Write (key,value); id} ->
      let offset =
        EndianBytes.LittleEndian.set_int8 buf offset 0;
        offset + 1
      in
      let offset, key_len =
        let key_len = String.length key in
        EndianBytes.LittleEndian.set_int64 buf offset (key_len|> Int64.of_int);
        offset + 8, key_len
      in 
      let offset = 
        Bytes.From_string.blito ~src:key ~dst:buf ~dst_pos:offset ();
        offset + key_len 
      in
      let offset, value_len =
        let value_len = String.length value in
        EndianBytes.LittleEndian.set_int64 buf offset (value_len|> Int64.of_int);
        offset + 8, value_len
      in 
      let offset = 
        Bytes.From_string.blito ~src:value ~dst:buf ~dst_pos:offset ();
        offset + value_len 
      in
      let _offset = 
        EndianBytes.LittleEndian.set_int64 buf offset id;
        offset + 8
      in 
      ()

  let decode_command buf ~offset =
    match EndianBytes.LittleEndian.get_int8 buf 0, offset + 1 with
    | 0, offset -> 
      let key_len, offset = 
        EndianBytes.LittleEndian.get_int64 buf offset |> Int64.to_int_exn,
        offset + 8
      in
      let key,offset = 
        Bytes.To_string.sub buf ~pos:offset ~len:key_len,
        offset + key_len
      in
      let id,_offset = 
        EndianBytes.LittleEndian.get_int64 buf offset,
        offset + 8
      in
      {op=Read key; id}
    | 1, offset -> 
      let key_len, offset = 
        EndianBytes.LittleEndian.get_int64 buf offset |> Int64.to_int_exn,
        offset + 8
      in
      let key,offset = 
        Bytes.To_string.sub buf ~pos:offset ~len:key_len,
        offset + key_len
      in
      let value_len, offset = 
        EndianBytes.LittleEndian.get_int64 buf offset |> Int64.to_int_exn,
        offset + 8
      in
      let value,offset = 
        Bytes.To_string.sub buf ~pos:offset ~len:value_len,
        offset + value_len
      in
      let id,_offset = 
        EndianBytes.LittleEndian.get_int64 buf offset,
        offset + 8
      in
      {op=Write (key,value); id}
    | _ -> assert false

  type op_result =
    | Success
    | Failure
    | ReadSuccess of key

  let op_result_failure () = Failure

  let update : t -> command -> op_result =
   fun t -> function
    | {op= Read key; _} -> (
      match Hashtbl.find t key with Some v -> ReadSuccess v | None -> Failure )
    | {op= Write (key, value); _} ->
        Hashtbl.set t ~key ~data:value ;
        Success

  let create () = Hashtbl.create (module String)

  let command_equal a b =
    Int64.(a.id = b.id)
    &&
    match (a.op, b.op) with
    | Read k, Read k' ->
        String.(k = k')
    | Write (k, v), Write (k', v') ->
        String.(k = k' && v = v')
    | Read _, Write _ | Write _, Read _ ->
        false
end

type command = StateMachine.command

type op_result = StateMachine.op_result

type term = int64 

type log_index = int64

(* To do this there are several changes required.
 * 
 * 1. Only the leader can respond to client requests
 * 2. In the failure case prospective leaders need to also get any missing client requests
        Thus request_vote_response messages must contain that info
        Thus request_vote messages must contain the highest known client_request
 *)

type log_entry = {command: command [@key 1]; term: term [@key 2]}

let pp_entry f entry =
  Fmt.pf f "(id:%a term:%a)" Fmt.int64 entry.command.id Fmt.int64 entry.term

let string_of_entry (entry : log_entry) = Fmt.str "%a" pp_entry entry

let string_of_entries entries = Fmt.str "(%a)" (Fmt.list pp_entry) entries

(* Messaging types *)
type request_vote = {term: int64; leader_commit: log_index}

type request_vote_response =
  { term: term
  ; vote_granted: bool
  ; entries: log_entry list
  ; start_index: log_index }

type append_entries =
  { term: term
  ; prev_log_index: log_index
  ; prev_log_term: term
  ; entries: log_entry list
  ; leader_commit: log_index }

(* success is either the highest replicated term (match index) or prev_log_index *)
type append_entries_response =
  {term: term; success: (log_index, log_index) Result.t}

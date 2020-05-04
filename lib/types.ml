(* types.ml *)

open Base

type time = float

let time_now : unit -> time = fun () -> Unix.gettimeofday ()

type unique_id = int [@@deriving protobuf]

let create_id () =
  Random.self_init () ;
  Random.int32 Int32.max_value |> Int32.to_int_exn

type node_id = unique_id [@@deriving protobuf]

type node_addr = string

type client_id = unique_id

type command_id = int [@@deriving protobuf]

module Lookup = struct
  open Base

  type ('a, 'b) t = ('a, 'b) Base.Hashtbl.t

  let pp ppf _v = Stdlib.Format.fprintf ppf "Hashtbl"

  let get t key =
    Hashtbl.find t key
    |> Result.of_option ~error:(Invalid_argument "No such key")

  let get_exn t key = Result.ok_exn (get t key)

  let remove t key = Hashtbl.remove t key ; t

  let set t ~key ~data = Hashtbl.set t ~key ~data ; t

  let removep t p =
    Hashtbl.iteri t ~f:(fun ~key ~data -> if p data then remove t key |> ignore) ;
    t

  let create key_module = Base.Hashtbl.create key_module

  let fold t ~f ~init =
    Base.Hashtbl.fold t ~f:(fun ~key:_ ~data acc -> f data acc) ~init

  let find_or_add = Base.Hashtbl.find_or_add
end

module StateMachine : sig
  type t

  type key = string [@@deriving protobuf]

  type value = string [@@deriving protobuf]

  type op = Read of key [@key 1] | Write of key * value [@key 2]
  [@@deriving protobuf]

  type command = {op: op; id: command_id} [@@deriving protobuf]

  type op_result =
    | Success [@key 1]
    | Failure [@key 2]
    | ReadSuccess of key [@key 3]
  [@@deriving protobuf]

  val op_result_failure : unit -> op_result

  val update : t -> command -> op_result

  val create : unit -> t

  val command_equal : command -> command -> bool
end = struct
  type key = string [@@deriving protobuf]

  type value = string [@@deriving protobuf]

  type t = (key, value) Hashtbl.t

  type op = Read of key [@key 1] | Write of key * value [@key 2]
  [@@deriving protobuf]

  type command = {op: op [@key 1]; id: command_id [@key 2]}
  [@@deriving protobuf]

  type op_result =
    | Success [@key 1]
    | Failure [@key 2]
    | ReadSuccess of key [@key 3]
  [@@deriving protobuf]

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
    Int.(a.id = b.id)
    &&
    match (a.op, b.op) with
    | Read k, Read k' ->
        String.(k = k')
    | Write (k, v), Write (k', v') ->
        String.(k = k' && v = v')
    | Read _, Write _ | Write _, Read _ ->
        false
end

module type Persistable = sig
  type t

  val init : unit -> t

  type op

  val op_to_protobuf : op -> Protobuf.Encoder.t -> unit

  val op_from_protobuf : Protobuf.Decoder.t -> op

  val apply : t -> op -> t
end

module Persistant (P : Persistable) : sig
  type t =
    { t: P.t
    ; mutable unsyncd: P.op list
    ; fd: Lwt_unix.file_descr
    ; channel: Lwt_io.output_channel }

  type op = P.op

  val sync : t -> t Lwt.t

  val of_file : string -> t Lwt.t

  val change : t -> op -> t
end = struct
  include P

  type t =
    { t: P.t
    ; mutable unsyncd: P.op list
    ; fd: Lwt_unix.file_descr
    ; channel: Lwt_io.output_channel }

  let sync t =
    match t.unsyncd with
    | [] ->
        Lwt.return t
    | _ ->
        Logs.debug (fun m ->
            m "There are %d ops to sync" (List.length t.unsyncd)) ;
        let vs = t.unsyncd |> List.rev in
        t.unsyncd <- [] ;
        let%lwt () =
          Lwt_list.iter_s
            (fun v ->
              Logs.debug (fun m -> m "Syncing op") ;
              let payload = Protobuf.Encoder.encode_exn P.op_to_protobuf v in
              let p_len = Bytes.length payload in
              let buf = Bytes.create (p_len + 4) in
              Bytes.blit ~src:payload ~src_pos:0 ~dst:buf ~dst_pos:4 ~len:p_len ;
              EndianBytes.LittleEndian.set_int32 buf 0 (Int32.of_int_exn p_len) ;
              let%lwt () =
                Lwt_io.write_from_exactly t.channel buf 0 (Bytes.length buf)
              in
              Lwt.return_unit)
            vs
        in
        Logs.debug (fun m -> m "Ops written to channel") ;
        let%lwt () = Lwt_io.flush t.channel in
        Logs.debug (fun m -> m "Channel flushed") ;
        let%lwt () = Lwt_unix.fsync t.fd in
        Logs.debug (fun m -> m "Ops fsync'd") ;
        Lwt.return {t with unsyncd= []}

  let read_value channel =
    let rd_buf = Bytes.create 4 in
    let%lwt () = Lwt_io.read_into_exactly channel rd_buf 0 4 in
    let size =
      EndianBytes.LittleEndian.get_int32 rd_buf 0 |> Int32.to_int_exn
    in
    let payload_buf = Bytes.create size in
    let%lwt () = Lwt_io.read_into_exactly channel payload_buf 0 size in
    payload_buf |> Lwt.return

  let of_file file =
    Logs.debug (fun m -> m "Trying to open file") ;
    let%lwt fd = Lwt_unix.openfile file Lwt_unix.[O_RDONLY; O_CREAT] 0o640 in
    let input_channel = Lwt_io.of_fd ~mode:Lwt_io.input fd in
    let stream =
      Lwt_stream.from (fun () ->
          try%lwt
            let%lwt v = read_value input_channel in
            Protobuf.Decoder.decode_exn P.op_from_protobuf v |> Lwt.return_some
          with End_of_file -> Lwt.return_none)
    in
    Logs.debug (fun m -> m "Reading in") ;
    let%lwt t = Lwt_stream.fold (fun v t -> P.apply t v) stream (P.init ()) in
    let%lwt () = Lwt_io.close input_channel in
    Logs.debug (fun m -> m "Creating fd for persistance") ;
    let%lwt fd = Lwt_unix.openfile file Lwt_unix.[O_WRONLY; O_APPEND] 0o640 in
    let channel = Lwt_io.of_fd ~mode:Lwt_io.output fd in
    Lwt.return {t; unsyncd= []; fd; channel}

  let change t op = {t with t= P.apply t.t op; unsyncd= op :: t.unsyncd}
end

type command = StateMachine.command

type op_result = StateMachine.op_result

type term = int [@@deriving protobuf]

type log_index = int [@@deriving protobuf]

let log_index_mod : int Base__.Hashtbl_intf.Key.t = (module Int)

type log_entry =
  { command: StateMachine.command [@key 1]
  ; term: term [@key 2]
  ; index: log_index [@key 3] }
[@@deriving protobuf]

let string_of_entry entry =
  let open StateMachine in
  let cmd =
    match entry.command with
    | {op= Read k; _} ->
        Printf.sprintf "(Read %s)" k
    | {op= Write (k, v); _} ->
        Printf.sprintf "(Write %s %s)" k v
  in
  let term, index = (Int.to_string entry.term, Int.to_string entry.index) in
  Printf.sprintf "(%s %s %s)" index term cmd

let string_of_entries entries =
  let res = entries |> List.map ~f:string_of_entry |> String.concat ~sep:" " in
  Printf.sprintf "(%s)" res

type partial_log = log_entry list

type persistent = Log_entry of log_entry

(* Messaging types *)
type request_vote_response =
  {term: term; voteGranted: bool; entries: log_entry list}

type append_entries_response = {term: term; success: bool}

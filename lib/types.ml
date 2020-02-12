(* types.ml *)

open Base

type time = float

let time_now : unit -> time = fun () -> Unix.time ()

type unique_id = int [@@deriving protobuf]

let create_id () = Random.int32 Int32.max_value |> Int32.to_int_exn

type node_id = unique_id [@@deriving protobuf]

type node_addr = string

type client_id = unique_id

type command_id = int [@@deriving protobuf]

module Lookup = struct
  open Base

  type ('a, 'b) t = ('a, 'b) Base.Hashtbl.t

  let get t key =
    Hashtbl.find t key
    |> Result.of_option ~error:(Invalid_argument "No such key")

  let get_exn t key = Result.ok_exn (get t key)

  let remove t key = Hashtbl.remove t key

  let set t ~key ~data = Hashtbl.set t ~key ~data ; t

  let removep t p =
    Hashtbl.iteri t ~f:(fun ~key ~data -> if p data then remove t key)

  let create key_module = Base.Hashtbl.create key_module

  let fold t ~f ~init =
    Base.Hashtbl.fold t ~f:(fun ~key:_ ~data acc -> f data acc) ~init
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
end

type term = int [@@deriving protobuf]

let get_term_from_file file =
  let open Unix in
  let term_fd = openfile file [O_RDWR] 0o640 in
  let input_channel = Lwt_io.of_unix_fd ~mode:Lwt_io.input term_fd in
  let%lwt (term : term) =
    let term = 0 in
    let rec loop (term : int) =
      let%lwt value =
        try%lwt
          let%lwt v = Lwt_io.read_value input_channel in
          Lwt.return_some v
        with End_of_file -> Lwt.return_none
      in
      match value with Some term -> loop term | None -> Lwt.return term
    in
    loop term
  in
  let%lwt () = Lwt_io.close input_channel in
  close term_fd ; Lwt.return term

type log_index = int [@@deriving protobuf]

let log_index_mod : int Base__.Hashtbl_intf.Key.t = (module Int)

type log_entry =
  { command: StateMachine.command [@key 1]
  ; term: term [@key 2]
  ; index: log_index [@key 3] }
[@@deriving protobuf]

module Log = struct
  type t = (log_index, log_entry) Lookup.t

  let get = Lookup.get

  let get_exn = Lookup.get_exn

  type op = Set of log_index * log_entry | Remove of log_index

  let set t ~index ~value : t * op list =
    (Lookup.set t ~key:index ~data:value, [Set (index, value)])

  let removep t p =
    let acc = ref [] in
    Lookup.removep t (fun x ->
        if p x then (
          acc := Remove x.index :: !acc ;
          true )
        else false) ;
    !acc

  let get_max_index (t : t) =
    Lookup.fold t ~init:1 ~f:(fun v acc -> Int.max v.index acc)

  let entries_after log leaderCommit =
    let rec loop i acc =
      match Lookup.get log i with
      | Ok v ->
          loop (i + 1) (v :: acc)
      | Error _ ->
          acc
    in
    List.rev @@ loop (leaderCommit + 1) []

  let add_entries_remove_conflicts t entries =
    (* ops accumulator is newest -> oldest ordering *)
    let rec loop t ops = function
      | [] ->
          (t, ops)
      | entry :: entries ->
        (* ordering of removals doesn't matter *)
          let removed =
            match get t entry.index with
            | Ok curr_entry when not Int.(curr_entry.term = entry.term) ->
                (* Remove all entries which conflict with this entry *)
                removep t (fun x -> x.index >= curr_entry.index)
            | _ ->
                []
          in
          let t, ops_added = 
            match get t entry.index with 
            | Error _ -> 
              set t ~index:entry.index ~value:entry
            | Ok _ -> t, []
          in
          loop t (ops_added @ removed @ ops) entries
    in
    loop t [] (List.rev entries)

  let append t v =
    let max_index = get_max_index t in
    set t ~index:(max_index + 1) ~value:v

  let apply t op =
    match op with
    | Set (index, value) ->
        set t ~index ~value |> fst
    | Remove index ->
        Lookup.remove t index ; t

  let get_log_from_file file =
    let open Unix in
    let log_fd = openfile file [O_RDWR] 0o640 in
    let input_channel = Lwt_io.of_unix_fd ~mode:Lwt_io.input log_fd in
    let%lwt (log : t) =
      let init_log = Lookup.create log_index_mod in
      let rec loop curr_log =
        let%lwt value =
          try%lwt
            let%lwt v = Lwt_io.read_value input_channel in
            Lwt.return_some v
          with End_of_file -> Lwt.return_none
        in
        match value with
        | Some v ->
            let next_log = apply curr_log v in
            loop next_log
        | None ->
            Lwt.return curr_log
      in
      loop init_log
    in
    let%lwt () = Lwt_io.close input_channel in
    close log_fd ; Lwt.return log
end

type log = Log.t

type partial_log = log_entry list

type persistent = Log_entry of log_entry

module Config = struct
  type file = {channel: Lwt_io.output_channel; fd: Unix.file_descr}

  type t =
    { majority: int
    ; followers: (node_id, node_addr) List.Assoc.t
    ; election_timeout: float
    ; idle_timeout: float
    ; log_file: file
    ; term_file: file
    ; num_nodes: int
    ; node_id: node_id }

  let get_addr_from_id_exn config id =
    List.Assoc.find_exn config.followers id ~equal:Int.equal

  let get_id_from_addr_exn config id =
    List.Assoc.find_exn (List.Assoc.inverse config.followers) id ~equal:String.equal

  let write_to_term t (v : term) =
    let%lwt () = Lwt_io.write_value t.term_file.channel v in
    let%lwt () = Lwt_io.flush t.term_file.channel in
    Lwt.return @@ Unix.fsync t.term_file.fd

  let write_to_log t (v : Log.op) =
    let%lwt () = Lwt_io.write_value t.log_file.channel v in
    let%lwt () = Lwt_io.flush t.log_file.channel in
    Lwt.return @@ Unix.fsync t.term_file.fd

  (* vs is oldest to newest *) 
  let write_all_to_log t (vs : Log.op list) =
    let%lwt () = Lwt_list.iter_s (Lwt_io.write_value t.log_file.channel) vs in
    let%lwt () = Lwt_io.flush t.log_file.channel in
    Lwt.return @@ Unix.fsync t.term_file.fd
end

type config = Config.t

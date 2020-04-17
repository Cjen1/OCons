(* types.ml *)

open Base

type time = float

let time_now : unit -> time = fun () -> Unix.time ()

type unique_id = int [@@deriving protobuf]

let create_id () = 
  Random.self_init ();
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

  let remove t key = Hashtbl.remove t key

  let set t ~key ~data = Hashtbl.set t ~key ~data ; t

  let removep t p =
    Hashtbl.iteri t ~f:(fun ~key ~data -> if p data then remove t key)

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

type command = StateMachine.command

type op_result = StateMachine.op_result

type term = int [@@deriving protobuf]

let get_term_from_file file =
  let open Unix in
  let term_fd = openfile file [O_RDWR; O_CREAT] 0o640 in
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
  Lwt.return term

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

(*
module Log' = struct
  type t = log_entry list

  let get t idx = List.nth t (idx + 1)

  let get_exn t idx = List.nth_exn t (idx + 1)

  let get_term t index = 
    match index with
    | 0 -> 0
    | _ -> 
      let entry = get_exn t index in
      entry.term

  type op = Set of log_index * log_entry | Remove of log_index

  let set t ~index ~value : t * op list = 
    if index < 0 then raise @@ Invalid_argument "Index cannot be negative";
    if index = 0 then raise @@ Invalid_argument "Cannot set initial element";
    let rec iter ls = function
      | 1 -> 

        *)

module Log = struct
  (* Common operations:
      - Get max_index
      - Indices after
      - Get specific index (close to end generally)
      - Append to end of log
  *)
  type t = (log_index, log_entry) Lookup.t

  type t_list = log_entry list (* Lowest index first *)

  let get = Lookup.get

  let get_exn = Lookup.get_exn

  let get_term t index =
    match index with
    | 0 ->
        0
    | _ ->
        let entry = get_exn t index in
        entry.term

  type op = Set of log_index * log_entry | Remove of log_index

  let set t ~index ~value : t * op list =
    Logs.debug (fun m -> m "Setting %d to %s" index (string_of_entry value)) ;
    (Lookup.set t ~key:index ~data:value, [Set (index, value)])

  let remove t i = Lookup.remove t i ; Remove i

  let removep t p =
    let acc = ref [] in
    Lookup.removep t (fun x ->
        if p x then (
          acc := Remove x.index :: !acc ;
          true )
        else false) ;
    !acc

  let get_max_index (t : t) =
    Lookup.fold t ~init:0 ~f:(fun v acc -> Int.max v.index acc)

  (*
  (* Entries after i inclusive *)
  let entries_after_inc log ~index : t_list =
    let index = if index = 0 then 1 else 0 in (* Avoids printing errors *)
    let rec loop i acc =
      match get log index with
      | Ok v ->
          loop (i + 1) (v :: acc)
      | Error _ ->
          acc
    in
    List.rev @@ loop (index) []
      *)
  let entries_after_inc log ~index : t_list =
    let res =
      Hashtbl.fold log ~init:[] ~f:(fun ~key ~data acc ->
          if key >= index then data :: acc else acc)
    in
    List.sort res ~compare:(fun a b -> Int.compare a.index b.index)

  let to_string t = 
    let entries = entries_after_inc t ~index:1 in
    string_of_entries entries

  let add_entries_remove_conflicts t (entries : t_list) =
    let rec delete_geq t i =
      match get t i with
      | Ok v ->
          assert (Int.(v.index = i)) ;
          remove t i :: delete_geq t (i + 1)
      | Error _ ->
          []
    in
    List.fold_left entries ~init:(t, []) ~f:(fun (t, ops_acc) e ->
        match get t e.index with
        | Ok {term; command; _}
          when Int.(term = e.term)
               && StateMachine.command_equal command e.command ->
            (* No log inconsistency *)
            (t, ops_acc)
        | Error _ ->
            (* No entry at index *)
            let t, ops = set t ~index:e.index ~value:e in
            (t, ops @ ops_acc)
        | Ok _ ->
            (* inconsitent log *)
            let t, ops = set t ~index:e.index ~value:e in
            let ops = ops @ delete_geq t e.index in
            (t, ops @ ops_acc))

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
    let log_fd = openfile file [O_RDWR; O_CREAT] 0o640 in
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
    Lwt.return log
end

type log = Log.t

type partial_log = log_entry list

type persistent = Log_entry of log_entry

(* Messaging types *)
type request_vote_response =
  {term: term; voteGranted: bool; entries: log_entry list}

type append_entries_response = {term: term; success: bool}

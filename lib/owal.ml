open! Core
open! Async

let logger =
  let open Async_unix.Log in
  create ~level:`Info ~output:[] ~on_error:`Raise
    ~transform:(fun m -> Message.add_tags m [("src", "Infra")])
    ()

module type Persistable = sig
  type t

  val init : unit -> t

  type op [@@deriving bin_io, sexp]

  val apply : t -> op -> t
end

module File = struct
  type t =
    { path: string
    ; writer: Async.Writer.t
    ; mutable cursor: int64
    ; mutable length: int64
    ; mutable state: [`Open | `Closed]
    ; mutable sync_cursor: int64
    ; mutable sync_promise: unit Deferred.t }

  let create path ~len ~append =
    let%bind writer = Writer.open_file ~append path in
    let%bind () = Unix.ftruncate (Writer.fd writer) ~len in
    let%bind () = Writer.fsync writer in
    return
      { path
      ; writer
      ; cursor= Int64.zero
      ; sync_cursor= Int64.zero
      ; sync_promise= return ()
      ; length= len
      ; state= `Open }

  let close t =
    match t.state with
    | `Closed ->
        (*print_endline @@ Fmt.str "%s: file.state closed" t.path ;*)
        Writer.close_finished t.writer
    | `Open -> (
        (*print_endline @@ Fmt.str "%s: file.state open" t.path ;*)
        t.state <- `Closed ;
        match Int64.(t.cursor = zero) with
        | true ->
            (*print_endline @@ Fmt.str "%s: empty file" t.path ;*)
            let%bind () = Writer.close t.writer in
            Unix.unlink t.path
        | false ->
            (*print_endline @@ Fmt.str "%s: truncating" t.path ;*)
            let%bind () = Unix.ftruncate (Writer.fd t.writer) ~len:t.cursor in
            Writer.close t.writer )

  let datasync t =
    match t.state with
    | `Closed ->
        Writer.close_finished t.writer
    | `Open when Int64.(t.sync_cursor >= t.cursor) ->
        t.sync_promise
    | `Open ->
        t.sync_cursor <- t.cursor ;
        let p = Writer.fdatasync t.writer in
        t.sync_promise <- p ;
        p

  let write_raw t (bin_prot : 'a Core.Bin_prot.Type_class.writer) v =
    let len =
      bin_prot.size v + Bin_prot.Utils.size_header_length |> Int64.of_int
    in
    t.cursor <- Int64.(t.cursor + len) ;
    Writer.write_bin_prot t.writer bin_prot v

  let write_bin_prot t (bin_prot : 'a Core.Bin_prot.Type_class.writer) v =
    (*print_endline @@ Fmt.str "Writing to %s" t.path;*)
    let len =
      bin_prot.size v + Bin_prot.Utils.size_header_length |> Int64.of_int
    in
    match t.state with
    | `Closed ->
        Error `Closed
    | `Open when Int64.(t.cursor + len > t.length) ->
        Error `OOM
    | `Open ->
        write_raw t bin_prot v ; Ok ()

  let write_bin_prot_oversized t b v = write_raw t b v
end

module U : sig
  type accumulator

  val make_accumulator : unit -> accumulator

  val accumulate : accumulator -> unit Deferred.t -> unit Deferred.t

  val get_acc : accumulator -> unit Deferred.t
end = struct
  type accumulator = unit Deferred.t ref

  let make_accumulator () = ref @@ return ()

  let accumulate t v =
    let p = Deferred.all_unit [!t; v] in
    t := p ;
    p

  let get_acc t = !t
end

module Persistant (P : Persistable) = struct
  type t =
    { default_file_size: int64
    ; mutable state: [`Open | `Closed]
    ; mutable current_file: File.t
    ; mutable next_file: File.t Deferred.t
    ; name_gen: unit -> string
    ; serialisation: unit Sequencer.t
    ; file_closure_chain: U.accumulator }

  let move_to_next_file t =
    U.accumulate t.file_closure_chain (File.close t.current_file)
    |> don't_wait_for ;
    let%bind next_file = t.next_file in
    t.current_file <- next_file ;
    let next_name = t.name_gen () in
    t.next_file <- File.create next_name ~append:false ~len:t.default_file_size ;
    return ()

  exception Closed

  let write_bin_prot t (bin_prot : 'a Core.Bin_prot.Type_class.writer) v =
    match t.state with
    | `Open ->
        Throttle.enqueue t.serialisation (fun () ->
            match File.write_bin_prot t.current_file bin_prot v with
            | Error `OOM ->
                File.write_bin_prot_oversized t.current_file bin_prot v ;
                move_to_next_file t
            | Error `Closed ->
                assert false
            | Ok () ->
                return () )
        |> don't_wait_for
    | `Closed ->
        raise Closed

  let datasync t =
    (* Need to ensure that datasync is called before any further writes, so *)
    let ivar = Ivar.create () in
    Throttle.enqueue t.serialisation (fun () ->
        (* Ensure previous files are closed before returning *)
        let p =
          let%bind () =
            Deferred.all_unit
              [U.get_acc t.file_closure_chain; File.datasync t.current_file]
          in
          Ivar.fill ivar () |> return
        in
        p |> don't_wait_for |> return )
    |> don't_wait_for ;
    Ivar.read ivar

  let close t =
    match t.state with
    | `Closed ->
        Throttle.cleaned t.serialisation
    | `Open ->
        t.state <- `Closed ;
        Throttle.enqueue t.serialisation (fun () ->
            let curr = File.close t.current_file in
            let next =
              let%bind file = t.next_file in
              File.close file
            in
            U.accumulate t.file_closure_chain curr |> don't_wait_for ;
            let%bind () = U.accumulate t.file_closure_chain next in
            Throttle.kill t.serialisation ;
            return () )

  let of_path ?(file_size = Int64.((of_int 2 ** of_int 20) * of_int 128)) path =
    let%bind _create_dir_if_needed =
      match%bind Sys.file_exists path with
      | `Yes ->
          return ()
      | _ ->
          Unix.mkdir path
    in
    let name_gen =
      let counter = ref 0 in
      fun () ->
        incr counter ;
        Fmt.str "%s/%d.wal" path !counter
    in
    let rec read_value_loop t reader =
      match%bind Reader.read_bin_prot reader P.bin_reader_op with
      | `Eof ->
          return t
      | `Ok op ->
          read_value_loop (P.apply t op) reader
    in
    let rec read_file_loop v =
      let path = name_gen () in
      let%bind res =
        try_with (fun () -> Reader.with_file path ~f:(read_value_loop v))
      in
      match res with Ok v -> read_file_loop v | Error _ -> return (v, path)
    in
    let%bind v, path = read_file_loop (P.init ()) in
    let%bind current_file = File.create path ~len:file_size ~append:false in
    let next_file = File.create (name_gen ()) ~len:file_size ~append:false in
    return
      ( { name_gen
        ; state= `Open
        ; default_file_size= file_size
        ; current_file
        ; next_file
        ; serialisation= Sequencer.create ()
        ; file_closure_chain= U.make_accumulator () }
      , v )

  let write t op = write_bin_prot t P.bin_writer_op op
end

open! Core

let logger =
  let open Async_unix.Log in
  create ~level:`Info ~output:[] ~on_error:`Raise
    ~transform:(fun m -> Message.add_tags m [("src", "Infra")])
    ()

module type Persistable = sig
  type t

  val init : unit -> t

  type op [@@deriving bin_io]

  val apply : t -> op -> t
end

module BufferedFile = struct
  type t =
    { path: string
    ; fd: Unix.File_descr.t
    ; mutable file_cursor: int
    ; len: int
    ; scheduled: Bigstring.t Unix.IOVec.t Deque.t }

  let create path ~len ~mode =
    let fd = Unix.(openfile ~mode path) in
    Unix.ftruncate fd ~len:(Int64.of_int len) ;
    Unix.fsync fd ;
    {path; fd; file_cursor= 0; len; scheduled= Deque.create ()}

  let dummy_iovec = Unix.IOVec.(empty bigstring_kind)

  let mk_iovecs t =
    let n_iovecs =
      Int.min (Deque.length t.scheduled) (Lazy.force Unix.IOVec.max_iovecs)
    in
    let iovecs = Array.create ~len:n_iovecs dummy_iovec in
    let iovecs_len = ref 0 in
    with_return (fun r ->
        let i = ref 0 in
        Deque.iter t.scheduled ~f:(fun iovec ->
            if !i >= n_iovecs then r.return () ;
            iovecs_len := !iovecs_len + iovec.Unix.IOVec.len ;
            iovecs.(!i) <- iovec ;
            incr i)) ;
    (iovecs, !iovecs_len)

  let rec dequeue_iovecs t bytes_written =
    let rec remove_done bytes_written =
      assert (bytes_written >= 0) ;
      match Deque.dequeue_front t.scheduled with
      | None ->
          if bytes_written > 0 then
            Fmt.failwith "Writer wrote nonzero amount but IO_queue is empty"
      | Some Unix.IOVec.{buf; pos; len} ->
          if bytes_written >= len then (
            Bigstring.unsafe_destroy buf ;
            remove_done (bytes_written - len) )
          else
            (* Partial IO: update and retry*)
            let new_iovec =
              Unix.IOVec.of_bigstring buf ~pos:(pos + bytes_written)
                ~len:(len - bytes_written)
            in
            Deque.enqueue_front t.scheduled new_iovec
    in
    remove_done bytes_written ;
    if not @@ Deque.is_empty t.scheduled then flush t

  and flush t =
    let iovecs, _len = mk_iovecs t in
    let written = Bigstring_unix.writev t.fd iovecs in
    dequeue_iovecs t written

  let datasync t = flush t ; Unix.fdatasync t.fd

  exception Oversized

  let write_bin_prot t (bin_prot : 'a Bin_prot.Type_class.writer) v =
    let len = bin_prot.size v + Bin_prot.Utils.size_header_length in
    if t.file_cursor + len > t.len then raise Oversized ;
    let buf = Bigstring.create len in
    ignore (Bigstring.write_bin_prot buf ~pos:0 bin_prot v : int) ;
    Deque.enqueue_back t.scheduled @@ Unix.IOVec.of_bigstring buf ;
    t.file_cursor <- t.file_cursor + len

  let close t =
    match () with
    | () when t.file_cursor = 0 ->
        Unix.close t.fd ; Unix.unlink t.path
    | () ->
        flush t ;
        Unix.ftruncate t.fd ~len:(Int64.of_int t.file_cursor) ;
        Unix.fsync t.fd ;
        Unix.close t.fd

  let _close_in_async t =
    datasync t ;
    let open Async in
    Thread_safe.run_in_async_exn (fun () ->
        let open Unix in
        let fd = Fd.create Fd.Kind.File t.fd @@ Info.of_string t.path in
        close fd)
end

module Persistant (P : Persistable) = struct
  type t =
    { default_file_size: int
    ; mutable current_file: BufferedFile.t
    ; name_gen: unit -> string }

  let move_to_next_file t =
    BufferedFile.close t.current_file ;
    let next_name = t.name_gen () in
    t.current_file <-
      BufferedFile.create next_name
        ~mode:Unix.[O_WRONLY; O_CREAT]
        ~len:t.default_file_size

  let datasync t = BufferedFile.datasync t.current_file

  let flush t = BufferedFile.flush t.current_file

  let close t = BufferedFile.close t.current_file

  let of_path_async ?(file_size = Int.pow 2 20 * 128) path =
    let open Async in
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
    let current_file =
      BufferedFile.create path ~len:file_size ~mode:[O_WRONLY; O_CREAT]
    in
    return ({name_gen; default_file_size= file_size; current_file}, v)

  let rec write_bin_prot t (bin_prot : 'a Core.Bin_prot.Type_class.writer) v =
    match BufferedFile.write_bin_prot t.current_file bin_prot v with
    | () ->
        ()
    | exception BufferedFile.Oversized ->
        move_to_next_file t ;
        write_bin_prot t bin_prot v

  let write t op = write_bin_prot t P.bin_writer_op op
end

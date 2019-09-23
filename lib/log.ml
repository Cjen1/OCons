open Core

module type LOGGER = sig
  type level = ERROR | WARN | INFO | DEBUG | TRACE

  val initialize_logs :
       ?err_chan:Lwt_io.output_channel
    -> ?warn_chan:Lwt_io.output_channel
    -> ?info_chan:Lwt_io.output_channel
    -> ?debug_chan:Lwt_io.output_channel
    -> ?trace_chan:Lwt_io.output_channel
    -> ?wal_chan:Lwt_io.output_channel
    -> unit
    -> unit

  val initialize_default : string -> unit Lwt.t

  val write_to_log : level -> string -> unit Lwt.t

  val write_with_timestamp : level -> string -> unit Lwt.t

  val write_to_wal : string -> unit Lwt.t
end

module Logger : LOGGER = struct
  type level = ERROR | WARN | INFO | DEBUG | TRACE

  type chans =
    { err_chan: Lwt_io.output_channel option
    ; warn_chan: Lwt_io.output_channel option
    ; info_chan: Lwt_io.output_channel option
    ; debug_chan: Lwt_io.output_channel option
    ; trace_chan: Lwt_io.output_channel option
    ; wal_chan: Lwt_io.output_channel option }

  let log_channels =
    ref
      { err_chan= None
      ; warn_chan= None
      ; info_chan= None
      ; debug_chan= None
      ; trace_chan= None
      ; wal_chan= None }

  let wal_fd = ref None

  let initialize_logs ?err_chan ?warn_chan ?info_chan ?debug_chan ?trace_chan
      ?wal_chan () =
    log_channels :=
      {err_chan; warn_chan; info_chan; debug_chan; trace_chan; wal_chan}

  let initialize_default (directory : string) =
    let%lwt info_chan =
      Lwt_io.open_file Lwt_io.Output (directory ^ "-info.log")
    in
    let%lwt debug_chan =
      Lwt_io.open_file Lwt_io.Output (directory ^ "-debug.log")
    in
    let%lwt trace_chan =
      Lwt_io.open_file Lwt_io.Output (directory ^ "-trace.log")
    in
    let fd =
      Unix.openfile [Unix.O_WRONLY; Unix.O_CREAT] (directory ^ "-wal.log")
    in
    let () = wal_fd := Some fd in
    let wal_chan =
      Lwt_io.of_fd Lwt_io.Output (fd |> Lwt_unix.of_unix_file_descr)
    in
    Lwt.return
      (initialize_logs ~err_chan:Lwt_io.stderr ~warn_chan:Lwt_io.stdout
         ~info_chan ~debug_chan ~trace_chan ~wal_chan ())

  let write_to_log level line =
    let chan_opt =
      match level with
      | ERROR ->
          !log_channels.err_chan
      | WARN ->
          !log_channels.warn_chan
      | INFO ->
          !log_channels.info_chan
      | DEBUG ->
          !log_channels.debug_chan
      | TRACE ->
          !log_channels.trace_chan
    in
    match chan_opt with
    | None ->
        Lwt.return_unit
    | Some chan ->
        Lwt_io.write_line chan line

  let write_to_wal line =
    match (!log_channels.wal_chan, !wal_fd) with
    | Some chan, Some fd ->
        let%lwt res = Lwt_io.write_line chan line in
        let () = Unix.fsync fd in
        Lwt.return res
    | _ ->
        assert false

  let write_with_timestamp level line =
    let time = Time.to_string (Time.now ()) in
    write_to_log level (time ^ ":\t" ^ line)
end

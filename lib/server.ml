open Utils
open Base

let server = Logs.Src.create "Server" ~doc:"Server module"

module Log = (val Logs_lwt.src_log server : Logs_lwt.LOG)

let handle_connection (sock, sockaddr) t connected_callback () =
     let* () =
        Log.debug (fun m ->
            m "handle_conn: Connection initialised from %s"
              (string_of_sockaddr sockaddr))
      in
  let open Lwt_io in
  let ic = of_fd ~mode:Input sock in
  let oc = of_fd ~mode:Output sock in
  ( try%lwt
      let read () = Lwt_io.read_value ic in
      let write msg = Lwt_io.write_value oc msg in
      connected_callback (read, (write : 'b -> unit Lwt.t)) t
    with
  | Unix.Unix_error (e, f, p) ->
      Log.debug (fun m ->
          m "handle_conn: failed with %s calling %s with parameter %s"
            (Unix.error_message e) f p)
  | End_of_file ->
      Log.debug (fun m -> m "handle_conn: connection closed while writing") )
    [%lwt.finally
      let open Lwt_unix in
      let* () = flush oc in
      let () = shutdown sock SHUTDOWN_ALL in
      let* () = close sock in
      Log.debug (fun m -> m "Connection finished")]

let create_server sock t connected_callback =
  let rec serve () =
    let* conn = Lwt_unix.accept sock in
    Lwt.async (handle_connection conn t connected_callback) ;
    serve ()
  in
  serve ()

let create_bind_socket listen_address port =
  let open Lwt_unix in
  let sock = socket PF_INET SOCK_STREAM 0 in
  try
    setsockopt sock SO_REUSEADDR true ;
    let* () = bind sock @@ ADDR_INET (listen_address, port) in
    let max_pending = 20 in
    listen sock max_pending ; Lwt.return sock
  with z ->
    let* () = close sock in
    raise z

let start :
       Unix.inet_addr
    -> int
    -> 'a
    -> ((unit -> string Lwt.t) * (string -> unit Lwt.t) -> 'a -> unit Lwt.t)
    -> unit Lwt.t =
 fun listen_address port t connected_callback ->
  let* sock = create_bind_socket listen_address port in
  create_server sock t connected_callback

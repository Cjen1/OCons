open Utils
open Base

module type Server = sig
  type t

  val connected_callback :
    Lwt_io.input_channel * Lwt_io.output_channel -> t -> unit Lwt.t
end

let server = Logs.Src.create "Server" ~doc:"Server module"
module Log = (val Logs_lwt.src_log server : Logs_lwt.LOG)


module Make_Server (S : Server) : sig
  val start : Unix.inet_addr -> int -> S.t -> unit Lwt.t
end = struct

  let handle_connection (sock, sockaddr) (t : S.t) () =
    let open Lwt_io in
    let open Lwt_unix in
    (
      try%lwt 
        let ic = of_fd ~mode:Input sock in
        let oc = of_fd ~mode:Output sock in
        Logs.debug (fun m ->
            m "handle_conn: Connection initialised from %s" (string_of_sockaddr sockaddr));
        let* () = S.connected_callback (ic, oc) t in
        shutdown sock SHUTDOWN_ALL;
        Log.debug (fun m -> m "Connection finished")
      with
      | Unix.Unix_error (e, f, p) ->
        Log.debug (fun m ->
            m "handle_conn: failed with %s calling %s with parameter %s"
              (Unix.error_message e) f p)
      | End_of_file ->
        Log.debug (fun m ->
            m "handle_conn: connection closed while writing")
    )[%lwt.finally (Lwt_unix.close sock)]

  let create_server sock (t : S.t) =
    let rec serve () =
      let* conn = Lwt_unix.accept sock in
      Lwt.async(handle_connection conn t);
      serve ()
    in
    serve

  let create_bind_socket listen_address port =
    let open Lwt_unix in
    let sock = socket PF_INET SOCK_STREAM 0 in
    try
      setsockopt sock SO_REUSEADDR true ;
      let* () = bind sock @@ ADDR_INET (listen_address, port) in
      let max_pending = 20 in
      Lwt_unix.listen sock max_pending ;
      Lwt.return sock
    with z -> (let* () = close sock in raise z)

  let start listen_address port (t : S.t) =
    let* sock = create_bind_socket listen_address port in
    create_server sock t ()
end

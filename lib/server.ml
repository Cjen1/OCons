open Utils

module type Server = sig
  type t

  val connected_callback :
    Lwt_io.input_channel * Lwt_io.output_channel -> t -> unit Lwt.t
end

module Make_Server (S : Server) : sig
  val start : Unix.inet_addr -> int -> S.t -> unit Lwt.t
end = struct
  let string_of_sockaddr s =
    match s with
    | Lwt_unix.ADDR_UNIX s ->
        s
    | Lwt_unix.ADDR_INET (inet, p) ->
        Unix.string_of_inet_addr inet ^ ":" ^ Int.to_string p

  let accept_connection (fd, sockaddr) (t : S.t) =
    let ic = Lwt_io.of_fd ~mode:Lwt_io.Input fd in
    let oc = Lwt_io.of_fd ~mode:Lwt_io.Output fd in
    let callback =
      (* Need to ensure to close both filei descriptors after call*)
      let* () = Logs_lwt.info (fun m -> m "Connection initialised") in
      let* () =
        Logs_lwt.debug (fun m ->
            m "Connection initialised from %s" (string_of_sockaddr sockaddr))
      in
      let* () = S.connected_callback (ic, oc) t in
      (* let* () = Lwt_io.close ic and* () = Lwt_io.close oc in *)
      let* () = Logs_lwt.info (fun m -> m "Connection finished") in
      Lwt.return_unit
    in
    let () =
      Lwt.on_failure callback (fun e ->
          Logs.err (fun m -> m "%s" (Printexc.to_string e)))
    in
    let* () = Lwt.pause () in
    Logs_lwt.info (fun m -> m "New connection")

  let create_server sock (t : S.t) =
    let rec serve () =
      let* conn = Lwt_unix.accept sock in
      Lwt.join [accept_connection conn t; serve ()]
    in
    serve

  let create_socket listen_address port =
    let open Lwt_unix in
    let sock = socket PF_INET SOCK_STREAM 0 in
    let* () = bind sock @@ ADDR_INET (listen_address, port) in
    let max_pending = 20 in
    Lwt_unix.listen sock max_pending ;
    Lwt.return sock

  let start listen_address port (t : S.t) =
    let serve =
      let* sock = create_socket listen_address port in
      create_server sock t ()
    in
    serve
end

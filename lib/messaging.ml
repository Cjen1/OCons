open Types
open Lwt.Infix

let ( >>>= ) a b = Lwt_result.bind a b

let msg = Logs.Src.create "Msg" ~doc:"Messaging module"

module Log = (val Logs.src_log msg : Logs.LOG)

module API = Messaging_api.Make [@inlined] (Capnp.BytesMessage)

let message_of_builder = Capnp.BytesMessage.StructStorage.message_of_builder

let builder_of_message = Capnp.BytesMessage.StructStorage.message_of_builder

let command_from_capnp command =
  let open API.Reader.Command in
  let op =
    let op = op_get command in
    let open API.Reader.Op in
    let key = key_get op in
    match get op with
    | Undefined _ ->
        assert false
    | Read ->
        StateMachine.Read key
    | Write v ->
        let value = API.Reader.Op.Write.value_get v in
        StateMachine.Write (key, value)
  in
  let id = id_get_int_exn command in
  StateMachine.{op; id}

let command_to_capnp cmd_root (command : command) =
  let op_root = API.Builder.Command.op_init cmd_root in
  API.Builder.Command.id_set_int cmd_root command.id ;
  match command.op with
  | Read key ->
      API.Builder.Op.key_set op_root key ;
      API.Builder.Op.read_set op_root
  | Write (key, value) ->
      API.Builder.Op.key_set op_root key ;
      let write = API.Builder.Op.write_init op_root in
      API.Builder.Op.Write.value_set write value

let result_from_capnp result =
  let open API.Reader.CommandResult in
  match get result with
  | Success ->
      StateMachine.Success
  | ReadSuccess s ->
      StateMachine.ReadSuccess s
  | Failure ->
      StateMachine.Failure
  | Undefined i ->
      Fmt.failwith "Got undefined result %d" i

let result_to_capnp cr result =
  let open API.Builder.CommandResult in
  match result with
  | StateMachine.Success ->
      success_set cr
  | StateMachine.ReadSuccess s ->
      read_success_set cr s
  | StateMachine.Failure ->
      failure_set cr

let log_entry_from_capnp entry =
  let open API.Reader.LogEntry in
  let command = command_get entry |> command_from_capnp in
  let term = term_get_int_exn entry in
  let index = index_get_int_exn entry in
  {command; term; index}

let log_entry_to_capnp entry =
  let open API.Builder.LogEntry in
  let root = init_root () in
  let cmd_root = API.Builder.LogEntry.command_init root in
  command_to_capnp cmd_root entry.command ;
  term_set_int root entry.term ;
  index_set_int root entry.index ;
  root

module ConnManager = struct
  type address = Unix of string | TCP of (string * int)

  type incomming = Msg_layer.Incomming_socket.t

  type outgoing =
    {sock: Msg_layer.Outgoing_socket.t; addr: address; switch: Lwt_switch.t}

  type message = API.Reader.ServerMessage.t

  type t =
    { mutable outgoing_conns: (int * outgoing) list
    ; id: int
    ; retry_connect_timeout: float
    ; recv_stream: (message * int) Lwt_stream.t
    ; recv_stream_push: message * int -> unit Lwt.t }

  let addr_of_host host =
    match Unix.gethostbyname host with
    | exception Not_found ->
        Fmt.failwith "Unknown host %S" host
    | addr ->
        if Array.length addr.Unix.h_addr_list = 0 then
          Fmt.failwith "No addresses found for host name %S" host
        else addr.Unix.h_addr_list.(0)

  let connect_socket = function
    | Unix path ->
        Log.info (fun f -> f "Connecting to %S..." path) ;
        let socket =
          Unix.(socket PF_UNIX SOCK_STREAM 0) |> Lwt_unix.of_unix_file_descr
        in
        Lwt_unix.connect socket (Unix.ADDR_UNIX path) >|= fun () -> socket
    | TCP (host, port) ->
        Log.info (fun f -> f "Connecting to %s:%d..." host port) ;
        let socket = Unix.(socket PF_INET SOCK_STREAM 0) in
        Unix.setsockopt socket Unix.SO_KEEPALIVE true ;
        let socket = Lwt_unix.of_unix_file_descr socket in
        Lwt_unix.connect socket (Unix.ADDR_INET (addr_of_host host, port))
        >|= fun () -> socket

  let bind_socket = function
    | Unix path ->
        Log.info (fun f -> f "Binding to %S..." path) ;
        ( match Unix.lstat path with
        | {Unix.st_kind= Unix.S_SOCK; _} ->
            Unix.unlink path
        | _ ->
            ()
        | exception Unix.Unix_error (Unix.ENOENT, _, _) ->
            () ) ;
        let socket = Unix.(socket PF_UNIX SOCK_STREAM 0) in
        let socket = socket |> Lwt_unix.of_unix_file_descr in
        Lwt_unix.bind socket (Unix.ADDR_UNIX path) >|= fun () -> socket
    | TCP (host, port) ->
        Log.info (fun f -> f "Binding to %s:%d..." host port) ;
        let socket = Unix.(socket PF_INET SOCK_STREAM 0) in
        Unix.setsockopt socket Unix.SO_REUSEADDR true ;
        let socket = Lwt_unix.of_unix_file_descr socket in
        Lwt_unix.bind socket (Unix.ADDR_INET (addr_of_host host, port))
        >|= fun () -> socket

  let create_incomming t addr () : unit Lwt.t =
    Lwt_result.catch (bind_socket addr)
    >>>= (fun socket ->
           try Lwt_unix.listen socket 128 ; Lwt.return_ok socket
           with e -> Lwt.return_error e)
    >>= function
    | Error e ->
        Fmt.failwith "Failed when binding socket: %a" Fmt.exn e
    | Ok socket ->
        let open API.Reader in
        let rec recv_loop client id =
          Msg_layer.Incomming_socket.recv client 
          >>>= fun msg ->
          let msg = ServerMessage.of_message msg in
          t.recv_stream_push (msg, id)
          >>= fun () -> (recv_loop [@tailcall]) client id
        in
        let rec accept_loop () =
          Lwt_result.catch (Lwt_unix.accept socket)
          >>= function
          | Error e ->
              Fmt.failwith "Failed when accepting connection: %a" Fmt.exn e
          | Ok (client_sock, _) ->
              let handler client_sock () =
                let recv =
                  Msg_layer.Incomming_socket.recv client_sock >>= fun _ ->
                  Msg_layer.Incomming_socket.recv client_sock
                  >>>= fun msg ->
                  let msg = ServerMessage.of_message msg in
                  match ServerMessage.get msg with
                  | ServerMessage.Register msg ->
                      recv_loop client_sock (Register.id_get_int_exn msg)
                  | _ ->
                      Lwt.return_error
                        (`Msg "Did not first get register from client_sock")
                in
                recv
                >>= function
                | Ok () ->
                    Log.info (fun m -> m "Socket closed") ;
                    Lwt.return_unit
                | Error `Closed ->
                    Log.err (fun m -> m "Socket closed unexpectedly") ;
                    Lwt.return_unit
                | Error (`Msg v) ->
                    Log.err (fun m -> m "Socket closed with msg %s" v) ;
                    Lwt.return_unit
              in
              let switch = Lwt_switch.create () in
              Lwt_switch.add_hook (Some switch) (fun () ->
                  Lwt_unix.close client_sock) ;
              let client_sock =
                Msg_layer.Incomming_socket.create ~switch client_sock
              in
              Lwt.async (handler client_sock) ;
              (accept_loop [@tailcall]) ()
        in
        accept_loop ()

  let rec add_outgoing t addr id =
    let connect () =
      Log.info (fun m -> m "Trying to connect to %d" id) ;
      Lwt_result.catch (connect_socket addr)
      >>= function
      | Error ex ->
          Lwt.return_error (`Exn ex)
      | Ok socket ->
          let switch = Lwt_switch.create () in
          Lwt_switch.add_hook (Some switch) (fun () -> Lwt_unix.close socket) ;
          let outgoing = Msg_layer.Outgoing_socket.create ~switch socket in
          let open API.Builder in
          let root = ServerMessage.init_root () in
          let r = ServerMessage.register_init root in
          Register.id_set_int r t.id ;
          Msg_layer.Outgoing_socket.send outgoing (message_of_builder root) >>= fun _ -> 
          Msg_layer.Outgoing_socket.send outgoing (message_of_builder root) >>= fun _ -> 
          Msg_layer.Outgoing_socket.send outgoing (message_of_builder root)
          >>>= fun () ->
          t.outgoing_conns <-
            (id, {sock= outgoing; addr; switch}) :: t.outgoing_conns ;
          Lwt.return_ok ()
    in
    connect ()
    >>= function
    | Ok () ->
        Log.info (fun m -> m "Connected to %d" id) ;
        Lwt.return_unit
    | Error `Closed ->
        Log.err (fun m -> m "Failed to connect, already closed... retrying") ;
        Lwt_unix.sleep t.retry_connect_timeout
        >>= fun () -> add_outgoing t addr id
    | Error (`Exn e) ->
        Log.err (fun m -> m "Failed to connect, retrying %a" Fmt.exn e) ;
        Lwt_unix.sleep t.retry_connect_timeout
        >>= fun () -> add_outgoing t addr id

  let send t id msg =
    let fail_handle () =
      match List.assoc_opt id t.outgoing_conns with
      | None ->
          Lwt.return_unit (* If not in list then already getting re-added *)
      | Some conn ->
          t.outgoing_conns <- List.remove_assoc id t.outgoing_conns ;
          Lwt.async (fun () -> add_outgoing t conn.addr id) ;
          Lwt.return_unit
    in
    match List.assoc_opt id t.outgoing_conns with
    | None ->
        fail_handle ()
    | Some conn -> (
        Msg_layer.Outgoing_socket.send conn.sock msg
        >>= function Error _ -> fail_handle () | Ok () -> Lwt.return_unit )

  let recv t = Lwt_stream.next t.recv_stream

  let create ?(buf_size = 1000) ?(retry_connect_timeout = 0.1) listen_address
      addresses id =
    let recv_stream, push_obj = Lwt_stream.create_bounded buf_size in
    let recv_stream_push = push_obj#push in
    let addresses = List.filter (fun (v, _) -> v <> id) addresses in
    let t =
      { id
      ; retry_connect_timeout
      ; recv_stream
      ; recv_stream_push
      ; outgoing_conns= [] }
    in
    Lwt.async (create_incomming t listen_address) ;
    Lwt.async (fun () ->
        Lwt_list.iter_p (fun (id, addr) -> add_outgoing t addr id) addresses) ;
    t

  let listen mgr handler () : unit Lwt.t =
    let rec handle_loop () =
      let open Lwt.Infix in
      recv mgr
      >>= fun (req_message, id) ->
      Lwt.async (fun () -> handler id req_message) ;
      (handle_loop [@tailcall]) ()
    in
    handle_loop ()
end

(** [Send] contains a few utility functions and the main user facing api's *)
module Send = struct
  module Socket = Msg_layer.Outgoing_socket

  type service = int

  open API.Builder

  let message_size = 256

  let requestVote conn_mgr (t : service) ~term ~leader_commit =
    let root = ServerMessage.init_root ~message_size () in
    let rv = ServerMessage.request_vote_init root in
    RequestVote.term_set_int rv term ;
    RequestVote.leader_commit_set_int rv leader_commit ;
    ConnManager.send conn_mgr t (message_of_builder root)

  let requestVoteResp conn_mgr (t : service) ~term ~voteGranted ~entries =
    let root = ServerMessage.init_root ~message_size () in
    let rvr = ServerMessage.request_vote_resp_init root in
    RequestVoteResp.term_set_int rvr term ;
    RequestVoteResp.vote_granted_set rvr voteGranted ;
    let _residual_reference =
      RequestVoteResp.entries_set_list rvr (List.map log_entry_to_capnp entries)
    in
    ConnManager.send conn_mgr t (message_of_builder root)

  let appendEntries conn_mgr (t : service) ~term ~prevLogIndex ~prevLogTerm
      ~entries ~leaderCommit =
    let root = ServerMessage.init_root ~message_size () in
    let ae = ServerMessage.append_entries_init root in
    AppendEntries.term_set_int ae term ;
    AppendEntries.prev_log_index_set_int ae prevLogIndex ;
    AppendEntries.prev_log_term_set_int ae prevLogTerm ;
    let _residual_reference =
      AppendEntries.entries_set_list ae (List.map log_entry_to_capnp entries)
    in
    AppendEntries.leader_commit_set_int ae leaderCommit ;
    ConnManager.send conn_mgr t (message_of_builder root)

  let appendEntriesResp conn_mgr (t : service) ~term ~success ~matchIndex =
    let root = ServerMessage.init_root ~message_size () in
    let aer = ServerMessage.append_entries_resp_init root in
    AppendEntriesResp.term_set_int aer term ;
    AppendEntriesResp.success_set aer success ;
    AppendEntriesResp.match_index_set_int aer matchIndex ;
    ConnManager.send conn_mgr t (message_of_builder root)

  let clientRequest conn_mgr (t : service) ~command =
    let root = ServerMessage.init_root ~message_size () in
    let crq = ServerMessage.client_request_init root in
    command_to_capnp crq command ;
    ConnManager.send conn_mgr t (message_of_builder root)

  let clientResponse conn_mgr (t : service) ~id ~result =
    let root = ServerMessage.init_root ~message_size () in
    let crp = ServerMessage.client_response_init root in
    ClientResponse.id_set_int crp id ;
    let cr = ClientResponse.result_init crp in
    let () =
      match result with
      | StateMachine.Success ->
          CommandResult.success_set cr
      | StateMachine.ReadSuccess s ->
          CommandResult.read_success_set cr s
      | StateMachine.Failure ->
          CommandResult.failure_set cr
    in
    ConnManager.send conn_mgr t (message_of_builder root)

  let register conn_mgr (t : service) ~id =
    let root = ServerMessage.init_root ~message_size () in
    let r = ServerMessage.register_init root in
    Register.id_set_int r id ;
    ConnManager.send conn_mgr t (message_of_builder root)
end

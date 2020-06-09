open Types
open Lwt.Infix

let ( >>>= ) a b = Lwt_result.bind a b

let msg = Logs.Src.create "Msg" ~doc:"Messaging module"

module Log = (val Logs.src_log msg : Logs.LOG)

module API = Messaging_api.Make [@inlined] (Capnp.BytesMessage)

let message_of_builder = Capnp.BytesMessage.StructStorage.message_of_builder

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

type address = Unix of string | TCP of (string * int)

module ConnUtils = struct
  let pp_addr f = function 
    | Unix s -> 
      Fmt.pf f "unix:%s" s
    | TCP (s, p) -> 
      Fmt.pf f "tcp://%s:%d" s p

  let addr_of_string s = 
    match String.split_on_char ':' s with
    | [addr] -> Ok (Unix addr)
    | [addr;port] -> begin
      match int_of_string_opt port with
      | Some port ->
        Ok (TCP (addr, port))
      | None -> 
        Error (`Msg (Fmt.str "Port (%s) is not a valid int" port))
    end 
    | _ -> Error (`Msg (Fmt.str "Expected address of the form ip:port, not %s"  s))

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

  let accept_incomming addr recv_handler ~switch () : unit Lwt.t =
    Lwt_result.catch (bind_socket addr)
    >>>= (fun socket ->
           try Lwt_unix.listen socket 128 ; Lwt.return_ok socket
           with e -> Lwt.return_error e)
    >>= function
    | Error e ->
        Fmt.failwith "Failed when binding socket: %a" Fmt.exn e
    | Ok socket ->
        Lwt_switch.add_hook (Some switch) (fun () -> Lwt_unix.close socket) ;
        let open API.Reader in
        let rec accept_loop () : unit Lwt.t=
          Lwt_result.catch (Lwt_unix.accept socket)
          >>= function
          | Error e when Lwt_switch.is_on switch ->
              Fmt.failwith "Failed when accepting connection: %a" Fmt.exn e
          | Error _ -> Lwt.return_unit
          | Ok (client_sock_raw, _) ->
              let accept_handler client_sock switch () =
                let recv =
                  Msg_layer.Incomming_socket.recv client_sock
                  >>>= fun msg ->
                  let msg = ServerMessage.of_message msg in
                  match ServerMessage.get msg with
                  | ServerMessage.Register msg ->
                      let id = Register.id_get_int_exn msg in
                      recv_handler client_sock client_sock_raw switch id
                  | _ ->
                      Lwt.return_error
                        (`Msg "Did not first get register from client_sock")
                in
                recv
                >>= fun res ->
                ( match res with
                | Ok () ->
                    Log.info (fun m -> m "Recv thread finished")
                | Error `Closed ->
                    Log.err (fun m -> m "Socket closed unexpectedly")
                | Error (`Msg v) ->
                    Log.err (fun m -> m "Socket closed with msg %s" v) ) ;
                Lwt_switch.turn_off switch
              in
              let switch = Lwt_switch.create () in
              Lwt_switch.add_hook (Some switch) (fun () ->
                  Lwt_unix.close client_sock_raw) ;
              let client_sock =
                Msg_layer.Incomming_socket.create ~switch client_sock_raw
              in
              Lwt.async (accept_handler client_sock switch) ;
              (accept_loop [@tailcall]) ()
        in
        accept_loop ()

  let rec connect_outgoing switch addr src_id retry_timeout =
    let connect () =
      Log.info (fun m -> m "Trying to connect to %a" pp_addr addr) ;
      Lwt_result.catch (connect_socket addr)
      >>= function
      | Error ex ->
          Lwt.return_error (`Exn ex)
      | Ok socket ->
          let switch = switch in
          Lwt_switch.add_hook (Some switch) (fun () -> Lwt_unix.close socket) ;
          let outgoing = Msg_layer.Outgoing_socket.create ~switch socket in
          let open API.Builder in
          let root = ServerMessage.init_root () in
          let r = ServerMessage.register_init root in
          Register.id_set_int r src_id ;
          (*Msg_layer.Outgoing_socket.send outgoing (message_of_builder root) >>= fun _ ->*)
          Msg_layer.Outgoing_socket.send outgoing (message_of_builder root) |> Lwt.return
          >>>= fun () -> Lwt.return_ok (outgoing, socket)
    in
    let rec loop () =
      connect ()
      >>= function
      | Ok res ->
          Log.info (fun m -> m "Connected to %a" pp_addr addr) ;
          Lwt.return res
      | Error `Closed ->
          Log.err (fun m -> m "Failed to connect, already closed... retrying") ;
          Lwt_unix.sleep retry_timeout >>= fun () -> loop ()
      | Error (`Exn e) ->
          Log.err (fun m -> m "Failed to connect, %a retrying " Fmt.exn e) ;
          Lwt_unix.sleep retry_timeout >>= fun () -> loop ()
    in
    loop ()
end

module ConnManager = struct
  open ConnUtils

  type incomming = Msg_layer.Incomming_socket.t

  type outgoing =
    {sock: Msg_layer.Outgoing_socket.t; addr: address; switch: Lwt_switch.t}

  type message = API.Reader.ServerMessage.t

  type t =
    { mutable outgoing_conns: (int * outgoing) list
    ; mutable outgoing_client_conns: (int * Msg_layer.Outgoing_socket.t) list
    ; outgoing_cond: unit Lwt_condition.t
    ; id: int
    ; switch: Lwt_switch.t
    ; retry_connect_timeout: float
    ; recv_stream: (message * int) Lwt_stream.t
    ; recv_stream_push: message * int -> unit Lwt.t }

  let create_incomming t addr () =
    let handler client_sock _raw _switch id =
      let rec loop () =
        Msg_layer.Incomming_socket.recv client_sock
        >>>= fun msg ->
        let msg = API.Reader.ServerMessage.of_message msg in
        t.recv_stream_push (msg, id) >>= fun () -> (loop [@tailcall]) ()
      in
      loop ()
    in
    accept_incomming addr ~switch:t.switch handler ()

  let create_client_incomming t addr () =
    let handler client_sock raw switch id =
      let rec loop () =
        Msg_layer.Incomming_socket.recv client_sock
        >>>= fun msg ->
        let msg = API.Reader.ServerMessage.of_message msg in
        t.recv_stream_push (msg, id) >>= fun () -> (loop [@tailcall]) ()
      in
      let outgoing = Msg_layer.Outgoing_socket.create ~switch raw in
      Lwt_switch.add_hook (Some switch) (fun () ->
          t.outgoing_client_conns <-
            List.filter (fun (i, _) -> i <> id) t.outgoing_client_conns ;
          Lwt.return_unit) ;
      t.outgoing_client_conns <- (id, outgoing) :: t.outgoing_client_conns ;
      loop ()
    in
    accept_incomming addr ~switch:t.switch handler ()

  let add_outgoing t addr id =
    connect_outgoing t.switch addr t.id t.retry_connect_timeout
    >>= fun (outgoing, _) ->
    t.outgoing_conns <-
      (id, {sock= outgoing; addr; switch= t.switch}) :: t.outgoing_conns ;
    Lwt_condition.broadcast t.outgoing_cond () ;
    Lwt.return ()

  let rec send sym t id msg =
    let fail_handle () =
      match (List.assoc_opt id t.outgoing_conns, sym) with
      | None, `AtLeastOnce ->
          Lwt_condition.wait t.outgoing_cond >>= fun () -> send sym t id msg
      | Some conn, `AtLeastOnce ->
          t.outgoing_conns <- List.remove_assoc id t.outgoing_conns ;
          Lwt.async (fun () -> add_outgoing t conn.addr id) ;
          Lwt_condition.wait t.outgoing_cond >>= fun () -> send sym t id msg
      | None, `AtMostOnce ->
          Lwt.return_unit
      | Some conn, `AtMostOnce ->
          t.outgoing_conns <- List.remove_assoc id t.outgoing_conns ;
          Lwt.async (fun () -> add_outgoing t conn.addr id) ;
          Lwt.return_unit
    in
    match
      ( List.assoc_opt id t.outgoing_conns
      , List.assoc_opt id t.outgoing_client_conns )
    with
    | Some conn, _ -> (
        Msg_layer.Outgoing_socket.send conn.sock msg |> Lwt.return
        >>= function Error _ -> fail_handle () | Ok () -> Lwt.return_unit )
    | None, Some conn -> (
        Msg_layer.Outgoing_socket.send conn msg |> Lwt.return
        >>= function Error _ -> fail_handle () | Ok () -> Lwt.return_unit )
    | None, None ->
        fail_handle ()

  let recv t = Lwt_stream.next t.recv_stream

  let close mgr = Lwt_switch.turn_off mgr.switch

  let listen mgr handler () : unit Lwt.t =
    let rec handle_loop () =
      let open Lwt.Infix in
      recv mgr
      >>= fun (req_message, id) ->
      Lwt.async (fun () -> handler id req_message) ;
      (handle_loop [@tailcall]) ()
    in
    handle_loop ()

  let create ?(buf_size = 1000) ?(retry_connect_timeout = 0.1) listen_address
      client_address addresses id =
    let recv_stream, push_obj = Lwt_stream.create_bounded buf_size in
    let recv_stream_push = push_obj#push in
    let addresses = List.filter (fun (v, _) -> v <> id) addresses in
    let t =
      { id
      ; switch= Lwt_switch.create ()
      ; retry_connect_timeout
      ; recv_stream
      ; recv_stream_push
      ; outgoing_conns= []
      ; outgoing_client_conns= []
      ; outgoing_cond= Lwt_condition.create () }
    in
    Lwt.async (create_incomming t listen_address) ;
    Lwt.async (create_client_incomming t client_address) ;
    Lwt.async (fun () ->
        Lwt_list.iter_p
          (fun (id, addr) ->
            add_outgoing t addr id
            >>= fun () ->
            Log.debug (fun f -> f "Connected to %d" id) |> Lwt.return)
          addresses) ;
    t
end

module ClientConn = struct
  open ConnUtils

  type message = API.Reader.ServerMessage.t

  type t =
    { retry_timeout: float
    ; mutable conns:
        ( address
        * ( Msg_layer.Outgoing_socket.t
          * Msg_layer.Incomming_socket.t
          * Lwt_switch.t ) )
        list
    ; conn_cond: unit Lwt_condition.t
    ; id: int
    ; recv_stream: message Lwt_stream.t
    ; recv_stream_push: message -> unit Lwt.t }

  let rec add_connection t addr =
    let switch = Lwt_switch.create () in
    connect_outgoing switch addr t.id t.retry_timeout
    >>= fun (outgoing, raw) ->
    let incoming = Msg_layer.Incomming_socket.create ~switch raw in
    let recv_handler () =
      let rec loop () =
        Msg_layer.Incomming_socket.recv incoming
        >>>= fun msg ->
        Log.debug (fun m -> m "client got message, pushing onto stream");
        t.recv_stream_push (API.Reader.ServerMessage.of_message msg)
        >>= fun () -> 
        Log.debug (fun m -> m "Pushed onto stream");
        loop ()
      in
      loop ()
      >>= function
      | Ok () ->
          Fmt.failwith "Loop ended unexpectedly"
      | Error `Closed ->
          Log.info (fun m -> m "Socket closed") ;
          Lwt_switch.turn_off switch
          >>= fun () -> (add_connection [@tailcall]) t addr
    in
    Lwt.async recv_handler ;
    t.conns <- (addr, (outgoing, incoming, switch)) :: t.conns ;
    Lwt_switch.add_hook (Some switch) (fun () ->
        t.conns <- List.filter (fun (i, _) -> i <> addr) t.conns ;
        Lwt.return_unit) ;
    Lwt_condition.broadcast t.conn_cond ();
    Lwt.return_unit

  let send t addr msg =
    let rec loop () =
      match List.assoc_opt addr t.conns with
      | None ->
          Lwt_condition.wait t.conn_cond >>= fun () -> loop ()
      | Some (outgoing, _, switch) -> (
          Log.debug (fun m -> m "Sending");
          Msg_layer.Outgoing_socket.send outgoing msg |> Lwt.return
          >>= function
          | Ok () ->
              Log.debug (fun m -> m "Sent");
              Lwt.return_unit
          | Error `Closed ->
              Log.debug (fun m -> m "Failed to send retrying");
              Lwt.async (fun () -> Lwt_switch.turn_off switch) ;
              Lwt_condition.wait t.conn_cond >>= fun () -> loop () )
    in
    loop ()

  let recv t = 
    Log.debug (fun m -> m "waiting for msg");
    Lwt_stream.next t.recv_stream >>= fun msg -> 
    Log.debug (fun m -> m "received msg");
    Lwt.return msg

  let create ?(bound = 128) ?(retry_timeout = 0.1) ~id () =
    let recv_stream, push_obj = Lwt_stream.create_bounded bound in
    { retry_timeout
    ; conns= []
    ; conn_cond= Lwt_condition.create ()
    ; id
    ; recv_stream
    ; recv_stream_push= push_obj#push }
end

(** [Send] contains a few utility functions and the main user facing api's *)
module Send = struct
  module Socket = Msg_layer.Outgoing_socket

  type service = int

  open API.Builder

  let message_size = 256

  module Serialise = struct
    let requestVote ~term ~leaderCommit =
      let root = ServerMessage.init_root ~message_size () in
      let rv = ServerMessage.request_vote_init root in
      RequestVote.term_set_int rv term ;
      RequestVote.leader_commit_set_int rv leaderCommit ;
      message_of_builder root

    let requestVoteResp ~term ~voteGranted ~entries =
      let root = ServerMessage.init_root ~message_size () in
      let rvr = ServerMessage.request_vote_resp_init root in
      RequestVoteResp.term_set_int rvr term ;
      RequestVoteResp.vote_granted_set rvr voteGranted ;
      let _residual_reference =
        RequestVoteResp.entries_set_list rvr
          (List.map log_entry_to_capnp entries)
      in
      message_of_builder root

    let appendEntries ~term ~prevLogIndex ~prevLogTerm ~entries ~leaderCommit =
      let root = ServerMessage.init_root ~message_size () in
      let ae = ServerMessage.append_entries_init root in
      AppendEntries.term_set_int ae term ;
      AppendEntries.prev_log_index_set_int ae prevLogIndex ;
      AppendEntries.prev_log_term_set_int ae prevLogTerm ;
      let _residual_reference =
        AppendEntries.entries_set_list ae (List.map log_entry_to_capnp entries)
      in
      AppendEntries.leader_commit_set_int ae leaderCommit ;
      message_of_builder root

    let appendEntriesResp ~term ~success ~matchIndex =
      let root = ServerMessage.init_root ~message_size () in
      let aer = ServerMessage.append_entries_resp_init root in
      AppendEntriesResp.term_set_int aer term ;
      AppendEntriesResp.success_set aer success ;
      AppendEntriesResp.match_index_set_int aer matchIndex ;
      message_of_builder root

    let clientRequest ~command =
      let root = ServerMessage.init_root ~message_size () in
      let crq = ServerMessage.client_request_init root in
      command_to_capnp crq command ;
      message_of_builder root

    let clientResponse ~id ~result =
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
      message_of_builder root

    let register ~id =
      let root = ServerMessage.init_root ~message_size () in
      let r = ServerMessage.register_init root in
      Register.id_set_int r id ; message_of_builder root
  end

  module RawSocket = struct
    let clientRequest sock ~command =
      Msg_layer.Outgoing_socket.send sock (Serialise.clientRequest ~command)

    let clientResponse sock ~id ~result =
      Msg_layer.Outgoing_socket.send sock (Serialise.clientResponse ~id ~result)

    let register sock ~id =
      Msg_layer.Outgoing_socket.send sock (Serialise.register ~id)
  end

  let requestVote ?(sym = `AtMostOnce) conn_mgr (t : service) ~term
      ~leaderCommit =
    ConnManager.send sym conn_mgr t (Serialise.requestVote ~term ~leaderCommit)

  let requestVoteResp ?(sym = `AtMostOnce) conn_mgr (t : service) ~term
      ~voteGranted ~entries =
    ConnManager.send sym conn_mgr t
      (Serialise.requestVoteResp ~term ~voteGranted ~entries)

  let appendEntries ?(sym = `AtMostOnce) conn_mgr (t : service) ~term
      ~prevLogIndex ~prevLogTerm ~entries ~leaderCommit =
    ConnManager.send sym conn_mgr t
      (Serialise.appendEntries ~term ~prevLogIndex ~prevLogTerm ~entries
         ~leaderCommit)

  let appendEntriesResp ?(sym = `AtMostOnce) conn_mgr (t : service) ~term
      ~success ~matchIndex =
    ConnManager.send sym conn_mgr t
      (Serialise.appendEntriesResp ~term ~success ~matchIndex)

  let clientRequest ?(sym = `AtMostOnce) conn_mgr (t : service) ~command =
    ConnManager.send sym conn_mgr t (Serialise.clientRequest ~command)

  let clientResponse ?(sym = `AtMostOnce) conn_mgr (t : service) ~id ~result =
    ConnManager.send sym conn_mgr t (Serialise.clientResponse ~id ~result)

  let register ?(sym = `AtMostOnce) conn_mgr (t : service) ~id =
    ConnManager.send sym conn_mgr t (Serialise.register ~id)
end

open Types
open Unix_capnp_messaging

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
  let id = id_get command in
  StateMachine.{op; id}

let command_to_capnp cmd_root (command : command) =
  let op_root = API.Builder.Command.op_init cmd_root in
  API.Builder.Command.id_set cmd_root command.id ;
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
  let command_id = command_id_get entry in
  let term = term_get entry in
  {command_id; term}

let log_entry_to_capnp entry =
  let open API.Builder.LogEntry in
  let root = init_root () in
  API.Builder.LogEntry.command_id_set root entry.command_id ;
  term_set root entry.term ;
  root

(** [Send] contains a few utility functions and the main user facing api's *)
module Send = struct
  type service = int64

  open API.Builder

  let message_size = 256

  module Serialise = struct
    let requestVote ~term ~leaderCommit =
      let root = ServerMessage.init_root ~message_size () in
      let rv = ServerMessage.request_vote_init root in
      RequestVote.term_set rv term ;
      RequestVote.leader_commit_set rv leaderCommit ;
      message_of_builder root

    let requestVoteResp ~term ~voteGranted ~entries =
      let root = ServerMessage.init_root ~message_size () in
      let rvr = ServerMessage.request_vote_resp_init root in
      RequestVoteResp.term_set rvr term ;
      RequestVoteResp.vote_granted_set rvr voteGranted ;
      let _residual_reference =
        RequestVoteResp.entries_set_list rvr
          (List.map log_entry_to_capnp entries)
      in
      message_of_builder root

    let appendEntries ~term ~prevLogIndex ~prevLogTerm ~entries ~leaderCommit =
      let root = ServerMessage.init_root ~message_size () in
      let ae = ServerMessage.append_entries_init root in
      AppendEntries.term_set ae term ;
      AppendEntries.prev_log_index_set ae prevLogIndex ;
      AppendEntries.prev_log_term_set ae prevLogTerm ;
      let _residual_reference =
        AppendEntries.entries_set_list ae (List.map log_entry_to_capnp entries)
      in
      AppendEntries.leader_commit_set ae leaderCommit ;
      message_of_builder root

    let appendEntriesResp ~term ~success =
      let root = ServerMessage.init_root ~message_size () in
      let aer = ServerMessage.append_entries_resp_init root in
      AppendEntriesResp.term_set aer term ;
      let () =
        match success with
        | Ok mi ->
            AppendEntriesResp.success_set aer mi
        | Error pli ->
            AppendEntriesResp.failure_set aer pli
      in
      message_of_builder root

    let clientRequest ~command =
      let root = ServerMessage.init_root ~message_size () in
      let crq = ServerMessage.client_request_init root in
      command_to_capnp crq command ;
      message_of_builder root

    let clientResponse ~id ~result =
      let root = ServerMessage.init_root ~message_size () in
      let crp = ServerMessage.client_response_init root in
      ClientResponse.id_set crp id ;
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

    let requestsAfter ~index = 
      let root = ServerMessage.init_root ~message_size () in
      ServerMessage.requests_after_set root index;
      message_of_builder root

    let requestUpdate ~commands = 
      let root = ServerMessage.init_root ~message_size () in
      let commands = List.map (fun cmd ->
          let root = Command.init_root ~message_size () in
          command_to_capnp root cmd;
          root ) commands 
      in 
      let _ = ServerMessage.requests_update_set_list root commands in
      message_of_builder root
  end

  let requestVote ?(sem = `AtMostOnce) conn_mgr (t : service) ~term
      ~leaderCommit =
    Conn_manager.send ~semantics:sem conn_mgr t
      (Serialise.requestVote ~term ~leaderCommit)

  let requestVoteResp ?(sem = `AtMostOnce) conn_mgr (t : service) ~term
      ~voteGranted ~entries =
    Conn_manager.send ~semantics:sem conn_mgr t
      (Serialise.requestVoteResp ~term ~voteGranted ~entries)

  let appendEntries ?(sem = `AtMostOnce) conn_mgr (t : service) ~term
      ~prevLogIndex ~prevLogTerm ~entries ~leaderCommit =
    Conn_manager.send ~semantics:sem conn_mgr t
      (Serialise.appendEntries ~term ~prevLogIndex ~prevLogTerm ~entries
         ~leaderCommit)

  let appendEntriesResp ?(sem = `AtMostOnce) conn_mgr (t : service) ~term
      ~success =
    Conn_manager.send ~semantics:sem conn_mgr t
      (Serialise.appendEntriesResp ~term ~success)

  let clientRequest ?(sem = `AtMostOnce) conn_mgr (t : service) ~command =
    Conn_manager.send ~semantics:sem conn_mgr t
      (Serialise.clientRequest ~command)

  let clientResponse ?(sem = `AtMostOnce) conn_mgr (t : service) ~id ~result =
    Conn_manager.send ~semantics:sem conn_mgr t
      (Serialise.clientResponse ~id ~result)

  let requestsAfter ?(sem = `AtLeastOnce) conn_mgr (t : service) ~index = 
    Conn_manager.send ~semantics:sem conn_mgr t
      (Serialise.requestsAfter ~index)

  let requestUpdate ?(sem = `AtLeastOnce) conn_mgr (t : service) ~commands =
    Conn_manager.send ~semantics:sem conn_mgr t
      (Serialise.requestUpdate ~commands)
end

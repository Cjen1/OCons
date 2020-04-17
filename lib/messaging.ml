open Types

let msg = Logs.Src.create "Msg" ~doc:"Messaging module"

module MLog = (val Logs.src_log msg : Logs.LOG)

module API = Messaging_api.MakeRPC (Capnp_rpc_lwt)
open Capnp_rpc_lwt

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

let log_entry_from_capnp entry =
  let open API.Reader.LogEntry in
  let command = command_get entry |> command_from_capnp in
  let term = term_get_int_exn entry in
  let index = index_get_int_exn entry in
  {command; term; index}

let command_to_capnp cmd_root (command : command) =
  let op_root = API.Builder.Command.op_init cmd_root in
  API.Builder.Command.id_set_int cmd_root command.id;
  match command.op with
  | Read key ->
      API.Builder.Op.key_set op_root key ;
      API.Builder.Op.read_set op_root
  | Write (key, value) ->
      API.Builder.Op.key_set op_root key ;
      let write = API.Builder.Op.write_init op_root in
      API.Builder.Op.Write.value_set write value

let log_entry_to_capnp entry =
  let open API.Builder.LogEntry in
  let root = init_root () in
  let cmd_root = API.Builder.LogEntry.command_init root in
  command_to_capnp cmd_root entry.command ;
  term_set_int root entry.term;
  index_set_int root entry.index;
  root

type 'a repairable_ref =
  { mutable cap: 'a Capability.t
  ; sr: 'a Sturdy_ref.t
  ; mutable repaired: unit Lwt.t }

module Send = struct
  (* Module containing a few utility functions and the main client facing api's *)
  module Serv = API.Client.ServiceInterface
  module Cli = API.Client.ClientInterface

  let get_sr_from_path path vat : 'a Sturdy_ref.t Lwt.t =
    let rec wait_until_exists path =
      if%lwt Lwt_unix.file_exists path then Lwt.return_unit
      else
        let%lwt () = Lwt_unix.sleep 1. in
        wait_until_exists path
    in
    let%lwt () = wait_until_exists path in
    match Capnp_rpc_unix.Cap_file.load vat path with
    | Ok sr ->
        Lwt.return sr
    | Error (`Msg m) ->
        failwith m

  type service = Serv.t repairable_ref

  type client_serv = Cli.t repairable_ref

  let is_error_handleable e =
    let open Capnp_rpc.Exception in
    MLog.warn (fun m -> m "Exception got: %a" Capnp_rpc.Exception.pp e) ;
    match e.ty with `Disconnected | `Overloaded -> Ok () | _ -> Error e

  let rec register_reconnect fulfiller t =
    Capability.when_broken
      (fun e ->
        MLog.warn (fun m ->
            m "Capability failed with: %a" Capnp_rpc.Exception.pp e) ;
        match is_error_handleable e with
        | Error _e ->
            failwith "Unable to recover"
        | Ok () ->
            MLog.warn (fun m ->
                m "Error is recoverable, attempting to recover.") ;
            Lwt.wakeup_later fulfiller () ;
            let repaired', fulfiller' = Lwt.task () in
            let rec p () =
              match%lwt Sturdy_ref.connect t.sr with
              | Ok cap' ->
                  t.cap <- cap' ;
                  t.repaired <- repaired' ;
                  register_reconnect fulfiller' t |> Lwt.return
              | Error e ->
                  let repair_interval = 10. in
                  MLog.err (fun m -> m "Got error repairing cap: %a" Capnp_rpc.Exception.pp e);
                  MLog.warn (fun m ->
                      m "Unable to repair cap, retrying in %f" repair_interval) ;
                  let%lwt () = Lwt_unix.sleep repair_interval in
                  p ()
            in
            Lwt.async p)
      t.cap

  let connect sr =
    let repaired, fulfiller = Lwt.task () in
    let%lwt cap = Sturdy_ref.connect_exn sr in
    let t = {repaired; cap; sr} in
    register_reconnect fulfiller t ;
    Lwt.return t

  let rec requestVote (t : service) ~term ~leader_commit ~src_id =
    let open Serv.RequestVote in
    let request, params = Capability.Request.create Params.init_pointer in
    Params.term_set_int params term ;
    Params.leader_commit_set_int params leader_commit ;
    Params.src_id_set_int params src_id;
    let%lwt result = Capability.call_for_value t.cap method_id request in
    match result with
    | Ok result ->
        let term = Results.term_get_int_exn result in
        let voteGranted = Results.vote_granted_get result in
        let entries = Results.entries_get_list result in
        let entries = List.map log_entry_from_capnp entries in
        Lwt.return {term; voteGranted; entries}
    | Error e -> (
      match e with
      | `Cancelled ->
          raise Lwt.Canceled
      | _ ->
          MLog.err (fun m -> m "retrying after the conn is repaired") ;
          let%lwt () = t.repaired in
          requestVote t ~term ~leader_commit ~src_id)

  let rec appendEntries (t : service) ~term ~prevLogIndex ~prevLogTerm ~entries
      ~leaderCommit =
    let open Serv.AppendEntries in
    let request, params = Capability.Request.create Params.init_pointer in
    Params.term_set_int params term ;
    Params.prev_log_index_set_int params prevLogIndex ;
    Params.prev_log_term_set_int params prevLogTerm ;
    let _residual_reference =
      Params.entries_set_array params
        (List.map log_entry_to_capnp entries |> Array.of_list)
    in
    Params.leader_commit_set_int params leaderCommit ;
    match%lwt Capability.call_for_value t.cap method_id request with
    | Ok result ->
        let term = Results.term_get_int_exn result in
        let success = Results.success_get result in
        Lwt.return {term; success}
    | Error e -> (
      match e with
      | `Cancelled ->
          raise Lwt.Canceled
      | _ ->
          MLog.err (fun m -> m "retrying after the conn is repaired") ;
          let%lwt () = t.repaired in
          appendEntries t ~term ~prevLogIndex ~prevLogTerm ~entries
            ~leaderCommit )

  let rec clientReq (t : client_serv) command =
    let open Cli.ClientRequest in
    let request, params = Capability.Request.create Params.init_pointer in
    let cmd_root = Params.command_init params in
    command_to_capnp cmd_root command ;
    match%lwt Capability.call_for_value t.cap method_id request with
    | Ok result -> (
        let result = Cli.ClientRequest.Results.result_get result in
        let open API.Reader.CommandResult in
        match get result with
        | Success ->
            Lwt.return StateMachine.Success
        | Failure ->
            Lwt.return StateMachine.Success
        | ReadSuccess s ->
            Lwt.return @@ StateMachine.ReadSuccess s
        | Undefined s ->
            failwith
              (Printf.sprintf
                 "Undefined result got from capnp response, idx = %d" s) )
    | Error e -> (
      match e with
      | `Cancelled ->
          raise Lwt.Canceled
      | _ ->
          MLog.err (fun m -> m "retrying after the conn is repaired") ;
          let%lwt () = t.repaired in
          clientReq t command )
end

module type S = sig
  type t

  val request_vote :
    t -> term:term -> leaderCommit:log_index -> src_id:node_id -> request_vote_response Lwt.t

  val append_entries :
       t
    -> term:term
    -> prevLogIndex:log_index
    -> prevLogTerm:term
    -> entries:log_entry list
    -> leaderCommit:log_index
    -> append_entries_response Lwt.t

  val client_req : t -> command -> op_result Lwt.t
end

module Recv (Impl : S) = struct
  let client (t : Impl.t Lwt.t) =
    MLog.debug (fun m -> m "Starting client service") ;
    let module Serv = API.Service.ClientInterface in
    Lwt.return @@ Serv.local
    @@ object
         inherit Serv.service

         method client_request_impl params release_param_caps =
           (* Use a promise to t to fix file ordering issue *)
           let open Serv.ClientRequest in
           let command = Params.command_get params |> command_from_capnp in
           release_param_caps () ;
           let response, results =
             Service.Response.create Results.init_pointer
           in
           let p () =
             let%lwt t = t in
             let%lwt op_result = Impl.client_req t command in
             let root = Results.result_init results in
             let open API.Builder.CommandResult in
             let () =
               match op_result with
               | Success ->
                   success_set root
               | ReadSuccess s ->
                   read_success_set root s
               | Failure ->
                   failure_set root
             in
             Lwt.return_ok response
           in
           Service.return_lwt p
       end

  let local (t : Impl.t Lwt.t) =
    let module Serv = API.Service.ServiceInterface in
    Lwt.return @@ Serv.local
    @@ object
         inherit Serv.service

         method request_vote_impl params release_param_caps =
           (* Use a promise to t to fix file ordering issue *)
           let open Serv.RequestVote in
           let term = Params.term_get_int_exn params in
           let leaderCommit = Params.leader_commit_get_int_exn params in
           let src_id = Params.src_id_get_int_exn params in
           release_param_caps () ;
           let response, results =
             Service.Response.create Results.init_pointer
           in
           let p () =
             let%lwt t = t in
             let%lwt {term; voteGranted; entries} =
               Impl.request_vote t ~term ~leaderCommit ~src_id
             in
             Results.term_set_int results term ;
             Results.vote_granted_set results voteGranted ;
             let _ignored =
               Results.entries_set_list results
                 (List.map log_entry_to_capnp entries)
             in
             Lwt.return_ok response
           in
           Service.return_lwt p

         method append_entries_impl params release_param_caps =
           (* Use a promise to t to fix file ordering issue *)
           let open Serv.AppendEntries in
           let term = Params.term_get_int_exn params in
           let prevLogIndex = Params.prev_log_index_get_int_exn params in
           let prevLogTerm = Params.prev_log_term_get_int_exn params in
           let entries =
             Params.entries_get_list params |> List.map log_entry_from_capnp
           in
           let leaderCommit = Params.leader_commit_get_int_exn params in
           release_param_caps () ;
           let response, results =
             Service.Response.create Results.init_pointer
           in
           let p =
             let%lwt t = t in
             let%lwt {term; success} =
               Impl.append_entries t ~term ~prevLogIndex ~prevLogTerm ~entries
                 ~leaderCommit
             in
             Results.term_set_int results term ;
             Results.success_set results success ;
             Lwt.return_ok response
           in
           Service.return_lwt (fun () -> p)
       end
end

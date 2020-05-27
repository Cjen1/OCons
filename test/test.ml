open Ocamlpaxos
open Lwt.Infix

open Messaging

let addresses =
  let open ConnManager in
  [ 1, TCP ("127.0.0.1", 4000)
  ; 2, TCP ("127.0.0.1", 4001) ]

let src = Logs.Src.create "test"
module Log = (val Logs.src_log src : Logs.LOG)

let mgr id = 
  Log.debug (fun m -> m "creating manager");
  ConnManager.create (List.assoc id addresses) (List.filter (fun (id,_) -> id <> 1) addresses) id

let test_send to_msg send equal =
  let m1,m2 = mgr 1, mgr 2 in
  send m1 2 >>= fun () ->
  ConnManager.recv m2 >>= fun (msg', id) ->
  let msg' = to_msg msg' in
  Crowbar.check_eq id 1;
  Crowbar.check (equal msg');
  Lwt.return_unit

let eq access m m' = 
  access m = access m'

let test_rv term leader_commit = 
  let term = Int32.to_int term in
  let leader_commit = Int32.to_int leader_commit in
  let open API.Reader.RequestVote in
  let send mgr id =
    Send.requestVote mgr id ~term ~leader_commit
  in 
  let equal m' = 
    term = term_get_int_exn m' &&
    leader_commit = leader_commit_get_int_exn m'
  in 
  let to_msg msg = 
    match API.Reader.ServerMessage.get msg with
    | RequestVote msg -> msg
    | Undefined i -> Log.err (fun m -> m "Got %d" i); assert false
    | _ -> assert false
  in 
  Lwt_main.run @@ test_send to_msg send equal

let test_rvr term voteGranted entries = 
  let term = Int32.to_int term in
  let open API.Reader.RequestVoteResp in
  let send mgr id =
    Send.requestVoteResp mgr id ~term ~voteGranted ~entries
  in 
  let equal m' = 
    term = term_get_int_exn m' &&
    voteGranted = vote_granted_get m' &&
    entries = (entries_get_list m' |> List.map Messaging.log_entry_from_capnp)
  in 
  let to_msg msg = 
    match API.Reader.ServerMessage.get msg with
    | RequestVoteResp msg -> msg
    | _ -> assert false
  in 
  Lwt_main.run @@ test_send to_msg send equal

let test_ae term prevLogIndex prevLogTerm entries leaderCommit = 
  let term = Int32.to_int term in
  let prevLogIndex = Int32.to_int prevLogIndex in
  let prevLogTerm = Int32.to_int prevLogTerm in
  let leaderCommit = Int32.to_int leaderCommit in
  let open API.Reader.AppendEntries in
  let send mgr id =
    Send.appendEntries mgr id ~term ~prevLogIndex ~prevLogTerm ~entries ~leaderCommit 
  in 
  let equal m' = 
    term = term_get_int_exn m' &&
    prevLogIndex = prev_log_index_get_int_exn m' &&
    prevLogTerm = prev_log_term_get_int_exn m' &&
    entries = (entries_get_list m' |> List.map Messaging.log_entry_from_capnp) &&
    leaderCommit = leader_commit_get_int_exn m'
  in 
  let to_msg msg = 
    match API.Reader.ServerMessage.get msg with
    | AppendEntries msg -> msg
    | _ -> assert false
  in 
  Lwt_main.run @@ test_send to_msg send equal

let test_aer term success matchIndex = 
  let term = Int32.to_int term in
  let matchIndex = Int32.to_int matchIndex in
  let open API.Reader.AppendEntriesResp in
  let send mgr id =
    Send.appendEntriesResp mgr id ~term ~success ~matchIndex
  in 
  let equal m' = 
    term = term_get_int_exn m' &&
    success = success_get m' &&
    matchIndex = match_index_get_int_exn m'
  in 
  let to_msg msg = 
    match API.Reader.ServerMessage.get msg with
    | AppendEntriesResp msg -> msg
    | _ -> assert false
  in 
  Lwt_main.run @@ test_send to_msg send equal

let test_cr command = 
  let send mgr id =
    Send.clientRequest mgr id ~command
  in 
  let equal m' = 
    command = command_from_capnp m'
  in 
  let to_msg msg = 
    match API.Reader.ServerMessage.get msg with
    | ClientRequest msg -> msg
    | _ -> assert false
  in 
  Lwt_main.run @@ test_send to_msg send equal

let test_crr result id = 
  let id = Int32.to_int id in
  let open API.Reader.ClientResponse in
  let send mgr sid =
    Send.clientResponse mgr sid ~id ~result
  in 
  let equal m' = 
    result = (result_get m' |> Messaging.result_from_capnp) && 
    id = id_get_int_exn m'
  in 
  let to_msg msg = 
    match API.Reader.ServerMessage.get msg with
    | ClientResponse msg -> msg
    | _ -> assert false
  in 
  Lwt_main.run @@ test_send to_msg send equal

let command = 
  let open Types.StateMachine in
  let open Crowbar in
  let create_cmd op id = 
    let id = Int32.to_int id in
    {op; id} 
  in
  let cmd_gen (k,v) = 
    map 
      [choose 
         [ const @@ Read k
         ; const @@ Write (k,v)]
      ; int32] 
      create_cmd 

  in
  dynamic_bind (pair bytes bytes) cmd_gen

let result_gen = 
  let open Types.StateMachine in
  let open Crowbar in
  let res_gen s = 
      choose
          [ const @@ Success
          ; const @@ ReadSuccess s
          ; const @@ Failure ]
  in
  dynamic_bind (bytes) res_gen


let entry = 
  let open Crowbar in
  let entry_gen command term index = 
    let term = Int32.to_int term in
    let index = Int32.to_int index in
    Types.{command; term; index}
  in map [command; int32; int32] entry_gen

let () =
  Logs.set_level (Some Logs.Debug);
  Logs.set_reporter (Logs_fmt.reporter () )

let () = 
  let t = Int32.of_int 0 in
  test_rv t t
  (*
  let open Crowbar in
  add_test ~name:"Msg RequestVote" [int32; int32] test_rv;
  add_test ~name:"Msg RequestVoteResp" [int32; bool; list entry] test_rvr;
  add_test ~name:"Msg AppendEntries" [int32; int32; int32; list entry; int32] test_ae;
  add_test ~name:"Msg AppendEntriesResp" [int32; bool; int32] test_aer;
  add_test ~name:"Msg ClientRequest" [command] test_cr;
  add_test ~name:"Msg ClientResponse" [result_gen; int32] test_crr
     *)

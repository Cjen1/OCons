open Ocamlpaxos
open Lwt.Infix
open Messaging

let addresses =
  [(1, TCP ("127.0.0.1", 4000)); (2, TCP ("127.0.0.1", 5000))]

let client_addresses = 
  [(1, TCP ("127.0.0.1", 4001)); (2, TCP ("127.0.0.1", 5001))]

let src = Logs.Src.create "test"

module Log = (val Logs.src_log src : Logs.LOG)

let mgr id = ConnManager.create (List.assoc id addresses) (List.assoc id client_addresses) addresses id

let test_send to_msg send equal m1 m2 =
  Log.debug (fun m -> m "Created managers") ;
  send m1 2
  >>= fun () ->
  Log.debug (fun m -> m "Sent msg") ;
  ConnManager.recv m2
  >>= fun (msg', id) ->
  Log.debug (fun m -> m "recv'd msg") ;
  let msg' = to_msg msg' in
  assert (id = 1) ;
  assert (equal msg') ;
  Lwt.return_unit

let eq access m m' = access m = access m'

let test_rv term leader_commit m1 m2 _ () =
  let open API.Reader.RequestVote in
  let send mgr id = Send.requestVote mgr id ~term ~leader_commit ~sym:`AtLeastOnce in
  let equal m' =
    term = term_get_int_exn m' && leader_commit = leader_commit_get_int_exn m'
  in
  let to_msg msg =
    match API.Reader.ServerMessage.get msg with
    | RequestVote msg ->
        msg
    | Undefined i ->
        Log.err (fun m -> m "Got %d" i) ;
        assert false
    | _ ->
        assert false
  in
  test_send to_msg send equal m1 m2

let test_rvr term voteGranted entries m1 m2 _ () =
  let open API.Reader.RequestVoteResp in
  let send mgr id = Send.requestVoteResp mgr id ~term ~voteGranted ~entries ~sym:`AtLeastOnce in
  let equal m' =
    term = term_get_int_exn m'
    && voteGranted = vote_granted_get m'
    && entries = (entries_get_list m' |> List.map Messaging.log_entry_from_capnp)
  in
  let to_msg msg =
    match API.Reader.ServerMessage.get msg with
    | RequestVoteResp msg ->
        msg
    | _ ->
        assert false
  in
  test_send to_msg send equal m1 m2

let test_ae term prevLogIndex prevLogTerm entries leaderCommit m1 m2 _ () =
  let open API.Reader.AppendEntries in
  let send mgr id =
    Send.appendEntries mgr id ~term ~prevLogIndex ~prevLogTerm ~entries
      ~leaderCommit ~sym:`AtLeastOnce
  in
  let equal m' =
    term = term_get_int_exn m'
    && prevLogIndex = prev_log_index_get_int_exn m'
    && prevLogTerm = prev_log_term_get_int_exn m'
    && entries = (entries_get_list m' |> List.map Messaging.log_entry_from_capnp)
    && leaderCommit = leader_commit_get_int_exn m'
  in
  let to_msg msg =
    match API.Reader.ServerMessage.get msg with
    | AppendEntries msg ->
        msg
    | _ ->
        assert false
  in
  test_send to_msg send equal m1 m2

let test_aer term success matchIndex m1 m2 _ () =
  let open API.Reader.AppendEntriesResp in
  let send mgr id = Send.appendEntriesResp mgr id ~term ~success ~matchIndex ~sym:`AtLeastOnce in
  let equal m' =
    term = term_get_int_exn m'
    && success = success_get m'
    && matchIndex = match_index_get_int_exn m'
  in
  let to_msg msg =
    match API.Reader.ServerMessage.get msg with
    | AppendEntriesResp msg ->
        msg
    | _ ->
        assert false
  in
  test_send to_msg send equal m1 m2

let test_cr command m1 m2 _ () =
  let send mgr id = Send.clientRequest mgr id ~command ~sym:`AtLeastOnce in
  let equal m' = command = command_from_capnp m' in
  let to_msg msg =
    match API.Reader.ServerMessage.get msg with
    | ClientRequest msg ->
        msg
    | _ ->
        assert false
  in
  test_send to_msg send equal m1 m2

let test_crr result id m1 m2 _ () =
  let open API.Reader.ClientResponse in
  let send mgr sid = Send.clientResponse mgr sid ~id ~result ~sym:`AtLeastOnce in
  let equal m' =
    result = (result_get m' |> Messaging.result_from_capnp)
    && id = id_get_int_exn m'
  in
  let to_msg msg =
    match API.Reader.ServerMessage.get msg with
    | ClientResponse msg ->
        msg
    | _ ->
        assert false
  in
  test_send to_msg send equal m1 m2

let test_read = Types.StateMachine.{op= Read "asdf"; id= 12381}

let test_write = Types.StateMachine.{op= Write ("asdf", "fsda"); id= 568}

let test_entries =
  let open Types in
  [ {command= test_read; term= 1512; index= 8512}
  ; {command= test_write; term= 684; index= 2461} ]

let () = 
  Logs.set_reporter @@ Logs_fmt.reporter ();
  Logs.set_level (Some Logs.Debug)

let () =
  let open Types.StateMachine in
  Lwt_main.run
    (let m1, m2 = (mgr 1, mgr 2) in
     Alcotest_lwt.run "messaging layer"
       [ ( "all"
         , [ Alcotest_lwt.test_case "Msg RequestVote" `Quick
               (test_rv 123 124512 m1 m2)
           ; Alcotest_lwt.test_case "Msg RequestVoteResp" `Quick
               (test_rvr 012 false test_entries m1 m2)
           ; Alcotest_lwt.test_case "Msg RequestVoteResp" `Quick
               (test_rvr 012 true test_entries m1 m2)
           ; Alcotest_lwt.test_case "Msg AppendEntries" `Quick
               (test_ae 1 182 914 test_entries 123 m1 m2)
           ; Alcotest_lwt.test_case "Msg AppendEntriesResp" `Quick
               (test_aer 912 true 912 m1 m2)
           ; Alcotest_lwt.test_case "Msg AppendEntriesResp" `Quick
               (test_aer 912 false 912 m1 m2)
           ; Alcotest_lwt.test_case "Msg ClientRequest" `Quick
               (test_cr test_read m1 m2)
           ; Alcotest_lwt.test_case "Msg ClientResponse" `Quick
               (test_crr Success 4 m1 m2)
           ; Alcotest_lwt.test_case "Msg ClientResponse" `Quick
               (test_crr Failure 4 m1 m2)
           ; Alcotest_lwt.test_case "Msg ClientResponse" `Quick
               (test_crr (ReadSuccess "asdf") 4 m1 m2) ] ) ]
     >>= fun () -> ConnManager.close m1 >>= fun () -> ConnManager.close m2)

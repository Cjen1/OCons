open Ocamlpaxos
open Lwt.Infix
open Messaging
open Unix_capnp_messaging
open Conn_manager

let addresses = [(1, TCP ("127.0.0.1", 7000))]

let client_addresses =
  [(1, TCP ("127.0.0.1", 7001))]

let src = Logs.Src.create "test"

module Log = (val Logs.src_log src : Logs.LOG)

let mgr id =
  ConnManager.create (List.assoc id addresses)
    (List.assoc id client_addresses)
    addresses id

let test_client send result cmd_eq res_eq m1 =
  let open API.Reader in
  let test () =
    Client.new_client ~cid:1234 [List.assoc 1 client_addresses] ()
    >>= fun client ->
    let client_p = send client in
    ConnManager.recv m1
    >>= fun (msg, cid) ->
    Alcotest.(check int) "client id" cid 1234 ;
    match ServerMessage.get msg with
    | ServerMessage.ClientRequest msg ->
        let command = Messaging.command_from_capnp msg in
        Alcotest.(check bool) "operation equal" (cmd_eq command) true ;
        Send.clientResponse ~sym:`AtLeastOnce m1 cid ~id:command.id ~result
        >>= fun () ->
        client_p
        >>= fun result ->
        Alcotest.(check bool) "result equal" (res_eq result) true ;
        Lwt.return_unit
    | _ ->
        Alcotest.fail "Unrecognised message received"
  in
  test ()

let test_client_write m1 _ () =
  let send client =
    Client.op_write client (Bytes.of_string "asdf") (Bytes.of_string "fsda")
  in
  let result = Types.StateMachine.Failure in
  let msg_eq command =
    command.Types.StateMachine.op = Types.StateMachine.Write ("asdf", "fsda")
  in
  let res_eq result' =
    match result' with
    | Error (`Msg "Application failed on cluster") ->
        true
    | _ ->
        false
  in
  test_client send result msg_eq res_eq m1

let test_client_read m1 _ () =
  let send client =
    Client.op_read client (Bytes.of_string "asdf")
  in
  let result = Types.StateMachine.Failure in
  let msg_eq command =
    command.Types.StateMachine.op = Types.StateMachine.Read "asdf"
  in
  let res_eq result' =
    match result' with
    | Error (`Msg "Application failed on cluster") ->
        true
    | _ ->
        false
  in
  test_client send result msg_eq res_eq m1

let test_client_success m1 _ () =
  let send client =
    Client.op_read client (Bytes.of_string "asdf")
  in
  let result = Types.StateMachine.Success in
  let msg_eq command =
    command.Types.StateMachine.op = Types.StateMachine.Read "asdf"
  in
  let res_eq result' =
    match result' with
    | Ok `Success -> true
    | _ ->
        false
  in
  test_client send result msg_eq res_eq m1

let test_client_read_success m1 _ () =
  let send client =
    Client.op_read client (Bytes.of_string "asdf")
  in
  let result = Types.StateMachine.ReadSuccess "fdsa" in
  let msg_eq command =
    command.Types.StateMachine.op = Types.StateMachine.Read "asdf"
  in
  let res_eq result' =
    match result' with
    | Ok (`ReadSuccess "fdsa")-> true
    | _ ->
        false
  in
  test_client send result msg_eq res_eq m1

let reporter =
  let report src level ~over k msgf =
    let k _ = over () ; k () in
    let src = Logs.Src.name src in
    msgf
    @@ fun ?header ?tags:_ fmt ->
    Fmt.kpf k Fmt.stdout
      ("[%a] %a %a @[" ^^ fmt ^^ "@]@.")
      Core.Time.pp (Core.Time.now ())
      Fmt.(styled `Magenta string)
      (Printf.sprintf "%14s" src)
      Logs_fmt.pp_header (level, header)
  in
  {Logs.report}

let () =
  Logs.(set_level(Some Debug));
  Logs.set_reporter reporter;
  Lwt_main.run 
    (let m1 = mgr 1 in
     Alcotest_lwt.run "client_server"
       [ ( "all"
         , [ Alcotest_lwt.test_case "client write" `Quick (test_client_write m1)
           ; Alcotest_lwt.test_case "client read" `Quick (test_client_read m1)
           ; Alcotest_lwt.test_case "client success" `Quick (test_client_success m1)
           ; Alcotest_lwt.test_case "client read_success" `Quick (test_client_read_success m1)
           ] ) ]
     >>= fun () ->
     ConnManager.close m1
    )

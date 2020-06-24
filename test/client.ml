open Ocamlpaxos
open Types
open Lwt.Infix
open Messaging
open Unix_capnp_messaging
open Conn_manager

let src = Logs.Src.create "test"

module Log = (val Logs.src_log src : Logs.LOG)

let addr i = TCP ("127.0.0.1", 5000 + i)

let mgr i handler =
  create ~listen_address:(addr i) ~node_id:(Int64.of_int i) handler

let stream_mgr i =
  let stream, push = Lwt_stream.create () in
  (mgr i (fun _ src msg -> push (Some (src, msg)) |> Lwt.return_ok), stream)

let timeout t f s =
  let p = f () >>= Lwt.return_ok in
  let t = Lwt_unix.sleep t >>= Lwt.return_error in
  Lwt.choose [ p; t ] >>= function
  | Ok v -> Lwt.return v
  | Error () -> Alcotest.fail s

let test_bytes = Bytes.of_string "asdf"

let send_read_req client = Client.op_read client test_bytes

let send_res res server cid cmdid = 
  Send.clientResponse ~sem:`AtLeastOnce ~id:cmdid ~result:res server cid

let res_eq_failure msg = 
  let test = function
    | Error (`Msg "Application failed on cluster") -> true
    | _ -> false
  in Alcotest.(check bool) "Result_string" true (test msg)

let failif t = match t with 
  | Ok v -> Lwt.return v
  | Error exn -> raise exn

open API.Reader

let test_client_loop ?(send=send_read_req) ?(send_res=send_res StateMachine.Failure) ?(res_eq=res_eq_failure) () = 
  let main = 
    let server, str1 = stream_mgr 1 in
    let cid = Random.int64 Int64.(Int64.add (of_int 1000) (of_int 1000)) in
    Client.new_client ~cid:cid [Int64.of_int 1,addr 1] () >>= fun client ->
    let p_c = send client in
    Lwt_stream.next str1 >>= fun (src,msg) -> 
    Alcotest.(check int64) "Client id reporting" src cid;
    match ServerMessage.(msg |> Capnp.BytesMessage.Message.readonly |> of_message |> get) with
    | ServerMessage.ClientRequest cmd ->
      send_res server cid (Command.id_get cmd) >>>= fun () ->
      p_c >|= res_eq >>= fun () ->
      Conn_manager.close server >>= fun () ->
      Client.close client >>=
      Lwt.return_ok
    | _ -> Alcotest.fail "Did not receive a client_request"
  in main >>= failif

let test_client_read () = 
  test_client_loop ()

let test_client_write () =
  let send client = Client.op_write client test_bytes test_bytes in
  test_client_loop ~send ()

let test_client_success () = 
  let res = StateMachine.Success in
  let res_eq a = 
    let res = match a with
      | Ok `Success -> true
      | _ -> false
    in Alcotest.(check bool) "Result success" true res
  in test_client_loop ~send_res:(send_res res) ~res_eq ()

let test_client_read_success () = 
  let res = StateMachine.ReadSuccess "asdf" in
  let res_eq a = 
    let res = match a with
      | Ok (`ReadSuccess "asdf") -> true
      | _ -> false
    in Alcotest.(check bool) "Result success" true res
  in test_client_loop ~send_res:(send_res res) ~res_eq ()

let test_internal () =
  let main = 
    let server, str1 = stream_mgr 1 in
    let cid = Random.int64 Int64.(Int64.add (of_int 1000) (of_int 1000)) in
    Client.new_client ~cid:cid [Int64.of_int 1,addr 1] () >>= fun client ->
    let p_c = Client.op_read client test_bytes in
    Lwt_stream.next str1 >>= fun (_src, msg) ->
    match ServerMessage.(msg |> Capnp.BytesMessage.Message.readonly |> of_message |> get) with
    | ServerMessage.ClientRequest cmd ->
      let test = Hashtbl.mem client.Client.ongoing_requests (Command.id_get cmd) in 
      Alcotest.(check bool) "Client request in resolvers" true test;
      Send.clientResponse ~sem:`AtLeastOnce ~id:(Command.id_get cmd) ~result:StateMachine.Failure server cid >>>= fun () ->
      p_c >>= fun _ ->
      let test = Hashtbl.mem client.Client.ongoing_requests (Command.id_get cmd) in
      Alcotest.(check bool) "Client response removed request from resolvers" false test;
      Conn_manager.close server >>= fun () ->
      Client.close client >>=
      Lwt.return_ok
    | _ -> Alcotest.fail "Did not receive a client_request"
  in main >>= failif

let test_wrapper f _ () = timeout 5. f "Timed out"

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
    (Alcotest_lwt.run "client library"
       [ ( "all"
         , [ 
             Alcotest_lwt.test_case "read" `Quick (test_wrapper test_client_read)
           ; Alcotest_lwt.test_case "write" `Quick (test_wrapper test_client_write)
           ; Alcotest_lwt.test_case "success" `Quick (test_wrapper test_client_success)
           ; Alcotest_lwt.test_case "read_success" `Quick (test_wrapper test_client_read_success)
           ; Alcotest_lwt.test_case "internals" `Quick (test_wrapper test_internal)
           ] ) ]
    )

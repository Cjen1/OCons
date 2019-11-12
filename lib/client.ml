open Base
open State_machine

type t =
  { cid: string
  ; context: Zmq.Context.t
  ; pub_socket: [`Pub] Zmq_lwt.Socket.t
  ; deal_socket: [`Dealer] Zmq_lwt.Socket.t
  ; in_flight: (string, string Lwt.u) Hashtbl.t
  ; fulfilled: string Hash_set.t }

let serialise_request op =
  let cid = Types.create_id () in
  let cmd : Messaging.client_request = {command= {op; id= cid}} in
  cmd
  |> Protobuf.Encoder.encode_exn Messaging.client_request_to_protobuf
  |> Bytes.to_string

let deserialise_response response : State_machine.StateMachine.op_result =
  ( response |> Bytes.of_string
  |> Protobuf.Decoder.decode_exn Messaging.client_response_from_protobuf )
    .result

let print_endline = Stdio.print_endline

let send t op =
  let msg = serialise_request op in
  let finished, fulfiller = Lwt.task () in
  let rid = Types.create_id () in
  let () = Hashtbl.set t.in_flight ~key:rid ~data:fulfiller in
  let%lwt resp =
    Msg_layer.retry ~finished ~timeout:5. (fun () ->
        Lwt.async (fun () ->
            try Zmq_lwt.Socket.send_all t.pub_socket [t.cid; rid; msg]
            with Zmq.ZMQ_exception (_, s) ->
              print_endline s ; Lwt.return_unit) ;
        print_endline "Trying to connect")
  in
  Lwt.return @@ deserialise_response resp

let op_read t k =
  let op = StateMachine.Read k in
  send t op

let op_write t k v =
  let op = StateMachine.Write (k, v) in
  send t op

let recv_loop t =
  print_endline "Recv_loop: spooling up" ;
  let open Zmq_lwt in
  let rec recv_loop () =
    let%lwt rid, msg =
      print_endline "Recv_loop: awaiting msg" ;
      let%lwt resp = Socket.recv_all t.deal_socket in
      match resp with
      | [rid; msg] ->
          print_endline "received msg" ;
          Lwt.return @@ (rid, msg)
      | _ ->
          print_endline "Not right msg format" ;
          assert false
    in
    ( match Hashtbl.find t.in_flight rid with
    | Some fulfiller ->
        Lwt.wakeup_later fulfiller msg ;
        Hashtbl.remove t.in_flight rid;
        Hash_set.add t.fulfilled rid
    | None ->
      print_endline @@ Printf.sprintf "Already fulfilled = %b" (Hash_set.mem t.fulfilled rid);
        () ) ;
    recv_loop ()
  in
  recv_loop ()

let new_client ?(cid = Types.create_id ()) ~req_endpoints ~rep_endpoints () =
  let context = Zmq.Context.create () in
  let pub_socket = Zmq.Socket.create context Zmq.Socket.pub in
  let deal_socket = Zmq.Socket.create context Zmq.Socket.dealer in
  let () = Zmq.Socket.set_identity deal_socket cid in
  let () =
    List.iter req_endpoints ~f:(fun endpoint ->
        let addr = "tcp://" ^ endpoint in
        Zmq.Socket.connect pub_socket addr)
  in
  let () =
    List.iter rep_endpoints ~f:(fun endpoint ->
        let addr = "tcp://" ^ endpoint in
        Zmq.Socket.connect deal_socket addr)
  in
  let pub_socket = Zmq_lwt.Socket.of_socket pub_socket in
  let deal_socket = Zmq_lwt.Socket.of_socket deal_socket in
  let t =
    { cid
    ; context
    ; pub_socket
    ; deal_socket
    ; in_flight= Hashtbl.create (module String)
    ; fulfilled= Hash_set.create (module String) }
  in
  (t, recv_loop t)

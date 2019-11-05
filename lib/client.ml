open Base

type t = {cid: string; context: Zmq.Context.t; pub_socket: [`Pub] Zmq_lwt.Socket.t; sub_socket: [`Sub] Zmq_lwt.Socket.t}

let new_client ~sub_endpoints ~pub_endpoints =
  let cid = Types.create_id () in
  let context = Zmq.Context.create () in
  let pub_socket = Zmq.Socket.create context Zmq.Socket.pub in
  let sub_socket = Zmq.Socket.create context Zmq.Socket.sub in
  let () =
    List.iter sub_endpoints ~f:(fun endpoint ->
        let addr = "tcp://" ^ endpoint in
        Zmq.Socket.connect sub_socket addr)
  in
  let () =
    List.iter pub_endpoints ~f:(fun endpoint ->
        let addr = "tcp://" ^ endpoint in
        Zmq.Socket.connect pub_socket addr)
  in
  Zmq.Socket.subscribe sub_socket cid;
  let pub_socket = Zmq_lwt.Socket.of_socket pub_socket in
  let sub_socket = Zmq_lwt.Socket.of_socket sub_socket in
  {cid;context; pub_socket; sub_socket}

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

open State_machine

let print_endline = Stdio.print_endline

let send t op =
  let msg = serialise_request op in
  let finished, fulfiller = Lwt.task() in
  let%lwt resp = Msg_layer.retry ~finished ~timeout:10. (fun () -> 
      print_endline "Trying to connect" ;
      Lwt.async (fun () -> 
          try
            let%lwt () = Zmq_lwt.Socket.send_all t.pub_socket [t.cid; msg] in
            let%lwt resp = Zmq_lwt.Socket.recv t.sub_socket in
            Lwt.wakeup fulfiller resp;
            Lwt.return_unit
          with Zmq.ZMQ_exception (_, s) -> (print_endline s; Lwt.return_unit)
        )
    )
  in Lwt.return @@ deserialise_response resp

let op_read_lwt t k =
  let op = StateMachine.Read k in
  send t op

let op_read t k = Lwt_main.run @@ op_read_lwt t k

let op_write_lwt t k v =
  let op = StateMachine.Write (k, v) in
  send t op

let op_write t k v = Lwt_main.run @@ op_write_lwt t k v

open Base

type t = {context: Zmq.Context.t; socket: [`Req] Zmq_lwt.Socket.t}

let new_client endpoints =
  let context = Zmq.Context.create () in
  let socket = Zmq.Socket.create context Zmq.Socket.req in
  let () =
    List.iter endpoints ~f:(fun endpoint ->
        let addr = "tcp://" ^ endpoint in
        Zmq.Socket.connect socket addr)
  in
  let socket = Zmq_lwt.Socket.of_socket socket in
  {context; socket}

let serialise_request op =
  let cid = Types.create_id () in
  let cmd : Messaging.client_request = {command= {op; id= cid}} in
  cmd
  |> Protobuf.Encoder.encode_exn Messaging.client_request_to_protobuf
  |> Bytes.to_string

let deserialise_response response =
  ( response |> Bytes.of_string
  |> Protobuf.Decoder.decode_exn Messaging.client_response_from_protobuf )
    .result

open State_machine

let print_endline = Stdio.print_endline

let send t op =
  let msg = serialise_request op in
  let p =
    let rec loop () =
      try
        print_endline "Trying to connect" ;
        let%lwt () = Zmq_lwt.Socket.send t.socket msg in
        let%lwt resp = Zmq_lwt.Socket.recv t.socket in
        match deserialise_response resp with
        | Some v ->
            Lwt.return v
        | None ->
          let%lwt () = Lwt_unix.sleep 0.01 in
            loop ()
      with Zmq.ZMQ_exception (_, s) -> print_endline s ; loop ()
    in
    loop ()
  in
  p

let op_read_lwt t k =
  let op = StateMachine.Read k in
  send t op

let op_read t k = Lwt_main.run @@ op_read_lwt t k

let op_write_lwt t k v =
  let op = StateMachine.Write (k, v) in
  send t op

let op_write t k v = Lwt_main.run @@ op_write_lwt t k v

open Types

let msg = Logs.Src.create "Msg" ~doc:"Messaging module"

module MLog = (val Logs.src_log msg : Logs.LOG)

type state_machine_command = StateMachine.command [@@deriving protobuf]

type client_request = {command: state_machine_command [@key 1]}
[@@deriving protobuf]

type request_vote_request =
  {term: term [@key 1]; leaderCommit: log_index [@key 2]}
[@@deriving protobuf]

type request_vote_response =
  { term: term [@key 1]
  ; voteGranted: bool [@key 2]
  ; entries: log_entry list [@key 3] }
[@@deriving protobuf]

type append_entries_request =
  { term: term [@key 1]
  ; prevLogIndex: log_index [@key 2]
  ; prevLogTerm: term [@key 3]
  ; entries: log_entry list [@key 4]
  ; leaderCommit: log_index [@key 5] }
[@@deriving protobuf]

type append_entries_response = {term: term [@key 1]; success: bool [@key 2]; 
                                (* match index required since leader does not know what was sent to server *)
                                matchIndex: log_index [@key 3]}
[@@deriving protobuf]

type client_response = {result: StateMachine.op_result [@key 1]}
[@@deriving protobuf]

type 'a msg_typ =
  | CRq : client_request msg_typ
  | RVRq : request_vote_request msg_typ
  | RVRp : request_vote_response msg_typ
  | AERq : append_entries_request msg_typ
  | AERp : append_entries_response msg_typ
  | CRp : client_response msg_typ

type 'a msg_filter = {typ: 'a msg_typ; filter: string}

let client_request =
  {typ= (CRq : client_request msg_typ); filter= "client_request"}

let request_vote_request =
  {typ= (RVRq : request_vote_request msg_typ); filter= "request_vote_request"}

let request_vote_response =
  {typ= (RVRp : request_vote_response msg_typ); filter= "request_vote_response"}

let append_entries_request =
  { typ= (AERq : append_entries_request msg_typ)
  ; filter= "append_entries_request" }

let append_entries_response =
  { typ= (AERp : append_entries_response msg_typ)
  ; filter= "append_entries_response" }

let client_response =
  {typ= (CRp : client_response msg_typ); filter= "client_response"}

let client_request =
  {typ= (CRq : client_request msg_typ); filter= "client_request"}

let to_bytes : type a. a msg_filter -> a -> bytes =
 fun filter msg ->
  match filter.typ with
  | CRq ->
      Protobuf.Encoder.encode_exn client_request_to_protobuf msg
  | RVRq ->
      Protobuf.Encoder.encode_exn request_vote_request_to_protobuf msg
  | RVRp ->
      Protobuf.Encoder.encode_exn request_vote_response_to_protobuf msg
  | AERq ->
      Protobuf.Encoder.encode_exn append_entries_request_to_protobuf msg
  | AERp ->
      Protobuf.Encoder.encode_exn append_entries_response_to_protobuf msg
  | CRp ->
      Protobuf.Encoder.encode_exn client_response_to_protobuf msg

let to_string filter msg = to_bytes filter msg |> Bytes.to_string

let from_bytes : type a. a msg_filter -> bytes -> a =
 fun filter msg ->
  match filter.typ with
  | CRq ->
      Protobuf.Decoder.decode_exn client_request_from_protobuf msg
  | RVRq ->
      Protobuf.Decoder.decode_exn request_vote_request_from_protobuf msg
  | RVRp ->
      Protobuf.Decoder.decode_exn request_vote_response_from_protobuf msg
  | AERq ->
      Protobuf.Decoder.decode_exn append_entries_request_from_protobuf msg
  | AERp ->
      Protobuf.Decoder.decode_exn append_entries_response_from_protobuf msg
  | CRp ->
      Protobuf.Decoder.decode_exn client_response_from_protobuf msg

let from_string filter msg = msg |> Bytes.of_string |> from_bytes filter

let send_router ~(sock : [`Router] Zmq_lwt.Socket.t) ~dest ~msg =
  MLog.debug (fun m -> m "Sending to %s" dest) ;
  Zmq_lwt.Socket.send_all sock (dest :: msg)

exception Invalid_response of string * string list

let string_of_resp xs =
  let rec string_of_elts = function
    | [] ->
        ""
    | [b] ->
        Printf.sprintf "%s" b
    | x :: xs ->
        Printf.sprintf "%s;%s" x (string_of_elts xs)
  in
  Printf.sprintf "[%s]" (string_of_elts xs)

let send_client_req ~dest ~rid ~msg =
  let msg = to_string client_request msg in
  Zmq_lwt.Socket.send_all dest [rid; msg]

let recv_client_req ~sock =
  let%lwt resp = Zmq_lwt.Socket.recv_all sock in
  ( match resp with
  | [addr; rid; msg] ->
      let msg = from_string client_request msg in
      Ok (addr, rid, msg)
  | _ ->
      MLog.debug (fun m -> m "Invalid msg received %s" (string_of_resp resp)) ;
      Error (Invalid_response ("client_request", resp)) )
  |> Lwt.return

let send_client_rep ~dest ~rid ~msg ~sock =
  let msg = to_string client_response msg in
  send_router ~sock ~dest ~msg:[rid; msg]

let recv_client_rep ~sock =
  let%lwt resp = Zmq_lwt.Socket.recv_all sock in
  match resp with
  | [rid; msg] ->
      let msg = from_string client_response msg in
      Lwt.return @@ Ok (rid, msg)
  | _ ->
      MLog.debug (fun m -> m "Invalid msg received") ;
      Lwt.return @@ Error (Invalid_response ("client_response", resp))

let send_inc ~sock ~dest ~filter ~src ~msg =
  send_router ~sock ~dest ~msg:[filter; src; msg]

let recv_inc ~sock =
  let%lwt resp = Zmq_lwt.Socket.recv_all sock in
  match resp with
  | [_src; filter; src; msg] ->
      Lwt.return @@ Ok (filter, src, msg)
  | _ ->
      MLog.debug (fun m -> m "Invalid msg received") ;
      Lwt.return @@ Error (Invalid_response ("Inter node", resp))

open Types
open State_machine

let msg = Logs.Src.create "Msg" ~doc:"Messaging module"

module MLog = (val Logs.src_log msg : Logs.LOG)

type client_request = {command: StateMachine.command [@key 1]}
[@@deriving protobuf]

type replica_request =
  {slot_num: Types.slot_number [@key 1]; command: StateMachine.command [@key 2]}
[@@deriving protobuf]

type p1a = {ballot: Ballot.t [@key 1] (*; high_slot : slotber [@key 2]*)}
[@@deriving protobuf]

type p1b =
  { ballot: Ballot.t [@key 1]
  ; accepted: Pval.t list [@key 2]
  ; id: string [@key 3] }
[@@deriving protobuf]

type p2a = {pval: Pval.t [@key 1]} [@@deriving protobuf]

type p2b =
  {id: unique_id [@key 1]; ballot: Ballot.t [@key 2]; pval: Pval.t [@key 3]}
[@@deriving protobuf]

type nack_p1 = {ballot: Ballot.t [@key 1]} [@@deriving protobuf]

type nack_p2 = {ballot: Ballot.t [@key 1]} [@@deriving protobuf]

type decision_response =
  {slot: Types.slot_number [@key 1]; command: StateMachine.command [@key 2]}
[@@deriving protobuf]

type client_response = {result: StateMachine.op_result [@key 1]}
[@@deriving protobuf]

type 'a msg_typ =
  | CRq : client_request msg_typ
  | RRq : replica_request msg_typ
  | P1a : p1a msg_typ
  | P1b : p1b msg_typ
  | P2a : p2a msg_typ
  | P2b : p2b msg_typ
  | Np1 : nack_p1 msg_typ
  | Np2 : nack_p2 msg_typ
  | DRp : decision_response msg_typ
  | CRp : client_response msg_typ

type 'a msg_filter = {typ: 'a msg_typ; filter: string}

let client_request =
  {typ= (CRq : client_request msg_typ); filter= "client_request"}

let replica_request =
  {typ= (RRq : replica_request msg_typ); filter= "replica_request"}

let p1a = {typ= (P1a : p1a msg_typ); filter= "p1a"}

let p1b = {typ= (P1b : p1b msg_typ); filter= "p1b"}

let p2a = {typ= (P2a : p2a msg_typ); filter= "p2a"}

let p2b = {typ= (P2b : p2b msg_typ); filter= "p2b"}

let nack_p1 = {typ= (Np1 : nack_p1 msg_typ); filter= "nack_p1"}

let nack_p2 = {typ= (Np2 : nack_p2 msg_typ); filter= "nack_p2"}

let decision_response =
  {typ= (DRp : decision_response msg_typ); filter= "decision_response"}

let client_response =
  {typ= (CRp : client_response msg_typ); filter= "client_response"}

let to_string : type a. a msg_typ -> a -> string =
 fun typ msg ->
  let bytes =
    match typ with
    | CRq ->
        Protobuf.Encoder.encode_exn client_request_to_protobuf msg
    | RRq ->
        Protobuf.Encoder.encode_exn replica_request_to_protobuf msg
    | P1a ->
        Protobuf.Encoder.encode_exn p1a_to_protobuf msg
    | P1b ->
        Protobuf.Encoder.encode_exn p1b_to_protobuf msg
    | P2a ->
        Protobuf.Encoder.encode_exn p2a_to_protobuf msg
    | P2b ->
        Protobuf.Encoder.encode_exn p2b_to_protobuf msg
    | Np1 ->
        Protobuf.Encoder.encode_exn nack_p1_to_protobuf msg
    | Np2 ->
        Protobuf.Encoder.encode_exn nack_p2_to_protobuf msg
    | DRp ->
        Protobuf.Encoder.encode_exn decision_response_to_protobuf msg
    | CRp ->
        Protobuf.Encoder.encode_exn client_response_to_protobuf msg
  in
  Bytes.to_string bytes

let from_bytes : type a. a msg_typ -> bytes -> a = function
  | CRq ->
      fun msg -> Protobuf.Decoder.decode_exn client_request_from_protobuf msg
  | RRq ->
      fun msg -> Protobuf.Decoder.decode_exn replica_request_from_protobuf msg
  | P1a ->
      fun msg -> Protobuf.Decoder.decode_exn p1a_from_protobuf msg
  | P1b ->
      fun msg -> Protobuf.Decoder.decode_exn p1b_from_protobuf msg
  | P2a ->
      fun msg -> Protobuf.Decoder.decode_exn p2a_from_protobuf msg
  | P2b ->
      fun msg -> Protobuf.Decoder.decode_exn p2b_from_protobuf msg
  | Np1 ->
      fun msg -> Protobuf.Decoder.decode_exn nack_p1_from_protobuf msg
  | Np2 ->
      fun msg -> Protobuf.Decoder.decode_exn nack_p2_from_protobuf msg
  | DRp ->
      fun msg -> Protobuf.Decoder.decode_exn decision_response_from_protobuf msg
  | CRp ->
      fun msg -> Protobuf.Decoder.decode_exn client_response_from_protobuf msg

let from_string typ msg = from_bytes typ (Bytes.of_string msg)

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
  let msg = to_string CRq msg in
  Zmq_lwt.Socket.send_all dest [rid; msg]

let recv_client_req ~sock =
  let%lwt resp = Zmq_lwt.Socket.recv_all sock in
  ( match resp with
  | [addr; rid; msg] ->
      let msg = from_string CRq msg in
      Ok (addr, rid, msg)
  | _ ->
      MLog.debug (fun m -> m "Invalid msg received %s" (string_of_resp resp)) ;
      Error (Invalid_response ("client_request", resp)) )
  |> Lwt.return

let send_client_rep ~dest ~rid ~msg ~sock =
  let msg = to_string CRp msg in
  send_router ~sock ~dest ~msg:[rid; msg]

let recv_client_rep ~sock =
  let%lwt resp = Zmq_lwt.Socket.recv_all sock in
  match resp with
  | [rid; msg] ->
      let msg = from_string CRp msg in
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

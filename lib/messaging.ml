open Types
open State_machine

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

(*
type msg = 
  | CRq of client_request       [@key 1] 
  | RRq of replica_request      [@key 2]
  | P1a of p1a                  [@key 3]
  | P1b of p1b                  [@key 4]
  | P2a of p2a                  [@key 5]
  | P2b of p2b                  [@key 6]
  | Np1 of nack_p1              [@key 7]
  | Np2 of nack_p2              [@key 8]
  | DRp of decision_response    [@key 9]
  | CRp of client_response      [@key 10]
[@@deriving protobuf]
   *)


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

let to_string :
  type a . a msg_typ -> a -> string 
  = fun typ msg -> let bytes = 
    match typ with
    | CRq -> Protobuf.Encoder.encode_exn client_request_to_protobuf msg
    | RRq -> Protobuf.Encoder.encode_exn replica_request_to_protobuf msg
    | P1a -> Protobuf.Encoder.encode_exn p1a_to_protobuf msg
    | P1b -> Protobuf.Encoder.encode_exn p1b_to_protobuf msg
    | P2a -> Protobuf.Encoder.encode_exn p2a_to_protobuf msg
    | P2b -> Protobuf.Encoder.encode_exn p2b_to_protobuf msg
    | Np1 -> Protobuf.Encoder.encode_exn nack_p1_to_protobuf msg
    | Np2 -> Protobuf.Encoder.encode_exn nack_p2_to_protobuf msg
    | DRp -> Protobuf.Encoder.encode_exn decision_response_to_protobuf msg
    | CRp -> Protobuf.Encoder.encode_exn client_response_to_protobuf msg
    in Bytes.to_string bytes

let from_bytes :
  type a . a msg_typ -> bytes -> a
  = function 
    | CRq -> (fun msg -> Protobuf.Decoder.decode_exn client_request_from_protobuf msg)
    | RRq -> (fun msg -> Protobuf.Decoder.decode_exn replica_request_from_protobuf msg)
    | P1a -> (fun msg -> Protobuf.Decoder.decode_exn p1a_from_protobuf msg)
    | P1b -> (fun msg -> Protobuf.Decoder.decode_exn p1b_from_protobuf msg)
    | P2a -> (fun msg -> Protobuf.Decoder.decode_exn p2a_from_protobuf msg)
    | P2b -> (fun msg -> Protobuf.Decoder.decode_exn p2b_from_protobuf msg)
    | Np1 -> (fun msg -> Protobuf.Decoder.decode_exn nack_p1_from_protobuf msg)
    | Np2 -> (fun msg -> Protobuf.Decoder.decode_exn nack_p2_from_protobuf msg)
    | DRp -> (fun msg -> Protobuf.Decoder.decode_exn decision_response_from_protobuf msg)
    | CRp -> (fun msg -> Protobuf.Decoder.decode_exn client_response_from_protobuf msg)

let from_string typ msg = from_bytes typ (Bytes.of_string msg)

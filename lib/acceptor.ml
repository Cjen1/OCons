(* acceptor.ml *)

open Utils
open Messaging

(* Types of acceptors *)
type t =
  { id: Types.unique_id
  ; mutable ballot_num: Ballot.t
  ; accepted: (Types.slot_number, Pval.t) Base.Hashtbl.t
  ; wal: Unix.file_descr
  ; msg: Msg_layer.t }

let p1 t msg =
  let p1a =
    Bytes.of_string msg |> Protobuf.Decoder.decode_exn p1a_from_protobuf
  in
  if Ballot.less_than t.ballot_num p1a.ballot then (
    p1a.ballot |> Ballot.to_string |> write_to_wal t.wal ;
    t.ballot_num <- p1a.ballot ) ;
  { ballot= t.ballot_num
  ; accepted=
      Base.Hashtbl.data t.accepted
      (* TODO reduce this? Can be done by including a high water mark in p1a message decisions *)
  }
  |> Protobuf.Encoder.encode_exn p1b_to_protobuf
  |> Bytes.to_string
  |> Msg_layer.send_msg t.msg ~filter:"p1b"

let p2 t msg =
  let p2a =
    Bytes.of_string msg |> Protobuf.Decoder.decode_exn p2a_from_protobuf
  in
  let ((ib, is, _) as ipval) = p2a.pval in
  let ( >= ) a b = not @@ Ballot.less_than a b in
  if ib >= t.ballot_num then (
    write_to_wal t.wal @@ Pval.to_string ipval ;
    Base.Hashtbl.set t.accepted ~key:is ~data:ipval ;
    {acceptor_id= t.id; ballot= t.ballot_num}
    |> Protobuf.Encoder.encode_exn p2b_to_protobuf
    |> Bytes.to_string
    |> Msg_layer.send_msg t.msg ~filter:"p2b" )

let create wal_loc msg = 
  { id= Types.create_id ()
  ; ballot_num= Ballot.bottom ()
  ; accepted= Base.Hashtbl.create (module Base.Int)
  ; wal=
         Unix.openfile wal_loc [Unix.O_RDWR; Unix.O_CREAT]
         @@ int_of_string "0x660"
  ; msg = msg}

(* Initialize a new acceptor *)
let create_independent wal_loc nodes local =
  let msg=Msg_layer.create nodes local in
  create wal_loc msg

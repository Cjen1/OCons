(* acceptor.ml *)

open Utils
open Messaging

let acceptor = Logs.Src.create "Acceptor" ~doc:"Acceptor module"

module ALog = (val Logs.src_log acceptor : Logs.LOG)

(* Types of acceptors *)
type t =
  { id: string
  ; mutable ballot_num: Ballot.t
  ; accepted: (Types.slot_number, Pval.t) Base.Hashtbl.t
  ; wal: Unix.file_descr
  ; msg_layer: Msg_layer.t }

let p1a_callback t (p1a : p1a) =
  let open Ballot.Infix in
  ALog.debug (fun m ->
      m "Got p1a msg, ballot=%s" @@ Ballot.to_string p1a.ballot) ;
  if p1a.ballot > t.ballot_num then (
    p1a.ballot |> Ballot.to_string |> write_to_wal t.wal ;
    t.ballot_num <- p1a.ballot ;
    ALog.debug (fun m ->
        m "Sending p1b, ballot=%s" @@ Ballot.to_string t.ballot_num) ;
    { id= t.id
    ; ballot= t.ballot_num
    ; accepted=
        Base.Hashtbl.data t.accepted
        (* TODO reduce this? Can be done by including a high water mark in p1a message decisions *)
    }
    |> Msg_layer.send_msg t.msg_layer ~msg_filter:p1b )
  else
    {ballot= t.ballot_num}
    |> Msg_layer.send_msg t.msg_layer ~msg_filter:nack_p1 ;
  Lwt.return_unit

let p2a_callback t (p2a : p2a) =
  let open Ballot.Infix in
  ALog.debug (fun m -> m "Got p2a msg") ;
  let ((ib, is, _) as ipval) = p2a.pval in
  if ib >= t.ballot_num then (
    write_to_wal t.wal @@ Pval.to_string ipval ;
    Base.Hashtbl.set t.accepted ~key:is ~data:ipval ;
    ALog.debug (fun m -> m "Sending p2b msg") ;
    {id= t.id; ballot= t.ballot_num; pval= ipval}
    |> Msg_layer.send_msg t.msg_layer ~msg_filter:p2b )
  else (
    ALog.debug (fun m -> m "Is incorrect ballot") ;
    {ballot= t.ballot_num}
    |> Msg_layer.send_msg t.msg_layer ~msg_filter:nack_p2 ) ;
  Lwt.return_unit

let create ~wal_loc ~msg_layer ~id =
  let t =
    { id
    ; ballot_num= Ballot.bottom ()
    ; accepted= Base.Hashtbl.create (module Base.Int)
    ; wal=
        Unix.openfile wal_loc [Unix.O_RDWR; Unix.O_CREAT]
        @@ int_of_string "0x666"
    ; msg_layer }
  in
  Msg_layer.attach_watch msg_layer ~msg_filter:Messaging.p1a
    ~callback:(p1a_callback t) ;
  Msg_layer.attach_watch msg_layer ~msg_filter:Messaging.p2a
    ~callback:(p2a_callback t) ;
  (t, Lwt.return_unit)

(* Initialize a new acceptor *)
let create_independent ~wal_loc ~node_list ~id ~alive_timeout =
  let%lwt msg_layer, psml = Msg_layer.create ~node_list:node_list ~id ~alive_timeout in
  let t, psa = create ~wal_loc ~msg_layer ~id in
  Lwt.return (t,Lwt.join [psa; psml])

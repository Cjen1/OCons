open Types
open Utils
open Base
open Messaging

(* TODO implement *)
type state

type 'a fulfilable_command = 'a Lwt_condition.t * command

let to_command : 'a fulfilable_command -> command = fun (_, c) -> c

type leader_msg =
  | Propose of slot_number * unit fulfilable_command
  | Adopted of Ballot.t * Pval.t list (* This is probably just a single element *)
  | Preempted of Ballot.t

type t =
  { id: leader_id
  ; acceptor_uris_p1: Lwt_unix.sockaddr list
  ; acceptor_uris_p2: Lwt_unix.sockaddr list
  ; replica_uris: Lwt_unix.sockaddr list
  ; mutable ballot_num: Ballot.t
  ; mutable is_leader: bool
  ; decided_log: (Types.slot_number, Pval.t) Base.Hashtbl.t
  ; mutable slot_out: int
  ; mutable current_proposals:
      (Types.slot_number, unit fulfilable_command) Base.Hashtbl.t
  ; mutable leader_queue: leader_msg Utils.Queue.t
  ; mutable timeout_s: float
  ; aimd_ai: float
  ; aimd_md: float }

let aimd_ai t = t.timeout_s <- t.timeout_s -. t.aimd_ai

let aimd_md t = t.timeout_s <- t.timeout_s *. t.aimd_md

exception ExnPreempted of Ballot.t

let achieve_quorum acceptor_uris promises =
  let rec wait_until_quorum q rem_list =
    if Quorum.is_majority q then Lwt.return_unit
    else
      let* found, rem = Lwt.nchoose_split rem_list in
      let q =
        List.fold found ~init:q ~f:(fun q (_, uri) -> Quorum.add q uri)
      in
      wait_until_quorum q rem
  in
  let* () = wait_until_quorum (List.length acceptor_uris, []) promises in
  let* resps, _ = Lwt.nchoose_split promises in
  Lwt.return @@ List.map resps ~f:fst

let rec advance_minimum_slot t =
  match Hashtbl.find t.decided_log t.slot_out with
  | Some _ ->
      t.slot_out <- t.slot_out + 1 ;
      advance_minimum_slot t
  | None ->
      ()

(* Scout in Paxos made moderately complex terminology *)
let p1 (t : t) =
  let* () = Logs_lwt.debug (fun m -> m "Attempting to attain leadership.") in
  let p1a : p1a = {ballot= t.ballot_num} in
  let p1a_bytes = Protobuf.Encoder.encode_exn p1a_to_protobuf p1a in
  try
    let parse_exn p1b_bytes =
      let p1b : p1b =
        Protobuf.Decoder.decode_exn p1b_from_protobuf p1b_bytes
      in
      if Ballot.equal p1b.ballot t.ballot_num then p1b
      else raise (ExnPreempted p1b.ballot)
    in
    let p_p1b_uris =
      List.map t.acceptor_uris_p1 ~f:(fun uri ->
          let* p1b_string = comm uri p1a_bytes in
          let p1b = parse_exn @@ Bytes.of_string p1b_string in
          Lwt.return (p1b, uri))
    in
    let* p1bs = achieve_quorum t.acceptor_uris_p1 p_p1b_uris in
    let pvals =
      List.concat
      @@ List.map p1bs ~f:(fun p1b ->
             let pvals = p1b.accepted in
             pvals)
    in
    let ballot =
      List.fold p1bs ~init:(Ballot.bottom ()) ~f:(fun bmax p1b ->
          if Ballot.less_than bmax p1b.ballot then p1b.ballot else bmax)
    in
    let* () =
      Logs_lwt.debug (fun m ->
          m "Achieved leadership over ballot %s." @@ Ballot.to_string ballot)
    in
    Lwt.return @@ Utils.Queue.add (Adopted (ballot, pvals)) t.leader_queue
  with ExnPreempted b ->
    let* () = Lwt_unix.sleep @@ Random.float_range 0. t.timeout_s in
    aimd_md t ;
    Lwt.return @@ Utils.Queue.add (Preempted b) t.leader_queue

(* Commander in Paxos made moderately complex terminology *)
let p2 t (pval : Pval.t) (fulfiller : unit Lwt_condition.t) : unit Lwt.t =
  let p2a : p2a = {pval} in
  let p2a_bytes = Protobuf.Encoder.encode_exn p2a_to_protobuf p2a in
  let* () = Logs_lwt.debug (fun m -> m "Proposing command for slot") in
  try
    let parse_exn p2b_bytes =
      let p2b =
        match Protobuf.Decoder.decode p2b_from_protobuf p2b_bytes with
        | Some v ->
            v
        | None ->
            let nack =
              Protobuf.Decoder.decode_exn nack_from_protobuf p2b_bytes
            in
            let () =
              Logs.debug (fun m ->
                  m "Preempted by ballot: %s" @@ Ballot.to_string nack.ballot)
            in
            raise (ExnPreempted nack.ballot)
      in
      p2b
    in
    let p_p2b_uris =
      List.map t.acceptor_uris_p2 ~f:(fun uri ->
          let* p2b_bytes = comm uri p2a_bytes in
          let p2b = parse_exn @@ Bytes.of_string p2b_bytes in
          Lwt.return (p2b, uri))
    in
    let* _ = achieve_quorum t.acceptor_uris_p1 p_p2b_uris in
    let* () = Logs_lwt.debug (fun m -> m "Achieved quorum over request") in
    let _, s, _ = pval in
    Hashtbl.set t.decided_log ~key:s ~data:pval ;
    aimd_ai t ;
    Lwt_condition.broadcast fulfiller () ;
    advance_minimum_slot t ;
    Lwt.return_unit
  with ExnPreempted b ->
    t.timeout_s <- t.timeout_s *. t.aimd_md ;
    (* If preempted by a ballot while waiting for quorum *)
    Utils.Queue.add (Preempted b) t.leader_queue ;
    Lwt.return_unit

(* Receive messages from client, p1 and p2 processes *)
let rec leader_queue_loop t =
  let* msg = Utils.Queue.take t.leader_queue in
  let p_process =
    match msg with
    | Propose (s, fc) -> (
      match
        ( Base.Hashtbl.find t.decided_log s
        , Base.Hashtbl.find t.current_proposals s )
      with
      | None, None ->
          (* doesn't get added to proposals since a thread is either spawned or or pawned off on another leader *)
          (*Base.Hashtbl.set t.current_proposals ~key:s ~data:fc ;*)
          if t.is_leader then
            let f, c = fc in
            p2 t (t.ballot_num, s, c) f
          else Lwt.return_unit
      | _ ->
          Lwt.return_unit )
    | Adopted (b, ps) ->
        if Ballot.equal b t.ballot_num then (
          (* ballot b that has been accepted, and all adopted pvals before b*)
          let () =
            (* Update decided_values with most recent value *)
            List.iter ps ~f:(fun ((b, s, _) as pval) ->
                Base.Hashtbl.change t.decided_log s ~f:(fun data ->
                    match data with
                    | Some ((b', _, _) as pval') ->
                        if Ballot.less_than b' b then Some pval else Some pval'
                    | None ->
                        Some pval))
          in
          advance_minimum_slot t ;
          t.is_leader <- true ;
          let current_proposals = Base.Hashtbl.to_alist t.current_proposals in
          t.current_proposals <- Base.Hashtbl.create (module Base.Int) ;
          (* Spawn a new thread for each proposal *)
          Lwt.join
            (List.map current_proposals ~f:(fun (s, fc) ->
                 let fulfiller, command = fc in
                 p2 t (b, s, command) fulfiller)) )
        else Lwt.return_unit
    | Preempted b ->
        if Ballot.less_than t.ballot_num b then (
          t.is_leader <- false ;
          t.ballot_num <- Ballot.succ_exn b ;
          p1 t )
        else Lwt.return_unit
  in
  Lwt.join [p_process; leader_queue_loop t]

module Replica_Request_Server = Server.Make_Server (struct
  type nonrec t = t

  let connected_callback :
      Lwt_io.input_channel * Lwt_io.output_channel -> t -> unit Lwt.t =
   fun (ic, oc) (t : t) ->
    let* msg = Lwt_io.read_line ic in
    let rep_req =
      msg |> Bytes.of_string
      |> Protobuf.Decoder.decode_exn Messaging.replica_request_from_protobuf
    in
    let fulfiller = Lwt_condition.create () in
    Utils.Queue.add
      (Propose (rep_req.slot_num, (fulfiller, rep_req.command)))
      t.leader_queue ;
    let* () = Lwt_condition.wait fulfiller in
    let decision_response =
      {slot_num= rep_req.slot_num; command= rep_req.command}
    in
    let decision_response_string =
      decision_response
      |> Protobuf.Encoder.encode_exn Messaging.decision_response_to_protobuf
      |> Bytes.to_string
    in
    List.iter t.replica_uris ~f:(fun uri ->
        Lwt.async (fun () -> Utils.send uri decision_response_string)) ;
    Lwt_io.write_line oc decision_response_string
end)

let create acceptor_uris_p1 acceptor_uris_p2 replica_uris timeout_s =
  Base.Random.self_init () ;
  { id= Types.create_id ()
  ; acceptor_uris_p1
  ; acceptor_uris_p2
  ; replica_uris
  ; ballot_num= Ballot.bottom ()
  ; is_leader= false
  ; decided_log= Base.Hashtbl.create (module Int)
  ; slot_out= 0
  ; current_proposals= Base.Hashtbl.create (module Int)
  ; leader_queue= Utils.Queue.create ()
  ; timeout_s
  ; aimd_ai= 0.1
  ; aimd_md= 1.2 }

let start t host port =
  Lwt.join [p1 t; leader_queue_loop t; Replica_Request_Server.start host port t]

let create_and_start_leader host client_port acceptor_uris_p1 acceptor_uris_p2
    replica_uris initial_timeout =
  let t =
    create acceptor_uris_p1 acceptor_uris_p2 replica_uris initial_timeout
  in
  start t host client_port

open Server
open Types
open Utils
open Base

(* TODO implement *)
type state

type 'a fulfilable_command = 'a Lwt_condition.t * command

let to_command : 'a fulfilable_command -> command = fun (_, c) -> c

type leader_msg =
  | Propose of slot_number * unit fulfilable_command
  | Adopted of Ballot.t * Pval.t list (* This is probably just a single element *)
  | Preempted of Ballot.t

exception NotLeader

let not_leader (cond, _) = Lwt_condition.broadcast_exn cond NotLeader

type t =
  { id: leader_id
  ; leader_uris: (leader_id, Lwt_unix.sockaddr) Base.Hashtbl.t
  ; acceptor_uris_p1: Lwt_unix.sockaddr list
  ; acceptor_uris_p2: Lwt_unix.sockaddr list
  ; mutable ballot_num: Ballot.t
  ; mutable is_leader: bool
  ; accepted_log: (Types.slot_number, Types.command) Base.Hashtbl.t
  ; mutable current_proposals:
      (Types.slot_number, unit fulfilable_command) Base.Hashtbl.t
  ; mutable leader_queue: leader_msg Utils.Queue.t
  ; mutable timeout_s: float
  ; aimd_ai: float
  ; aimd_md: float
  ; state: state
  ; window: int
  ; mutable slot_in: slot_number
  ; mutable slot_out: slot_number
  ; slot_free: unit Lwt_condition.t
  ; believed_leader: leader_id option }

module Leader = struct
  type nonrec t = t

  exception ExnPreempted of Ballot.t

  (* Attempts to send msg to all acceptors parses the response as required
   * parser should throw preempted if required *)
  (* Needed bc. can't test the ballot equality without parsing... *)
  let parse_achieve_quorum acceptor_uris msg parse =
    let ps =
      List.map acceptor_uris ~f:(fun uri ->
          let* ic, oc = Utils.connect uri in
          let* () = Lwt_io.write_line oc msg in
          let* resp = Lwt_io.read_line ic |> parse in
          (* If premepted is raised then that will escalate out of function *)
          Lwt.return (resp, uri))
    in
    (* Add responses from rem_list into the quorum as they arrive *)
    let rec wait_until_quorum q rem_list =
      if Quorum.is_majority q then Lwt.return_unit
      else
        let* found, rem = Lwt.nchoose_split rem_list in
        let q =
          List.fold found ~init:q ~f:(fun q (_, uri) -> Quorum.add q uri)
        in
        wait_until_quorum q rem
    in
    let* () = wait_until_quorum (List.length acceptor_uris, []) ps in
    Lwt.nchoose
    @@ List.map ps ~f:(fun p ->
           let* a, _ = p in
           a)

  (* Scout in Paxos made moderately complex terminology *)
  let p1 node =
    let msg = assert false in
    try
      let parse_exn = node.ballot_num |> assert false in
      let* p1bs = parse_achieve_quorum node.acceptor_uris_p1 msg parse_exn in
      let pvals = p1bs |> assert false in
      let ballot = p1bs |> assert false in
      Lwt.return @@ Utils.Queue.add (Adopted (ballot, pvals)) node.leader_queue
    with ExnPreempted b ->
      (* If preempted by a ballot while waiting for quorum *)
      let* () = Lwt_unix.sleep @@ Random.float_range 0. node.timeout_s in
      node.timeout_s <- node.timeout_s *. node.aimd_md ;
      Lwt.return @@ Utils.Queue.add (Preempted b) node.leader_queue

  (* Commander in Paxos made moderately complex terminology *)
  let p2 node (pval : Pval.t) (fulfiller : unit Lwt_condition.t) : unit Lwt.t =
    let parse = node.ballot_num |> assert false in
    let p2a_msg = pval |> assert false in
    try
      let* _ = parse_achieve_quorum node.acceptor_uris_p2 p2a_msg parse in
      let _, s, c = pval in
      Hashtbl.set node.accepted_log ~key:s ~data:c ;
      node.timeout_s <- node.timeout_s -. node.aimd_ai ;
      Lwt_condition.broadcast fulfiller () ;
      Lwt.return_unit
    with ExnPreempted b ->
      node.timeout_s <- node.timeout_s *. node.aimd_md ;
      (* If preempted by a ballot while waiting for quorum *)
      Utils.Queue.add (Preempted b) node.leader_queue ;
      Lwt.return_unit

  let rec receive_leader_msg node =
    let* msg = Utils.Queue.take node.leader_queue in
    Lwt.join
      [ ( match msg with
        | Propose (s, fc) -> (
          match
            ( Base.Hashtbl.find node.accepted_log s
            , Base.Hashtbl.find node.current_proposals s )
          with
          | None, None ->
              (* doesn't get added to proposals since a thread is either spawned or or pawned off on another leader *)
              (*Base.Hashtbl.set node.current_proposals ~key:s ~data:fc ;*)
              let* () =
                if node.is_leader then
                  let f, c = fc in
                  p2 node (node.ballot_num, s, c) f
                else (not_leader fc ; Lwt.return_unit)
              in
              Lwt.return_unit
          | _ ->
              Lwt.return_unit )
        | Adopted (b, ps) ->
            if Ballot.equal b node.ballot_num then (
              (* ballot b that has been accepted, and all adopted pvals before b*)
              let () =
                (* Update proposals with more recent value *)
                List.iter ps ~f:(fun (_, s, c) ->
                    Base.Hashtbl.change node.accepted_log s ~f:(fun data ->
                        match data with
                        | Some c' ->
                            assert (Types.commands_equal c' c) ;
                            Some c
                        | None ->
                            Some c))
              in
              node.is_leader <- true ;
              let current_proposals =
                Base.Hashtbl.to_alist node.current_proposals
              in
              node.current_proposals <- Base.Hashtbl.create (module Base.Int) ;
              (* Spawn a new thread for each proposal *)
              Lwt.join
                (List.map current_proposals ~f:(fun (s, fc) ->
                     let fulfiller, command = fc in
                     p2 node (b, s, command) fulfiller)) )
            else Lwt.return_unit
        | Preempted b ->
            if Ballot.less_than node.ballot_num b then (
              node.is_leader <- false ;
              node.ballot_num <- Ballot.succ_exn b ) ;
            p1 node )
      ; receive_leader_msg node ]

  let create node _ = Lwt.join [p1 node; receive_leader_msg node]
end

module Replica = struct
  type nonrec t = t

  let slot_run node f =
    let* () =
      if node.slot_in >= node.slot_out + node.window then
        Lwt_condition.wait node.slot_free
      else Lwt.return_unit
    in
    node.slot_in <- node.slot_in + 1 ;
    let* () = f () in
    node.slot_out <- node.slot_out + 1 ;
    Lwt_condition.signal node.slot_free () ;
    Lwt.return_unit

  module ClientRequestServer = Make_Server (struct
    type nonrec t = t

    let connected_callback :
        Lwt_io.input_channel * Lwt_io.output_channel -> t -> unit Lwt.t =
     fun (ic, oc) (node : t) ->
      slot_run node (fun () ->
          let* msg = Lwt_io.read_line ic in
          let s, c = msg |> assert false in
          let complete = Lwt_condition.create () in
          (* If the computation is complete *)
          try
            let () =
              Utils.Queue.add (Propose (s, (complete, c))) node.leader_queue
            in
            let* () = Lwt_condition.wait complete in
            let resp = assert false in
            Lwt_io.write_line oc resp
          with NotLeader ->
            let new_leader_id =
              match node.believed_leader with
              | Some v ->
                  v
              | None ->
                  assert false
            in
            let leader_uri = Hashtbl.find_exn node.leader_uris new_leader_id in
            let* ic', oc' = Utils.connect leader_uri in
            let* () = Lwt_io.write_line oc' msg in
            let* resp = Lwt_io.read_line ic' in
            Lwt_io.write_line oc resp)
  end)
end

(*
let initialize leader_id leader_ids leader_uris init_timeout aimd init_state =
  Base.Random.self_init () ;
  assert false
   *)

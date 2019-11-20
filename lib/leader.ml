open Types
open Base
open Messaging
open State_machine
open Utils

let leader = Logs.Src.create "Leader" ~doc:"Leader module"

module LLog = (val Logs.src_log leader : Logs.LOG)

type leader_state =
  | Leader of {ballot: Ballot.t}
  | Not_Leader of {ballot: Ballot.t}
  | Deciding of
      {ballot: Ballot.t; quorum: string Quorum.t; fulfiller: unit Lwt.u}

let string_of_leader_state = function
  | Leader {ballot} ->
      Printf.sprintf "(Leader %s)" @@ Ballot.to_string ballot
  | Not_Leader {ballot} ->
      Printf.sprintf "(Not_Leader %s)" @@ Ballot.to_string ballot
  | Deciding {ballot; _} ->
      Printf.sprintf "(Deciding %s)" @@ Ballot.to_string ballot

type in_flight = {quorum: string Quorum.t; pval: Pval.t; fulfiller: unit Lwt.u}

type t =
  { id: leader_id
  ; mutable leader_state: leader_state
  ; msg_layer: Msg_layer.t
  ; quorum_p1: unit -> string Quorum.t
  ; quorum_p2: unit -> string Quorum.t
  ; proposals: StateMachine.command Hash_set.t
  ; in_flight: (int, in_flight) Hashtbl.t
  ; awaiting_sm: (int, StateMachine.op_result Lwt.u) Hashtbl.t
  ; decided_log: (int, StateMachine.command) Hashtbl.t
  ; decided_commands: (StateMachine.command, StateMachine.op_result) Hashtbl.t
  ; slot_queue: PQueue.t
  ; slot_committed: unit Lwt_condition.t
  ; state: StateMachine.t
  ; mutable highest_undecided_slot: int
  ; mutable leader_duel_timeout: ExpTimeout.t
  ; leader_duel_timeout_gen: unit -> ExpTimeout.t }

let p1a (t : t) () =
  LLog.debug (fun m -> m "Sending p1a msgs") ;
  match t.leader_state with
  | Not_Leader {ballot} ->
      let ballot = Ballot.succ_exn ballot t.id in
      let p1a = ({ballot} : Messaging.p1a) |> Messaging.to_string P1a in
      let promise, fulfiller = Lwt.task () in
      t.leader_state <- Deciding {ballot; quorum= t.quorum_p1 (); fulfiller} ;
      LLog.debug (fun m -> m "p1a: sent, now awaiting quorum") ;
      Msg_layer.send_msg t.msg_layer p1a ~finished:promise ~filter:"p1a"
  | _ ->
      LLog.debug (fun m -> m "Not sending p1a, wrong state") ;
      (* Either already leader, or already sent p1a *)
      Lwt.return_unit

let p1b_callback t (p1b : p1b) =
  LLog.debug (fun m ->
      m "%s: Got p1b for ballot = %s"
        (string_of_leader_state t.leader_state)
        (Ballot.to_string p1b.ballot)) ;
  ( match t.leader_state with
  | Deciding {ballot; quorum; fulfiller; _}
    when Ballot.phys_equal p1b.ballot ballot ->
      quorum.add p1b.id ;
      if quorum.sufficient () then (
        LLog.debug (fun m ->
            m "p1b: got quorum, now leader of ballot %s"
            @@ Ballot.to_string ballot) ;
        t.leader_state <- Leader {ballot} ;
        t.leader_duel_timeout <- t.leader_duel_timeout_gen ();
        Lwt.wakeup_later fulfiller () )
  | _ ->
      () ) ;
  Lwt.return_unit

let node_dead_watch t ballot = 
  match 
    Msg_layer.node_dead_watch t.msg_layer
      ~node:(Ballot.get_leader_id_exn ballot)
      ~callback:(p1a t)
  with
  | Ok () ->
    ()
  | Error `NodeNotFound ->
    assert false 

let preempt_p1 t incomming_ballot =
  let open Ballot.Infix in
  ( match t.leader_state with
  | Deciding {ballot; fulfiller; _}
    when incomming_ballot >= ballot
         && not (Ballot.phys_equal incomming_ballot ballot) ->
      t.leader_state <- Not_Leader {ballot= incomming_ballot} ;
      Lwt.wakeup_later fulfiller () ;
      LLog.debug (fun m ->
          m "Leader duel in progress, sending p1a, preemption_timeout=%f"
            t.leader_duel_timeout.timeout) ;
      Lwt.async (fun () ->
          let%lwt () = ExpTimeout.wait t.leader_duel_timeout in
          p1a t ())
  | Leader {ballot} when incomming_ballot <= ballot -> () (* Either is for this ballot (just delayed) or previous *)
  | Leader {ballot} when incomming_ballot > ballot -> (
      (*If p1 then either remote node wins leader election, or duel occurs and thus at least one wins *)
      (*Preempted thus wait for new leader to die then call another p1a *)
      LLog.debug (fun m -> m "Got preempted, setting p1a") ;
      let () =  node_dead_watch t incomming_ballot in 
      t.leader_state <- Not_Leader {ballot= incomming_ballot} ;
    )
  | Not_Leader {ballot} when Ballot.greater_than incomming_ballot ballot ->
    let () = node_dead_watch t incomming_ballot in
    t.leader_state <- Not_Leader {ballot} 
  | _ ->
      () ) ;
  Lwt.return_unit

let p1b_preempted_callback t (p1b : p1b) = preempt_p1 t p1b.ballot

let nack_p1_preempted_callback t (nack : nack_p1) = preempt_p1 t nack.ballot

let p2a t pval () =
  LLog.debug (fun m -> m "Sending p2a msgs") ;
  match t.leader_state with
  | Leader _ ->
      let p2a =
        {pval}
        |> Protobuf.Encoder.encode_exn p2a_to_protobuf
        |> Bytes.to_string
      in
      let promise, fulfiller = Lwt.task () in
      let result, result_fulfiller = Lwt.task () in
      let quorum = t.quorum_p2 () in
      ( match
          ( Hashtbl.add t.in_flight ~key:(Pval.get_slot pval)
              ~data:{quorum; pval; fulfiller}
          , Hashtbl.add t.awaiting_sm ~key:(Pval.get_slot pval)
              ~data:result_fulfiller )
        with
      | `Ok, `Ok ->
          ()
      | _ ->
          assert false ) ;
      let%lwt () =
        Msg_layer.send_msg t.msg_layer p2a ~finished:promise ~filter:"p2a"
      in
      result
  | Not_Leader _ | Deciding _ ->
      Lwt.return @@ StateMachine.op_result_failure ()

let broadcast_decision t pval =
  let slot = Pval.get_slot pval in
  let command = Pval.get_cmd pval in
  let decision_response =
    {slot; command}
    |> Protobuf.Encoder.encode_exn decision_response_to_protobuf
    |> Bytes.to_string
  in
  Lwt.async
  @@ fun () ->
  Msg_layer.send_msg t.msg_layer ~filter:"decision" decision_response

let update_state t slot command =
  Hashtbl.remove t.in_flight slot ;
  Hashtbl.set t.decided_log ~key:slot ~data:command ;
  Lwt_condition.broadcast t.slot_committed ()

(* Notify state advancer to attempt to increment state *)

let decide t pval fulfiller =
  update_state t (Pval.get_slot pval) (Pval.get_cmd pval) ;
  Lwt.wakeup_later fulfiller () ;
  broadcast_decision t pval

let p2b_callback t (p2b : p2b) =
  LLog.debug (fun m ->
      m "%s: Got p2b %s "
        (string_of_leader_state t.leader_state)
        (Ballot.to_string p2b.ballot)) ;
  ( match Hashtbl.find t.in_flight (Pval.get_slot p2b.pval) with
  | Some {quorum; pval; fulfiller; _} when Pval.equal pval p2b.pval ->
      quorum.add p2b.id ;
      if quorum.sufficient () then decide t p2b.pval fulfiller
  | Some {pval; _} when not (Pval.equal pval p2b.pval) ->
      assert false
  | _ ->
      () ) ;
  Lwt.return_unit

let decision_callback t decision_response =
  LLog.debug (fun m -> m "Got a decision") ;
  update_state t decision_response.slot decision_response.command ;
  Lwt.return_unit

let state_advancer t () =
  let rec loop () =
    let%lwt () = Lwt_condition.wait t.slot_committed in
    let rec inner_loop () =
      LLog.debug (fun m -> m "Attempting to advance state") ;
      match Hashtbl.find t.decided_log t.highest_undecided_slot with
      | Some command ->
          LLog.debug (fun m ->
              m "Advancing state from %d to %d" t.highest_undecided_slot
                (t.highest_undecided_slot + 1)) ;
          let slot = t.highest_undecided_slot in
          t.highest_undecided_slot <- slot + 1 ;
          let res = StateMachine.update t.state command in
          ( match Hashtbl.find t.awaiting_sm slot with
          | Some fulfiller ->
              Lwt.wakeup_later fulfiller res ;
              Hashtbl.remove t.awaiting_sm slot
          | None ->
              () ) ;
          Hashtbl.set t.decided_commands ~key:command ~data:res ;
          inner_loop ()
      | None ->
          Lwt.return_unit
    in
    let%lwt () = inner_loop () in
    loop ()
  in
  loop ()

let get_ballot t =
  match t.leader_state with
  | Leader {ballot} | Not_Leader {ballot; _} | Deciding {ballot; _} ->
      ballot

let preempt_p2 t incomming_ballot =
  let ballot = get_ballot t in
  if Ballot.Infix.(incomming_ballot > ballot) then (
    ( match t.leader_state with
    | Deciding {fulfiller; _} ->
        Lwt.wakeup_later fulfiller ()
    | _ ->
        () ) ;
    (*Preempted thus wait for new leader to die then p1a *)
    t.leader_state <- Not_Leader {ballot= incomming_ballot} ;
    LLog.debug (fun m ->
        m "%s: Got preempted, setting p1a"
        @@ string_of_leader_state t.leader_state) ;
    match
      Msg_layer.node_dead_watch t.msg_layer
        ~node:(Ballot.get_leader_id_exn incomming_ballot)
        ~callback:(p1a t)
    with
    | Ok _ ->
        ()
    | Error `NodeNotFound ->
        assert false )

let preempt_p2a t (p2a : p2a) =
  preempt_p2 t (p2a.pval |> Pval.get_ballot) ;
  Lwt.return_unit

let preempt_p2b t (p2b : p2b) = preempt_p2 t p2b.ballot ; Lwt.return_unit

let preempt_nack_p2 t (nack : nack_p2) =
  preempt_p2 t nack.ballot ; Lwt.return_unit

let client t msg writer =
  LLog.debug (fun m ->
      m "%s: Got client request" @@ string_of_leader_state t.leader_state) ;
  let client_request =
    msg |> Bytes.of_string
    |> Protobuf.Decoder.decode_exn client_request_from_protobuf
  in
  match
    (t.leader_state, Hashtbl.find t.decided_commands client_request.command)
  with
  | _, Some result ->
      let client_response =
        {result}
        |> Protobuf.Encoder.encode_exn client_response_to_protobuf
        |> Bytes.to_string
      in
      Lwt.async (fun () ->
          LLog.debug (fun m ->
              m "Already decided client_command %s"
                ( client_request.command |> StateMachine.sexp_of_command
                |> Sexp.to_string )) ;
          writer client_response) ;
      Lwt.return_unit
  | Leader {ballot}, None ->
      let slot = Utils.PQueue.take t.slot_queue in
      let%lwt result = p2a t (ballot, slot, client_request.command) () in
      let client_response =
        {result}
        |> Protobuf.Encoder.encode_exn client_response_to_protobuf
        |> Bytes.to_string
      in
      Lwt.async (fun () ->
          LLog.debug (fun m -> m "Sending client response for slot:%d" slot) ;
          writer client_response) ;
      Lwt.return_unit
  | _ ->
      Lwt.return_unit

let create msg_layer local nodes ~nodeid ~address_req ~address_rep =
  let quorum_p1, quorum_p2 = Quorum.majority nodes in
  let leader_state = Not_Leader {ballot= Ballot.init local} in
  Random.self_init () ;
  let leader_duel_timeout_gen () = ExpTimeout.create ~timeout:5. ~c:(1. +. (1. /. nodeid)) in
  let leader_duel_timeout = leader_duel_timeout_gen () in
  let t =
    { id= local
    ; leader_state
    ; msg_layer
    ; quorum_p1
    ; quorum_p2
    ; proposals= Hash_set.Poly.create ()
    ; in_flight= Hashtbl.create (module Int)
    ; awaiting_sm= Hashtbl.create (module Int)
    ; decided_log= Hashtbl.create (module Int)
    ; decided_commands= Hashtbl.Poly.create ()
    ; slot_queue= Utils.PQueue.create ()
    ; slot_committed= Lwt_condition.create ()
    ; state= StateMachine.create ()
    ; highest_undecided_slot= 0
    ; leader_duel_timeout 
    ; leader_duel_timeout_gen}
  in
  let open Messaging in
  Msg_layer.attach_watch t.msg_layer ~filter:"p1b" ~callback:(p1b_callback t)
    ~typ:P1b ;
  Msg_layer.attach_watch t.msg_layer ~filter:"p2b" ~callback:(p2b_callback t)
    ~typ:P2b ;
  Msg_layer.attach_watch t.msg_layer ~filter:"decision"
    ~callback:(decision_callback t) ~typ:DRp ;
  Msg_layer.attach_watch t.msg_layer ~filter:"p1b"
    ~callback:(p1b_preempted_callback t) ~typ:P1b ;
  Msg_layer.attach_watch t.msg_layer ~filter:"nack_p1"
    ~callback:(nack_p1_preempted_callback t)
    ~typ:Np1 ;
  Msg_layer.attach_watch t.msg_layer ~filter:"p2a" ~callback:(preempt_p2a t)
    ~typ:P2a ;
  Msg_layer.attach_watch t.msg_layer ~filter:"p2b" ~callback:(preempt_p2b t)
    ~typ:P2b ;
  Msg_layer.attach_watch t.msg_layer ~filter:"nack_p2"
    ~callback:(preempt_nack_p2 t) ~typ:Np2 ;
  let c =
    Msg_layer.client_socket ~callback:(client t) ~address_req ~address_rep
      msg_layer
  in
  let sp = state_advancer t () in
  let p_p1a =
    let s_nodes = List.sort nodes ~compare:String.compare in
    match s_nodes with
    | l :: _ when String.equal l local ->
        LLog.debug (fun m -> m "Primary -> attempting p1a") ;
        p1a t ()
    | l :: _ -> (
        LLog.debug (fun m -> m "Not primary -> assigning death watch") ;
        match
          Msg_layer.node_dead_watch t.msg_layer ~node:l ~callback:(p1a t)
        with
        | Ok _ ->
            Lwt.return_unit
        | Error `NodeNotFound ->
            assert false )
    | _ ->
        assert false
  in
  (t, Lwt.join [c; sp; p_p1a])

let create_independent local nodes ~nodeid ~address_req ~address_rep
    alive_timeout =
  let msg, psml = Msg_layer.create ~node_list:nodes ~local ~alive_timeout in
  let t, psa = create msg local nodes ~nodeid ~address_req ~address_rep in
  (t, Lwt.join [psml; psa])

open Types
open Base
open Messaging
open State_machine

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
  ; slot_queue: Utils.PQueue.t
  ; slot_committed: unit Lwt_condition.t
  ; state: StateMachine.t
  ; mutable highest_undecided_slot: int
  ; timeout: float
  ; mutable preemption_timeout: float
  ; preemption_coefficient: float }

let p1a (t : t) () =
  LLog.debug (fun m -> m "Sending p1a msgs") ;
  match t.leader_state with
  | Not_Leader {ballot} ->
      let ballot = Ballot.succ_exn ballot t.id in
      let p1a =
        ({ballot} : Messaging.p1a)
        |> Protobuf.Encoder.encode_exn Messaging.p1a_to_protobuf
        |> Bytes.to_string
      in
      let promise, fulfiller = Lwt.task () in
      t.leader_state <- Deciding {ballot; quorum= t.quorum_p1 (); fulfiller} ;
      LLog.debug (fun m -> m "p1a: awaiting quorum") ;
      let%lwt () =
        Lwt_unix.sleep @@ Random.float_range 0. t.preemption_timeout
      in
      Msg_layer.send_msg t.msg_layer p1a ~finished:promise ~filter:"p1a"
  | _ ->
      LLog.debug (fun m -> m "Not sending p1a, wrong state") ;
      (* Either already leader, or already sent p1a *)
      Lwt.return_unit

let p1b_callback t msg =
  let p1b =
    Bytes.of_string msg |> Protobuf.Decoder.decode_exn p1b_from_protobuf
  in
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
        Lwt.wakeup_later fulfiller () ;
        t.preemption_timeout <- t.timeout )
  | _ ->
      () ) ;
  Lwt.return_unit

let preempt_p1 t incomming_ballot =
  let ( >= ) a b = not @@ Ballot.less_than a b in
  ( match t.leader_state with
  | Deciding {ballot; fulfiller; _}
    when incomming_ballot >= ballot
         && not (Ballot.phys_equal incomming_ballot ballot) ->
      t.leader_state <- Not_Leader {ballot= incomming_ballot} ;
      Lwt.wakeup_later fulfiller () ;
      t.preemption_timeout <- t.preemption_timeout *. t.preemption_coefficient ;
      LLog.debug (fun m -> m "Leader duel in progress, sending p1a") ;
      Lwt.async (p1a t)
  | Leader {ballot} when Ballot.greater_than incomming_ballot ballot -> (
      (*Preempted thus wait for new leader to die then p1a *)
      t.leader_state <- Not_Leader {ballot= incomming_ballot} ;
      LLog.debug (fun m -> m "Got preempted, setting p1a") ;
      match
        Msg_layer.node_dead_watch t.msg_layer
          ~node:(Ballot.get_leader_id_exn incomming_ballot)
          ~callback:(p1a t)
      with
      | Ok _ ->
          ()
      | Error `NodeNotFound ->
          assert false )
  | Not_Leader {ballot} when Ballot.greater_than incomming_ballot ballot ->
      t.leader_state <- Not_Leader {ballot} (* TODO reapply death_watch? *)
  | _ ->
      () ) ;
  Lwt.return_unit

let p1a_preempted_callback t msg =
  let p1a =
    Bytes.of_string msg |> Protobuf.Decoder.decode_exn p1a_from_protobuf
  in
  preempt_p1 t p1a.ballot

let p1b_preempted_callback t msg =
  let p1b =
    Bytes.of_string msg |> Protobuf.Decoder.decode_exn p1b_from_protobuf
  in
  preempt_p1 t p1b.ballot

let nack_p1_preempted_callback t msg =
  let nack =
    Bytes.of_string msg |> Protobuf.Decoder.decode_exn nack_p1_from_protobuf
  in
  preempt_p1 t nack.ballot

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
      | _ (*res1, res2*) ->
          (*
        let err_to_string = function
            | `Ok ->
                "Ok"
            | `Duplicate ->
                "Duplicate"
          in
          LLog.debug (fun m ->
              m "res1: %s\n res2: %s\n" (res1 |> err_to_string)
                (res2 |> err_to_string)) ;
            *)
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
  LLog.debug (fun m -> m "Updating state for slot: %d" slot) ;
  Hashtbl.remove t.in_flight slot ;
  Hashtbl.set t.decided_log ~key:slot ~data:command ;
  Lwt_condition.broadcast t.slot_committed ()

let p2b_callback t msg =
  LLog.debug (fun m ->
      m "%s: Got p1b " (string_of_leader_state t.leader_state)) ;
  let p2b =
    Bytes.of_string msg |> Protobuf.Decoder.decode_exn p2b_from_protobuf
  in
  ( match Hashtbl.find t.in_flight (Pval.get_slot p2b.pval) with
  | Some {quorum; pval; fulfiller; _} when Pval.equal pval p2b.pval ->
      quorum.add p2b.id ;
      if quorum.sufficient () then (
        update_state t (Pval.get_slot p2b.pval) (Pval.get_cmd p2b.pval) ;
        Lwt.wakeup_later fulfiller () ;
        broadcast_decision t p2b.pval )
  | Some {pval; _} when not (Pval.equal pval p2b.pval) ->
      assert false
  | _ ->
      () ) ;
  Lwt.return_unit

let client t msg =
  LLog.debug (fun m ->
      m "%s: Got client request" @@ string_of_leader_state t.leader_state) ;
  let client_request =
    msg |> Bytes.of_string
    |> Protobuf.Decoder.decode_exn client_request_from_protobuf
  in
  match t.leader_state with
  | Leader {ballot} ->
      let slot = Utils.PQueue.take t.slot_queue in
      let%lwt result = p2a t (ballot, slot, client_request.command) () in
      let client_response =
        {result= Some result}
        |> Protobuf.Encoder.encode_exn client_response_to_protobuf
        |> Bytes.to_string
      in
      Lwt.return client_response
  | _ ->
      {result= None}
      |> Protobuf.Encoder.encode_exn client_response_to_protobuf
      |> Bytes.to_string |> Lwt.return

let decision_callback t msg =
  LLog.debug (fun m -> m "Got a decision") ;
  let decision_response =
    msg |> Bytes.of_string
    |> Protobuf.Decoder.decode_exn decision_response_from_protobuf
  in
  update_state t decision_response.slot decision_response.command ;
  Lwt.return_unit

let state_advancer t () =
  let rec loop () =
    let%lwt () = Lwt_condition.wait t.slot_committed in
    let rec inner_loop () =
      LLog.debug (fun m -> m "Attempting to advance state") ;
      let slot = t.highest_undecided_slot in
      match Hashtbl.find t.decided_log slot with
      | Some command ->
          LLog.debug (fun m -> m "Advancing state for slot %d" slot) ;
          t.highest_undecided_slot <- slot + 1 ;
          let res = StateMachine.update t.state command in
          ( match Hashtbl.find t.awaiting_sm slot with
          | Some fulfiller ->
              Lwt.wakeup_later fulfiller res ;
              Hashtbl.remove t.awaiting_sm slot
          | None ->
              () ) ;
          inner_loop ()
      | None ->
          Lwt.return_unit
    in
    let%lwt () = inner_loop () in
    loop ()
  in
  loop ()

let preempt_p2 t incomming_ballot ballot =
  if Ballot.greater_than incomming_ballot ballot then (
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

let get_ballot t =
  match t.leader_state with
  | Leader {ballot} | Not_Leader {ballot; _} | Deciding {ballot; _} ->
      ballot

let preempt_p2a t msg =
  let incomming_ballot =
    let p2a =
      msg |> Bytes.of_string |> Protobuf.Decoder.decode_exn p2a_from_protobuf
    in
    p2a.pval |> Pval.get_ballot
  in
  let ballot = get_ballot t in
  preempt_p2 t incomming_ballot ballot ;
  Lwt.return_unit

let preempt_p2b t msg =
  let incomming_ballot =
    let p2b =
      msg |> Bytes.of_string |> Protobuf.Decoder.decode_exn p2b_from_protobuf
    in
    p2b.ballot
  in
  let ballot = get_ballot t in
  preempt_p2 t incomming_ballot ballot ;
  Lwt.return_unit

let preempt_nack_p2 t msg =
  let incomming_ballot =
    let nack =
      msg |> Bytes.of_string
      |> Protobuf.Decoder.decode_exn nack_p2_from_protobuf
    in
    nack.ballot
  in
  let ballot = get_ballot t in
  preempt_p2 t incomming_ballot ballot ;
  Lwt.return_unit

let create msg_layer local nodes client_address =
  let quorum_p1, quorum_p2 = Quorum.majority nodes in
  let leader_state = Not_Leader {ballot= Ballot.init local} in
  let timeout = 3. in
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
    ; slot_queue= Utils.PQueue.create ()
    ; slot_committed= Lwt_condition.create ()
    ; state= StateMachine.create ()
    ; highest_undecided_slot= 0
    ; timeout
    ; preemption_timeout= timeout
    ; preemption_coefficient= 1.5 }
  in
  Msg_layer.attach_watch t.msg_layer ~filter:"p1b" ~callback:(p1b_callback t) ;
  Msg_layer.attach_watch t.msg_layer ~filter:"p2b" ~callback:(p2b_callback t) ;
  Msg_layer.attach_watch t.msg_layer ~filter:"decision"
    ~callback:(decision_callback t) ;
  Msg_layer.attach_watch t.msg_layer ~filter:"p1a"
    ~callback:(p1a_preempted_callback t) ;
  Msg_layer.attach_watch t.msg_layer ~filter:"p1b"
    ~callback:(p1b_preempted_callback t) ;
  Msg_layer.attach_watch t.msg_layer ~filter:"nack_p1"
    ~callback:(nack_p1_preempted_callback t) ;
  Msg_layer.attach_watch t.msg_layer ~filter:"p2a" ~callback:(preempt_p2a t) ;
  Msg_layer.attach_watch t.msg_layer ~filter:"p2b" ~callback:(preempt_p2b t) ;
  Msg_layer.attach_watch t.msg_layer ~filter:"nack_p2"
    ~callback:(preempt_nack_p2 t) ;
  let sp = state_advancer t () in
  let p =
    Msg_layer.one_use_socket ~callback:(client t) ~address:client_address
      msg_layer
  in
  (t, Lwt.join [p; sp; p1a t ()])

let create_independent local nodes client_address alive_timeout =
  let msg, psml = Msg_layer.create ~node_list:nodes ~local ~alive_timeout in
  let t, psa = create msg local nodes client_address in
  (t, Lwt.join [psml; psa])

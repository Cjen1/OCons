open Base
open Types
open Utils

module BoundedSlot = struct
  type t = {mutable slot: int; semaphore: Semaphore.t}

  let create initial window =
    {slot= initial; semaphore= Semaphore.create window}

  let request t =
    let* () = Semaphore.wait t.semaphore in
    let slot = t.slot in
    t.slot <- t.slot + 1 ;
    Lwt.return slot

  let release t = Semaphore.signal t.semaphore
end

module DecisionSet : sig
  type t

  val find_slot : t -> int -> command option

  val find_command : t -> command -> int option

  val set : t -> int -> command -> unit

  val create : unit -> t
end = struct
  type t = {slot: (int, command) Hashtbl.t; command: (command, int) Hashtbl.t}

  let find_slot t s = Hashtbl.find t.slot s

  let find_command t s = Hashtbl.find t.command s

  let set t s c =
    Hashtbl.set t.slot ~key:s ~data:c ;
    Hashtbl.set t.command ~key:c ~data:s

  let create () =
    {slot= Hashtbl.create (module Int); command= Hashtbl.Poly.create ()}
end

type replica_in_msg =
  | Proposal of Types.result Lwt.u * command
  | Decision of int * command

type t =
  { state: app_state
  ; mutable slot_out: int
  ; mutable slot_in: int
  ; msg_queue: replica_in_msg Queue.t
  ; requests: (Types.result Lwt.u * command) Base.Queue.t
        (* not the lwt based utils one *)
  ; proposals: (int, Types.result Lwt.u * command) Base.Hashtbl.t
  ; decisions: DecisionSet.t
  ; mutable leader_uris: Lwt_unix.sockaddr list
  ; window: int }

let send_to_leaders t (s, c) =
  let msg : Messaging.replica_request = {command= c; slot_num= s} in
  let req =
    msg |> Protobuf.Encoder.encode_exn Messaging.replica_request_to_protobuf
  in
  let* () =
    Logs_lwt.debug (fun m ->
        m "send_to_leaders: sending %s" (string_of_command c))
  in
  let ps =
    List.map t.leader_uris ~f:(fun uri ->
        let* msg = Utils.comm uri req in
        let res =
          msg |> Bytes.of_string
          |> Protobuf.Decoder.decode_exn
               Messaging.decision_response_from_protobuf
        in
        Logs.debug (fun m ->
            m "send_to_leaders_callback: got decision back for slot: %d"
              res.slot_num) ;
        Lwt.return
        @@ Queue.add (Decision (res.slot_num, res.command)) t.msg_queue)
  in
  let* () = Lwt.pick ps in
  let* () =
    Logs_lwt.debug (fun m ->
        m "send_to_leaders: exiting %s" (string_of_command c))
  in
  Lwt.return_unit

let alert_fulfiller = Logs.Src.create "alert_fulfiller" ~doc:""

module Log_alert = (val Logs.src_log alert_fulfiller : Logs.LOG)

let alert_fulfiller_if_exist o_f res =
  match o_f with
  | Some f ->
      Log_alert.debug (fun m -> m "alert_fulfiller: fulfilling request") ;
      Lwt.wakeup_later f res
  | None ->
      Log_alert.debug (fun m -> m "alert_fulfiller: no fulfiller found") ;
      ()

let perform = Logs.Src.create "perform" ~doc:""

module Log_perform = (val Logs.src_log perform : Logs.LOG)

let rec perform t =
  match DecisionSet.find_slot t.decisions t.slot_out with
  | Some command ->
      (let o_f =
         match Hashtbl.find t.proposals t.slot_out with
         | Some (f, c'') -> (
             Hashtbl.remove t.proposals t.slot_out ;
             match commands_equal command c'' with
             | true ->
                 Some f
             | false ->
                 Base.Queue.enqueue t.requests (f, c'') ;
                 None )
         | None ->
             None
       in
       (* Check to see if command has been decided upon before *)
       match (DecisionSet.find_command t.decisions command, command) with
       | _, (_, Reconfigure _) ->
           Log_perform.debug (fun m -> m "Reconfigure in slot: %d " t.slot_out) ;
           t.slot_out <- t.slot_out + 1
       | Some s, _ when s < t.slot_out ->
           Log_perform.debug (fun m -> m "Command already performed") ;
           t.slot_out <- t.slot_out + 1
       | _, (_, op) ->
           let result = Types.apply t.state op in
           let slot = t.slot_out in
           t.slot_out <- t.slot_out + 1 ;
           Log_perform.debug (fun m -> m "Sending alert: slot=%d" slot) ;
           alert_fulfiller_if_exist o_f result) ;
      perform t
  | None ->
      ()

let propose = Logs.Src.create "propose" ~doc:""

module Log_propose = (val Logs.src_log propose : Logs.LOG)

let rec propose t =
  if t.slot_in < t.slot_out + t.window then
    match Base.Queue.dequeue t.requests with
    | Some ((_, c) as fc) ->
        Log_propose.debug (fun m -> m "got request for %s" (string_of_command c)) ;
        (* Do reconfigure if required *)
        let () =
          match DecisionSet.find_slot t.decisions (t.slot_in - t.window) with
          | Some (_, Reconfigure leaders) ->
              t.leader_uris <- List.map ~f:uri_of_string leaders
          | _ ->
              ()
        in
        (* Either send request to leaders with slot, or requeue it *)
        let () =
          match DecisionSet.find_slot t.decisions t.slot_in with
          | None ->
              Log_propose.debug (fun m ->
                  m "No command in slot, proposing %s" (string_of_command c)) ;
              Hashtbl.set t.proposals ~key:t.slot_in ~data:fc ;
              (*Hashtbl.set t.fulfillers ~key:t.slot_in ~data:f ;*)
              Lwt.async (fun () -> send_to_leaders t (t.slot_in, c))
          | Some _ ->
              Base.Queue.enqueue t.requests fc
        in
        t.slot_in <- t.slot_in + 1 ;
        propose t
    | None ->
        ()
  else ()

let rec msg_loop t =
  Logs.debug (fun m -> m "ml: awaiting msg in queue") ;
  let* msg = Queue.take t.msg_queue in
  ( match msg with
  | Proposal (f, c) ->
      Logs.debug (fun m -> m "ml: got proposal for %s" (string_of_command c)) ;
      Base.Queue.enqueue t.requests (f, c)
  | Decision (s, c) ->
      Logs.debug (fun m ->
          m "ml: got decision for %d %s" s (string_of_command c)) ;
      let () = DecisionSet.set t.decisions s c in
      perform t ) ;
  Logs.debug (fun m -> m "ml: proposing possible messages") ;
  propose t ;
  msg_loop t

module ClientRequestServer = Server.Make_Server (struct
  type nonrec t = t

  let connected_callback :
      Lwt_io.input_channel * Lwt_io.output_channel -> t -> unit Lwt.t =
   fun (ic, oc) (t : t) ->
    let* msg = Lwt_io.read_value ic in
    let c =
      ( msg |> Bytes.of_string
      |> Protobuf.Decoder.decode_exn Messaging.client_request_from_protobuf )
        .command
    in
    let p_result, f_result = Lwt.task () in
    let* () =
      Logs_lwt.debug (fun m ->
          m "client_server: Adding request to queue: %s"
            (Types.string_of_command c))
    in
    Queue.add (Proposal (f_result, c)) t.msg_queue ;
    (* If the computation is complete *)
    let* result = p_result in
    let* () = Logs_lwt.debug (fun m -> m "client_server: got fulfilled") in
    let resp =
      ({result} : Messaging.client_response)
      |> Protobuf.Encoder.encode_exn Messaging.client_response_to_protobuf
      |> Bytes.to_string
    in
    Lwt_io.write_value oc resp
end)

module DecisionServer = Server.Make_Server (struct
  type nonrec t = t

  let connected_callback :
      Lwt_io.input_channel * Lwt_io.output_channel -> t -> unit Lwt.t =
   fun (ic, _) (t : t) ->
    let* msg = Lwt_io.read_value ic in
    let res =
      msg |> Bytes.of_string
      |> Protobuf.Decoder.decode_exn Messaging.decision_response_from_protobuf
    in
    Logs.debug (fun m ->
        m "decision_server: got decision for slot: %d" res.slot_num) ;
    Queue.add (Decision (res.slot_num, res.command)) t.msg_queue ;
    Lwt.return_unit
end)

let create leader_uris =
  { state= Types.initial_state ()
  ; slot_out= 0
  ; slot_in= 0
  ; msg_queue= Queue.create ()
  ; requests= Base.Queue.create ()
  ; proposals= Base.Hashtbl.create (module Int)
  ; decisions= DecisionSet.create ()
  ; leader_uris
  ; window= 100 }

let start t host client_port decision_port =
  Lwt.join
    [ ClientRequestServer.start host client_port t
    ; DecisionServer.start host decision_port t
    ; msg_loop t ]

let create_and_start host client_port decision_port leader_uris =
  let* () = Logs_lwt.info (fun m -> m "Spinning up replica") in
  let t = create leader_uris in
  start t host client_port decision_port

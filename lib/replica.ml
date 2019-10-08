open Base
open Types
open Utils

let window = 5

type t =
  { state: app_state
  ; mutable slot_in: slot_number
  ; mutable slot_out: slot_number
  ; requests: (Types.result Lwt_condition.t * command) Queue.t
  ; proposals:
      (slot_number, Types.result Lwt_condition.t * command) Base.Hashtbl.t
  ; decisions: (slot_number, command) Base.Hashtbl.t
  ; mutable leader_uris: Lwt_unix.sockaddr list
  ; slot_out_update: unit Lwt_condition.t }

let inc_slot_out t =
  t.slot_out <- t.slot_out + 1 ;
  Lwt_condition.signal t.slot_out_update ()

let rec advance_state t =
  match Hashtbl.find t.decisions t.slot_out with
  | Some ((_, op) as c) ->
      (let res = Types.apply t.state op in
       inc_slot_out t ;
       match Hashtbl.find t.proposals t.slot_out with
       | Some ((fulfiller, c') as fc) ->
           Hashtbl.remove t.proposals t.slot_out ;
           if not (commands_equal c c') then Queue.add fc t.requests
           else Lwt_condition.broadcast fulfiller res
       | None ->
           ()) ;
      advance_state t
  | None ->
      ()

let send_to_leaders t (s, c) =
  let msg : Messaging.replica_request = {command= c; slot_num= s} in
  let req =
    msg
    |> Protobuf.Encoder.encode_exn Messaging.replica_request_to_protobuf
    |> Bytes.to_string
  in
  let ps =
    List.map t.leader_uris ~f:(fun uri ->
        let* ic, oc = Utils.connect uri in
        let* () = Lwt_io.write_line oc req in
        let* msg = Lwt_io.read_line ic in
        let res =
          msg |> Bytes.of_string
          |> Protobuf.Decoder.decode_exn
               Messaging.decision_response_from_protobuf
        in
        Hashtbl.set t.decisions ~key:res.slot_num ~data:res.command ;
        Lwt.return @@ advance_state t)
  in
  Lwt.pick ps

let rec proposal_loop t =
  let* fc = Queue.take t.requests in
  let* () =
    if not (t.slot_in < t.slot_out + window) then
      Lwt_condition.wait t.slot_out_update
    else Lwt.return_unit
  in
  let slot = t.slot_in in
  t.slot_in <- t.slot_in + 1 ;
  (* May need to be done after receiving a decision *)
  ( match Hashtbl.find_exn t.decisions (t.slot_in - window) with
  | _, Reconfigure leader_list ->
      t.leader_uris <-
        List.map leader_list ~f:(fun leader_str ->
            Utils.uri_of_string leader_str)
  | _ ->
      () ) ;
  Hashtbl.set t.proposals ~key:slot ~data:fc ;
  Lwt.join [send_to_leaders t (slot, snd fc); proposal_loop t]

module ClientRequestServer = Server.Make_Server (struct
  type nonrec t = t

  let connected_callback :
      Lwt_io.input_channel * Lwt_io.output_channel -> t -> unit Lwt.t =
   fun (ic, oc) (t : t) ->
    let* msg = Lwt_io.read_line ic in
    let c =
      ( msg |> Bytes.of_string
      |> Protobuf.Decoder.decode_exn Messaging.client_request_from_protobuf )
        .command
    in
    let request_decided = Lwt_condition.create () in
    Queue.add (request_decided, c) t.requests ;
    (* If the computation is complete *)
    let* result = Lwt_condition.wait request_decided in
    let resp =
      ({result} : Messaging.client_response)
      |> Protobuf.Encoder.encode_exn Messaging.client_response_to_protobuf
      |> Bytes.to_string
    in
    Lwt_io.write_line oc resp
end)

module DecisionServer = Server.Make_Server (struct
  type nonrec t = t

  let connected_callback :
      Lwt_io.input_channel * Lwt_io.output_channel -> t -> unit Lwt.t =
   fun (ic, _) (t : t) ->
    let* msg = Lwt_io.read_line ic in
    let res =
      msg |> Bytes.of_string
      |> Protobuf.Decoder.decode_exn Messaging.decision_response_from_protobuf
    in
    Hashtbl.set t.decisions ~key:res.slot_num ~data:res.command ;
    Lwt.return @@ advance_state t
end)

let create leader_uris =
  { state= Types.initial_state ()
  ; slot_in= 0
  ; slot_out= 0
  ; requests= Queue.create ()
  ; proposals= Base.Hashtbl.create (module Int)
  ; decisions= Base.Hashtbl.create (module Int)
  ; leader_uris
  ; slot_out_update= Lwt_condition.create () }

let start t host client_port decision_port =
  Lwt.join
    [ proposal_loop t
    ; ClientRequestServer.start host client_port t
    ; DecisionServer.start host decision_port t ]

let create_and_start host client_port decision_port leader_uris =
  let t = create leader_uris in
  start t host client_port decision_port

open Base
open State_machine

let client = Logs.Src.create "Client" ~doc:"Client module"

module CLog = (val Logs.src_log client : Logs.LOG)

type t =
  { cid: string
  ; context: Zmq.Context.t
  ; in_flight: (string, Messaging.client_response Lwt.u) Hashtbl.t
  ; fulfilled: string Hash_set.t
  ; endpoints: (string * [`Dealer] Zmq_lwt.Socket.t) list }

let serialise_request op =
  let cid = Types.create_id () in
  ({command= {op; id= cid}} : Messaging.client_request)

let deserialise_response response : State_machine.StateMachine.op_result =
  Messaging.(from_string CRp response).result

let rid = ref 0

let send_to_all t ~rid ~msg =
  List.fold t.endpoints ~init:[] ~f:(fun acc (node_name, endpoint) ->
      let p =
        try%lwt
          CLog.debug (fun m -> m "Attempting to send %s to %s" rid node_name) ;
          let%lwt p = Messaging.send_client_req ~dest:endpoint ~rid ~msg in
          CLog.debug (fun m -> m "Sent %s to %s" rid node_name) ;
          Lwt.return p
        with Unix.Unix_error (e, _, _) ->
          CLog.err (fun m ->
              m "%s failed with %s" node_name (Unix.error_message e)) ;
          Lwt.return_unit
      in
      p :: acc)
  |> Lwt.join

let send t op =
  let msg = serialise_request op in
  let rid =
    rid := !rid + 1 ;
    !rid
  in
  let rid = Printf.sprintf "%s_%i" t.cid rid in
  let finished, fulfiller = Lwt.task () in
  let () = Hashtbl.set t.in_flight ~key:rid ~data:fulfiller in
  let%lwt resp =
    Msg_layer.retry ~finished ~timeout:30. (fun () ->
        Lwt.async (fun () ->
            try%lwt
              CLog.debug (fun m -> m "Attempting to send %s" rid) ;
              let%lwt p = send_to_all t ~rid ~msg in
              CLog.debug (fun m -> m "Sent %s" rid) ;
              Lwt.return p
            with Zmq.ZMQ_exception (_, s) ->
              CLog.err (fun m -> m "%s" s) ;
              Lwt.return_unit))
  in
  Lwt.return @@ resp.result

let op_read t k = send t @@ StateMachine.Read k

let op_write t k v = send t @@ StateMachine.Write (k, v)

let recv_loop t =
  CLog.debug (fun m -> m "Recv_loop: spooling up") ;
  let rec recv_loop ((id, sock) as endpoint) =
    let%lwt resp = Messaging.recv_client_rep ~sock in
    ( match resp with
    | Ok (rid, msg) -> (
      match Hashtbl.find t.in_flight rid with
      | Some fulfiller ->
          Lwt.wakeup_later fulfiller msg ;
          Hashtbl.remove t.in_flight rid ;
          Hash_set.add t.fulfilled rid
      | None ->
          CLog.debug (fun m ->
              m "Already fulfilled = %b, %s" (Hash_set.mem t.fulfilled rid) rid) ;
          () )
    | Error _ ->
        CLog.debug (fun m ->
            m "Encountered an error while receiving from %s" id) ;
        () ) ;
    recv_loop endpoint
  in
  List.map t.endpoints ~f:recv_loop |> Lwt.join

let new_client ?(cid = Types.create_id ()) ~endpoints () =
  let context = Zmq.Context.create () in
  let endpoints =
    List.map endpoints ~f:(fun (id, addr) ->
        let open Zmq.Socket in
        let sock = create context dealer in
        set_identity sock cid ;
        connect sock ("tcp://" ^ addr) ;
        (id, sock))
  in
  let t =
    { cid
    ; context
    ; endpoints=
        List.map endpoints ~f:(fun (id, sock) ->
            (id, Zmq_lwt.Socket.of_socket sock))
    ; in_flight= Hashtbl.create (module String)
    ; fulfilled= Hash_set.create (module String) }
  in
  let ps =
    let%lwt () = recv_loop t in
    List.iter endpoints ~f:(fun (_, sock) -> Zmq.Socket.close sock) ;
    Zmq.Context.terminate context ;
    Lwt.return_unit
  in
  (t, ps)

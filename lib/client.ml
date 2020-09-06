open Types
open Messaging
open Lwt.Infix
module U = Unix_capnp_messaging

let ( >>>= ) = Lwt_result.bind

let client = Logs.Src.create "Client" ~doc:"Client module"

module Log = (val Logs.src_log client : Logs.LOG)

type send_fn = unit -> unit Lwt.t

type t =
  { mgr: U.Conn_manager.t
  ; addrs: int64 list
  ; send_stream: (send_fn * StateMachine.op_result Lwt.t) Lwt_stream.t
  ; push: (send_fn * StateMachine.op_result Lwt.t) option -> unit
  ; ongoing_requests: (int64, StateMachine.op_result Lwt.u) Hashtbl.t
  ; connection_retry: float }

let send t op =
  let id = Random.int32 Int32.max_int |> Int64.of_int32 in
  Log.debug (fun m -> m "Sending %a" Fmt.int64 id) ;
  let command : command = {op; id} in
  let msg = Serialise.clientRequest ~command in
  let prom, fulfiller = Lwt.wait () in
  let send () =
    List.map
      (fun addr ->
        Log.debug (fun m -> m "Sending to %a" Fmt.int64 addr) ;
        U.Conn_manager.send ~semantics:`AtMostOnce t.mgr addr msg
        >|= function
        | Ok () ->
            ()
        | Error exn ->
            Log.err (fun m -> m "Failed to send %a" Fmt.exn exn))
      t.addrs
    |> Lwt.choose
  in
  Hashtbl.add t.ongoing_requests id fulfiller ;
  t.push (Some (send, prom)) ;
  Lwt.on_failure (send ()) (fun _ -> ()) ;
  prom
  >>= function
  | StateMachine.Success ->
      Lwt.return_ok `Success
  | StateMachine.ReadSuccess v ->
      Lwt.return_ok (`ReadSuccess v)
  | StateMachine.Failure ->
      Lwt.return_error (`Msg "Application failed on cluster")

let fulfiller t _mgr src msg =
  t
  >>= fun t ->
  let handle msg =
    let open API.Reader in
    match
      msg |> Capnp.BytesMessage.Message.readonly |> ServerMessage.of_message
      |> ServerMessage.get
    with
    | ServerMessage.ClientResponse resp -> (
        let id = ClientResponse.id_get resp in
        match Hashtbl.find_opt t.ongoing_requests id with
        | Some fulfiller ->
            let res =
              match ClientResponse.get resp with
              | ClientResponse.Success ->
                  StateMachine.Success
              | ClientResponse.Failure ->
                  StateMachine.Failure
              | ClientResponse.ReadSuccess s ->
                  StateMachine.ReadSuccess s
              | Undefined d ->
                  Fmt.failwith "Got undefined client response %d" d
            in
            Log.debug (fun m -> m "Resolving %a" Fmt.int64 id) ;
            Hashtbl.remove t.ongoing_requests id ;
            Lwt.wakeup_later fulfiller res
        | None ->
            () )
    | _ ->
        ()
  in
  Log.debug (fun m -> m "Received response from %a" Fmt.int64 src) ;
  handle msg |> Lwt.return_ok

let send_wrapper t msg =
  Utils.catch (fun () -> send t msg)
  >|= function
  | Ok res ->
      res
  | Error e ->
      Error (`Msg (Fmt.str "Exception caught: %a" Fmt.exn e))

let op_read t k = send_wrapper t @@ StateMachine.Read (Bytes.to_string k)

let op_write t k v =
  send_wrapper t @@ StateMachine.Write (Bytes.to_string k, Bytes.to_string v)

let resend_iter t (send_fn, promise) =
  let rec loop () =
    let timeout () =
      let p_t () = Lwt_unix.sleep t.connection_retry in
      let catch exn =
        Log.debug (fun m -> m "Failed %a while sleeping" Fmt.exn exn) ;
        Lwt.return_unit
      in
      Lwt.catch p_t catch >>= Lwt.return_error
    in
    let promise = promise >>= fun _ -> Lwt.return_ok () in
    Lwt.choose [promise; timeout ()]
    >>= function
    | Error () ->
        Log.err (fun m -> m "Timed out while waiting for response") ;
        send_fn () >>= fun () -> loop ()
    | Ok () ->
        Lwt.return_unit
  in
  loop ()

let new_client ?(cid = Types.create_id ()) ?(connection_retry = 2.)
    ?(max_concurrency = 128) ?(client_port = Random.int 30768 + 10000) addresses
    () =
  let t_p, t_f = Lwt.wait () in
  let cmgr =
    U.Conn_manager.create
      ~listen_address:(TCP ("0.0.0.0", client_port))
      ~node_id:cid (fulfiller t_p)
  in
  let ps =
    List.map
      (fun (id, addr) ->
        U.Conn_manager.add_outgoing cmgr id addr (`Persistant addr))
      addresses
  in
  (* get at least once connection established *)
  Lwt.choose ps
  >>= fun () ->
  let send_stream, push = Lwt_stream.create () in
  let t =
    { mgr= cmgr
    ; addrs= List.map fst addresses
    ; send_stream
    ; push
    ; ongoing_requests= Hashtbl.create 1024
    ; connection_retry }
  in
  Lwt.wakeup t_f t ;
  Lwt.async (fun () ->
      Lwt_stream.iter_n ~max_concurrency (resend_iter t) send_stream) ;
  Lwt.return t

exception Closed

let close t =
  U.Conn_manager.close t.mgr
  >>= fun () ->
  t.push None ;
  Hashtbl.iter (fun _ f -> Lwt.wakeup_exn f Closed) t.ongoing_requests ;
  Lwt.return_unit

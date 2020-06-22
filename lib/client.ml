open Types
open Messaging
open Lwt.Infix
open Unix_capnp_messaging

let ( >>>= ) = Lwt_result.bind

let client = Logs.Src.create "Client" ~doc:"Client module"

module Log = (val Logs.src_log client : Logs.LOG)

type send_fn = unit -> unit

type t =
  { mgr: Conn_manager.t
  ; addrs: int64 list
  ; send_stream: (send_fn * StateMachine.op_result Lwt.t) Lwt_stream.t
  ; push: (send_fn * StateMachine.op_result Lwt.t) option -> unit
  ; ongoing_requests: (int, StateMachine.op_result Lwt.u) Hashtbl.t
  ; connection_retry: float }

let send t op =
  let id = Random.int32 Int32.max_int |> Int32.to_int in
  Log.debug (fun m -> m "Sending %d" id);
  let command : command = {op; id} in
  let msg = Send.Serialise.clientRequest ~command in
  let prom, fulfiller = Lwt.wait () in
  let send () =
    List.iter
      (fun addr ->
        let async () =
          Log.debug (fun m -> m "Sending to %a" Fmt.int64 addr) ;
          Conn_manager.send t.mgr addr msg
          >|= function
          | Ok () ->
              ()
          | Error exn ->
              Log.err (fun m -> m "Failed to send %a" Fmt.exn exn)
        in
        Lwt.async async)
      t.addrs
  in
  Hashtbl.add t.ongoing_requests id fulfiller ;
  t.push (Some (send, prom)) ;
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
        let id = ClientResponse.id_get_int_exn resp in
        match Hashtbl.find_opt t.ongoing_requests id with
        | Some fulfiller ->
            let res =
              match ClientResponse.result_get resp |> CommandResult.get with
              | CommandResult.Success ->
                  StateMachine.Success
              | CommandResult.Failure ->
                  StateMachine.Failure
              | CommandResult.ReadSuccess s ->
                  StateMachine.ReadSuccess s
              | Undefined d ->
                  Fmt.failwith "Got undefined client response %d" d
            in
            Hashtbl.remove t.ongoing_requests id ;
            Lwt.wakeup fulfiller res
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
    send_fn () ;
    let timeout =
      Lwt_unix.sleep t.connection_retry >>= fun () -> Lwt.return_error ()
    in
    let promise = promise >>= fun _ -> Lwt.return_ok () in
    Lwt.choose [timeout; promise]
    >>= function Error () -> loop () | Ok () -> Lwt.return_unit
  in
  loop ()

let new_client ?(cid = Types.create_id ()) ?(connection_retry = 1.)
    ?(max_concurrency = 1024) ?(client_port = 8080) addresses () =
  let t_p, t_f = Lwt.task () in
  let cmgr =
    Conn_manager.create
      ~listen_address:(TCP ("0.0.0.0", client_port))
      ~node_id:cid (fulfiller t_p)
  in
  let ps =
    List.map
      (fun (id, addr) ->
        Conn_manager.add_outgoing cmgr id addr (`Persistant addr))
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
  Lwt.wakeup t_f t;
  Lwt.async (fun () ->
      Lwt_stream.iter_n ~max_concurrency (resend_iter t) send_stream) ;
  Lwt.return t

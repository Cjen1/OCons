open Types
open Messaging
open Lwt.Infix

let ( >>>= ) = Lwt_result.bind

let client = Logs.Src.create "Client" ~doc:"Client module"

module Log = (val Logs.src_log client : Logs.LOG)

type send_fn = unit -> unit

type t =
  { mgr: ClientConn.t
  ; addrs: address list
  ; send_stream: (send_fn * StateMachine.op_result Lwt.t) Lwt_stream.t
  ; push: (send_fn * StateMachine.op_result Lwt.t) option -> unit
  ; ongoing_requests: (int, StateMachine.op_result Lwt.u) Hashtbl.t
  ; connection_retry: float }

let send t op =
  let id = Random.int32 Int32.max_int |> Int32.to_int in
  let command : command = {op; id} in
  let msg = Send.Serialise.clientRequest ~command in
  let prom, fulfiller = Lwt.wait () in
  let send () =
    List.iter
      (fun addr ->
        Lwt.async (fun () ->
            Log.debug (fun m -> m "Sending to %a" ConnUtils.pp_addr addr) ;
            ClientConn.send t.mgr addr msg))
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

let fulfiller_loop t () =
  let resp msg =
    let open API.Reader in
    match API.Reader.ServerMessage.get msg with
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
  let rec loop () =
    ClientConn.recv t.mgr
    >>= fun msg ->
    Log.debug (fun m -> m "waiting to recieve") ;
    resp msg ;
    Log.debug (fun m -> m "received") ;
    loop ()
  in
  loop ()

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
    send_fn ();
    let timeout = Lwt_unix.sleep t.connection_retry >>= fun () -> Lwt.return_error () in
    let promise = promise >>= fun _ -> Lwt.return_ok () in
    Lwt.choose [timeout; promise] >>= function
    | Error () -> loop ()
    | Ok () -> Lwt.return_unit
  in
  loop ()

let new_client ?(cid = Types.create_id ()) ?(connection_retry = 1.) ?(max_concurrency=1024) addresses ()
    =
  let clientmgr = ClientConn.create ~id:cid () in
  let ps = List.map (ClientConn.add_connection clientmgr) addresses in
  (* get at least once connection established *)
  Lwt.choose ps
  >>= fun () ->
  let send_stream, push = Lwt_stream.create () in
  let t =
    { mgr= clientmgr
    ; addrs= addresses
    ; send_stream
    ; push
    ; ongoing_requests= Hashtbl.create 1024
    ; connection_retry }
  in
  Lwt.async (fulfiller_loop t) ;
  Lwt.async (fun () -> Lwt_stream.iter_n ~max_concurrency (resend_iter t) send_stream) ;
  Lwt.return t

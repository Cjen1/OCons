open! Core
open! Async
open! Utils
open! Ppx_log_async
module PC = Async_rpc_kernel.Persistent_connection

let logger =
  let open Async_unix.Log in
  create ~level:`Info ~output:[] ~on_error:`Raise
    ~transform:(fun m -> Message.add_tags m [("src", "Client")])
    ()

type t = {conns: PC.Rpc.t list; retry_delay: Time.Span.t}

let get_current_conns t = List.filter_map t.conns ~f:PC.Rpc.current_connection

let send t op =
  let retry_loop () =
    let conns = get_current_conns t in
    (* For each conn send out request
       match response with
       | Ok (Ok v) -> return v
       | Ok (Error `Unapplied) -> do nothing
       | Error e ->
                log error;
                do nothing
    *)
    let reqs =
      List.map conns ~f:(fun conn ->
          match%map Rpc.Rpc.dispatch Types.client_rpc conn op with
          | Ok (Ok v) ->
              Some (`Finished v)
          | Ok (Error `Unapplied) ->
              None
          | Error e ->
              [%log.error
                logger "Error while dispatching request" (e : Error.t)] ;
              None )
    in
    let p_timeout =
      after t.retry_delay >>= fun () -> return @@ Some (`Repeat ())
    in
    let ivar = Ivar.create () in
    List.iter (p_timeout :: reqs) ~f:(fun p ->
        upon p (function Some v -> Ivar.fill_if_empty ivar v | None -> ()) ) ;
    let%bind value = Ivar.read ivar in
    let () =
      match value with
      | `Repeat _ ->
          [%log.debug logger "Timed out repeating" (List.length reqs : int)]
      | `Finished _ ->
          ()
    in
    return value
  in
  Deferred.repeat_until_finished () retry_loop

let op_read t k = send t Types.{op= Read (Bytes.to_string k); id= Id.create ()}

let op_write t ~k ~v =
  send t
    Types.{op= Write (Bytes.to_string k, Bytes.to_string v); id= Id.create ()}

let new_client ?(retry_delay = Time.Span.of_sec 1.) addresses =
  let conns = List.map addresses ~f:connect_persist in
  {conns; retry_delay}

let close t = Deferred.List.iter ~how:`Parallel t.conns ~f:PC.Rpc.close

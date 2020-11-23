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

type t =
  {conns: PC.Rpc.t list; connection_retry: Time.Span.t; retry_delay: Time.Span.t}

let send =
  let dispatch op ivar t ongoing conn =
    let req =
      match PC.Rpc.current_connection conn with
      | Some conn ->
          Rpc.Rpc.dispatch Types.RPCs.client_request conn op
      | None ->
          let%bind conn = PC.Rpc.connected conn in
          Rpc.Rpc.dispatch Types.RPCs.client_request conn op
    in
    upon req (function
      | Ok (Ok (Types.Success as v)) ->
          Ivar.fill_if_empty ivar (`Finished v)
      | Ok (Ok (Types.ReadSuccess _ as v)) ->
          Ivar.fill_if_empty ivar (`Finished v)
      | Ok (Ok (Types.Failure as v)) ->
          Ivar.fill_if_empty ivar (`Finished v)
      | Ok (Error `Unapplied) ->
          decr ongoing ;
          if !ongoing <= 0 then Ivar.fill_if_empty ivar (`Repeat (op, t))
      | Error e ->
          [%log.error logger "Error while dispatching request" (e : Error.t)] ;
          decr ongoing ;
          if !ongoing <= 0 then Ivar.fill_if_empty ivar (`Repeat (op, t)))
  in
  let handle_ivar op t i =
    let ongoing = ref @@ List.length t.conns in
    List.iter t.conns ~f:(dispatch op i t ongoing)
  in
  let repeater (op, t) = Deferred.create (handle_ivar op t) in
  fun t op -> Deferred.repeat_until_finished (op, t) repeater

let op_read t k = send t Types.{op= Read (Bytes.to_string k); id= Id.create ()}

let op_write t ~k ~v =
  send t
    Types.{op= Write (Bytes.to_string k, Bytes.to_string v); id= Id.create ()}

let new_client ?(connection_retry = Time.Span.of_sec 1.)
    ?(retry_delay = Time.Span.of_sec 1.) addresses =
  let conns = List.map addresses ~f:connect_persist in
  {conns; connection_retry; retry_delay}

let close t = Deferred.List.iter ~how:`Parallel t.conns ~f:PC.Rpc.close

open! Core
open! Async
open! Utils
module PC = Async_rpc_kernel.Persistent_connection

let client = Logs.Src.create "Client" ~doc:"Client module"

module Log = (val Logs.src_log client : Logs.LOG)

type t =
  {conns: PC.Rpc.t list; connection_retry: Time.Span.t; retry_delay: Time.Span.t}

let send =
  let inner (t, op) =
    let reqs =
      Deferred.List.filter_map ~how:`Parallel t.conns ~f:(fun conn ->
          let%bind conn = PC.Rpc.connected conn in
          match%bind Rpc.Rpc.dispatch Types.RPCs.client_request conn op with
          | Ok Types.Failure ->
              None |> return
          | Ok v ->
              Some v |> return
          | Error _ ->
              assert false)
    in
    match%bind reqs with
    | [] ->
        let%bind () = after t.retry_delay in
        `Repeat (t, op) |> return
    | res :: _ ->
        `Finished res |> return
  in
  fun t op -> Deferred.repeat_until_finished (t, op) inner

let op_read t k = send t Types.{op= Read (Bytes.to_string k); id= Id.create ()}

let op_write t k v =
  send t
    Types.{op= Write (Bytes.to_string k, Bytes.to_string v); id= Id.create ()}

let new_client ?(connection_retry = Time.Span.of_sec 1.)
    ?(retry_delay = Time.Span.of_sec 1.) addresses =
  let conns = List.map addresses ~f:connect_persist in
  {conns; connection_retry; retry_delay}

let close t = Deferred.List.iter ~how:`Parallel t.conns ~f:PC.Rpc.close

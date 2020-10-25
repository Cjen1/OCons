open! Core
open! Async
open! Utils
module PC = Async_rpc_kernel.Persistent_connection

let client = Logs.Src.create "Client" ~doc:"Client module"

module Log = (val Logs.src_log client : Logs.LOG)

type t =
  {conns: PC.Rpc.t list; connection_retry: Time.Span.t; retry_delay: Time.Span.t}

let choose_n_split ps =
  let%bind _ = Deferred.any ps in
  let resolved =
    List.filter ps ~f:Deferred.is_determined |> List.map ~f:Deferred.value_exn
  in
  let unresolved =
    List.filter ps ~f:(fun v -> v |> Deferred.is_determined |> not)
  in
  return (resolved, unresolved)

let send =
  let main (t, op) =
    let reqs =
      List.map t.conns ~f:(fun conn ->
          let%bind conn = PC.Rpc.connected conn in
          Rpc.Rpc.dispatch Types.RPCs.client_request conn op)
    in
    let rec inner rem =
      let%bind res, unres = choose_n_split rem in
      let res =
        List.filter res ~f:(function
          | Ok Types.Success | Ok (Types.ReadSuccess _) ->
              true
          | Ok Types.Failure ->
              false
          | Error e ->
              Log.err (fun m ->
                  m "Err while dispatching request: %s" (Error.to_string_hum e)) ;
              false)
      in
      match (res, unres) with
      | x :: _, _ ->
          `Finished x |> return
      | [], [] ->
          let%bind () = after t.retry_delay in
          `Repeat (t, op) |> return
      | [], rem ->
          inner rem
    in
    inner reqs
  in
  fun t op -> Deferred.repeat_until_finished (t, op) main

let op_read t k = send t Types.{op= Read (Bytes.to_string k); id= Id.create ()}

let op_write t k v =
  send t
    Types.{op= Write (Bytes.to_string k, Bytes.to_string v); id= Id.create ()}

let new_client ?(connection_retry = Time.Span.of_sec 1.)
    ?(retry_delay = Time.Span.of_sec 1.) addresses =
  let conns = List.map addresses ~f:connect_persist in
  {conns; connection_retry; retry_delay}

let close t = Deferred.List.iter ~how:`Parallel t.conns ~f:PC.Rpc.close

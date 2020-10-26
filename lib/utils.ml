open! Core
open! Async

let src = Logs.Src.create "Utils" ~doc:"Utils"

module Log = (val Logs.src_log src : Logs.LOG)

module Quorum = struct
  type 'a t = {elts: 'a list; n: int; threshold: int; eq: 'a -> 'a -> bool}
  [@@deriving sexp_of]

  let empty threshold eq = {elts= []; n= 0; threshold; eq}

  let add v t =
    if List.mem t.elts v ~equal:t.eq then Error `AlreadyInList
    else Ok {t with elts= v :: t.elts; n= t.n + 1}

  let satisified t = t.n >= t.threshold
end

module Batcher = struct
  type 'a t =
    { limit: 'a list -> bool
    ; dispatch_timeout: Time.Span.t
    ; callback: 'a list -> unit Deferred.t
    ; mutable current_jobs: 'a list
    ; mutable dispatched: bool }

  let perform t =
    let jobs = t.current_jobs in
    t.current_jobs <- [] ;
    t.dispatched <- false ;
    t.callback jobs

  let dispatch t v =
    t.current_jobs <- v :: t.current_jobs ;
    match () with
    | () when t.limit t.current_jobs ->
        don't_wait_for @@ perform t
    | () when not t.dispatched ->
        t.dispatched <- true ;
        let p =
          (*schedule' ~priority:Priority.low @@ fun () ->*)
          let%bind () = at Time.(add (now ()) t.dispatch_timeout) in
          perform t
        in
        p |> don't_wait_for
    | () ->
        ()

  let create ~f ~dispatch_timeout ~limit =
    {callback= f; dispatch_timeout; limit; current_jobs= []; dispatched= false}

  let create_counter ~f ~dispatch_timeout ~limit =
    let counter = ref 0 in
    let f xs =
      counter := 0 ;
      f xs
    in
    create ~f ~dispatch_timeout ~limit:(fun _ ->
        incr counter ; !counter > limit)
end

let connect_persist ?(retry_delay = Time_ns.Span.of_sec 1.) name =
  let server_address = Host_and_port.of_string name in
  Async_rpc_kernel.Persistent_connection.Rpc.create
    ~retry_delay:(fun () -> retry_delay)
    ~server_name:name
    ~connect:(fun host_and_port ->
      let%bind conn =
        Rpc.Connection.client
          (Tcp.Where_to_connect.of_host_and_port host_and_port)
      in
      match conn with
      | Ok v ->
          Ok v |> return
      | Error exn ->
          Error (Error.of_exn exn) |> return)
    (fun () -> Ok server_address |> return)

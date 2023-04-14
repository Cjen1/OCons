open Eio
open! Util

type conn_state = Open of {w: Buf_write.t; r: Buf_read.t} | Closed

type resolver = Switch.t -> Eio.Flow.two_way

type t =
  { mutable conn_state: conn_state
  ; has_failed_cond: Condition.t
  ; has_recovered_cond: Condition.t
  ; mutable should_close: bool
  ; closed_promise: unit Promise.t * unit Promise.u }

let close_inflight t =
  t.conn_state <- Closed ;
  Condition.broadcast t.has_failed_cond ;
  Fiber.yield () (* Allow reconnect if possible *)

let print_parse_exn e =
  dtraceln "Failed operation: %a" Fmt.exn_backtrace
    (e, Printexc.get_raw_backtrace ()) ;
  dtraceln "Callstack: %a" Fmt.exn_backtrace (e, Printexc.get_callstack 4)

(* If default is not set, waits until the conn is open *)
let rec do_if_open ?default t f =
  match (t.conn_state, default) with
  | Closed, Some a ->
      dtraceln "do_if_open: closed" ;
      a
  | Closed, None ->
      dtraceln "do_if_open: closed, retry" ;
      Condition.await_no_mutex t.has_recovered_cond ;
      do_if_open ?default t f
  | Open c, _ -> (
    try
      dtraceln "do_if_open: read" ;
      f (c.w, c.r)
    with e when Util.is_not_cancel e ->
      print_parse_exn e ; close_inflight t ; do_if_open ?default t f )

let send_blit ?(block_until_open = false) t bf =
  let write (w, _) = bf w in
  match block_until_open with
  | true ->
      do_if_open t write
  | false ->
      do_if_open ~default:() t write |> ignore

let send ?(block_until_open = false) t cs =
  send_blit ~block_until_open t (fun b -> Buf_write.cstruct b cs)

let recv ?default t parse =
  let read (_, r) = parse r in
  do_if_open ?default t read

let recv_iter t val_parse f =
  (* Reconnect loop *)
  let yielder = Util.maybe_yield ~energy:128 in
  let parse =
    let open Buf_read in
    let open Syntax in
    let* b = at_end_of_input in
    if b then return None
    else
      let* v = val_parse in
      return (Some v)
  in
  let open struct
    exception EOF
  end in
  while not t.should_close do
    Fiber.check () ;
    try
      (* Recv loop *)
      while not t.should_close do
        Fiber.check () ;
        match t.conn_state with
        | Closed ->
            dtraceln "recv_iter: closed retrying" ;
            Condition.await_no_mutex t.has_recovered_cond
        | Open {r; _} -> (
          match parse r with
          | None ->
              raise EOF
          | Some v ->
              dtraceln "recv_iter: read" ; f v ; yielder () )
      done
    with
    | EOF ->
        close_inflight t
    | e when Util.is_not_cancel e ->
        traceln "Failed while using conn: %a" Fmt.exn_backtrace
          (e, Printexc.get_raw_backtrace ()) ;
        close_inflight t
  done

let close t =
  t.should_close <- true ;
  close_inflight t ;
  Promise.await (fst t.closed_promise)

let flush t = do_if_open ~default:() t (fun (w, _) -> Buf_write.flush w)

let is_open t = not t.should_close

let switch_run ~on_error f =
  try Switch.run f with e when is_not_cancel e -> on_error e

let create ?connected ~sw (f : Switch.t -> Flow.two_way) delayer =
  let t =
    { conn_state= Closed
    ; should_close= false
    ; has_failed_cond= Condition.create ()
    ; has_recovered_cond= Condition.create ()
    ; closed_promise= Promise.create () }
  in
  let on_error e = dtraceln "Connection failed with\n%a" Fmt.exn e in
  let connect_handler () =
    dtraceln "connect_handler: entry" ;
    (* Continually retry the connection until it connects *)
    while not t.should_close do
      dtraceln "connect_handler: retry_closed" ;
      Fiber.yield () ;
      delayer () ;
      switch_run ~on_error (fun sw ->
          let flow = f sw in
          Buf_write.with_flow flow (fun w ->
              let r = Buf_read.of_flow flow ~max_size:1_000_000 in
              Fiber.check () ;
              t.conn_state <- Open {w; r} ;
              (* Notify upwards of connection status *)
              Option.iter
                (fun (p, u) ->
                  if not (Promise.is_resolved p) then Promise.resolve u () )
                connected ;
              Condition.broadcast t.has_recovered_cond ;
              dtraceln "Connection now open" ;
              while
                not
                  ( t.should_close
                  || match t.conn_state with Closed -> true | _ -> false )
              do
                dtraceln "connect_handler: await_closed" ;
                Condition.await_no_mutex t.has_failed_cond
              done ) ;
          dtraceln "Finished with conn" )
    done ;
    Promise.resolve (snd t.closed_promise) ()
  in
  Fiber.fork ~sw connect_handler ;
  Switch.on_release sw (fun () -> close t) ;
  t

let%expect_test "PersistantConn" =
  Eio_main.run
  @@ fun _ ->
  Switch.run
  @@ fun sw ->
  let f = ref @@ Eio_mock.Flow.make "PConn" in
  Eio_mock.Flow.on_read !f
    [ `Return "1\n"
    ; `Return "2\n"
    ; `Raise End_of_file
    ; `Raise (Failure "err")
    ; `Return "3\n"
    ; `Return "4\n"
    ; `Raise End_of_file ] ;
  let c = create ~sw (fun _ -> (!f :> Eio.Flow.two_way)) (fun () -> ()) in
  let p_line = Buf_read.line in
  print_endline (recv c p_line) ;
  [%expect {|
      +PConn: read "1\n"
      1|}] ;
  print_endline (recv c p_line) ;
  [%expect {|
      +PConn: read "2\n"
      2|}] ;
  print_endline (recv c p_line) ;
  [%expect {|
      +PConn: read "3\n"
      3|}] ;
  print_endline (recv c p_line) ;
  [%expect {|
    +PConn: read "4\n"
    4 |}] ;
  send c (Cstruct.of_string "1") ;
  send c (Cstruct.of_string "2") ;
  send c (Cstruct.of_string "3") ;
  send c (Cstruct.of_string "4") ;
  flush c ;
  [%expect {| +PConn: wrote "1234" |}] ;
  close c

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
  if e <> End_of_file then (
    dtraceln "Failed operation: %a" Fmt.exn_backtrace
      (e, Printexc.get_raw_backtrace ()) ;
    dtraceln "Callstack: %a" Fmt.exn_backtrace (e, Printexc.get_callstack 4) )

(* If default is not set, waits until the conn is open *)
let rec do_if_open ?default t f =
  match (t.conn_state, default) with
  | Closed, Some a ->
      a
  | Closed, None ->
      Condition.await_no_mutex t.has_recovered_cond ;
      do_if_open ?default t f
  | Open c, _ -> (
    try f (c.w, c.r)
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

let recv_iter t parse f =
  (* Reconnect loop *)
  let yielder = Util.maybe_yield ~energy:128 in
  while not t.should_close do
    match t.conn_state with
    | Open {r; _} -> (
      (* TODO avoid alloc of seq *)
      try r |> Buf_read.seq parse |> Seq.iter (fun v -> f v ; yielder ())
      with e when Util.is_not_cancel e -> print_parse_exn e ; close_inflight t )
    | Closed ->
        Condition.await_no_mutex t.has_recovered_cond
  done

let close t =
  t.should_close <- true ;
  close_inflight t ;
  Promise.await (fst t.closed_promise)

let flush t =
  match t.conn_state with Open s -> Buf_write.flush s.w | Closed -> ()

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
    (* Continually retry the connection until it connects *)
    while not t.should_close do
      Fiber.check () ;
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
                Condition.await_no_mutex t.has_failed_cond
              done ) ;
          dtraceln "Finished with conn" ) ;
      Fiber.yield () ;
      delayer ()
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
    ; `Return "3\n"
    ; `Return "4\n"
    ; `Raise End_of_file ] ;
  let c = create ~sw (fun _ -> (!f :> Eio.Flow.two_way)) (fun () -> ()) in
  let p_line = Buf_read.line in
  print_endline (recv c p_line) ;
  [%expect {|
      +Connection now open
      +PConn: read "1\n"
      1|}] ;
  print_endline (recv c p_line) ;
  [%expect {|
      +PConn: read "2\n"
      2|}] ;
  print_endline (recv c p_line) ;
  [%expect
    {|
      (* CR expect_test_collector: This test expectation appears to contain a backtrace.
         This is strongly discouraged as backtraces are fragile.
         Please change this test to not include a backtrace. *)

      +Failed operation: Exception: End_of_file
      +                  Raised at Eio__Buf_read.ensure_slow_path in file "vendor/eio/lib_eio/buf_read.ml", line 126, characters 6-23
      +                  Called from Eio__Buf_read.ensure in file "vendor/eio/lib_eio/buf_read.ml" (inlined), line 129, characters 20-40
      +                  Called from Eio__Buf_read.line.aux in file "vendor/eio/lib_eio/buf_read.ml", line 355, characters 6-26
      +                  Called from Eio__Buf_read.line in file "vendor/eio/lib_eio/buf_read.ml", line 363, characters 8-13
      +                  Called from Ocons_conn_mgr__Persistent_conn.do_if_open in file "ocons_conn_mgr/persistent_conn.ml", line 29, characters 8-20
      +Callstack: Exception: End_of_file
      +           Raised by primitive operation at Ocons_conn_mgr__Persistent_conn.do_if_open in file "ocons_conn_mgr/persistent_conn.ml", line 33, characters 53-77
      +           Called from Ocons_conn_mgr__Persistent_conn.(fun) in file "ocons_conn_mgr/persistent_conn.ml", line 134, characters 16-31
      +           Called from Eio__core__Switch.run_internal in file "vendor/eio/lib_eio/core/switch.ml", line 132, characters 8-12
      +           Called from Eio__core__Cancel.with_cc in file "vendor/eio/lib_eio/core/cancel.ml", line 116, characters 8-12
      +Finished with conn
      +Connection now open
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
  close c ;
  [%expect {| +Finished with conn |}]

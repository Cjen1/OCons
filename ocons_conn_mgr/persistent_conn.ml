open Eio

type conn_state = Open of {w: Buf_write.t; r: Buf_read.t} | Closed

type t =
  { f: unit -> Flow.two_way
  ; mutable conn_state: conn_state
  ; mutable encountered_failure: bool
  ; has_failed_cond: Condition.t
  ; has_recovered_cond: Condition.t
  ; mutable should_close: bool
  ; closed_promise: unit Promise.t * unit Promise.u }

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
    with End_of_file ->
      t.encountered_failure <- true ;
      Condition.broadcast t.has_failed_cond ;
      Fiber.yield () ;
      do_if_open ?default t f )

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

let close t =
  t.should_close <- true ;
  Condition.broadcast t.has_failed_cond ;
  Promise.await (fst t.closed_promise)

let flush t =
  match t.conn_state with Open s -> Buf_write.flush s.w | Closed -> ()

let is_open t = not t.should_close

let create ~sw (f : unit -> Flow.two_way) =
  let t =
    { f
    ; conn_state= Closed
    ; should_close= false
    ; encountered_failure= false
    ; has_failed_cond= Condition.create ()
    ; has_recovered_cond= Condition.create ()
    ; closed_promise= Promise.create () }
  in
  Fiber.fork ~sw (fun () ->
      while not t.should_close do
        Fiber.check ();
        let flow = f () in
        Buf_write.with_flow flow
        @@ fun w ->
        let r = Buf_read.of_flow flow ~max_size:1_000_000 in
        t.conn_state <- Open {w; r} ;
        t.encountered_failure <- false ;
        while not (t.encountered_failure || t.should_close) do
          Condition.await_no_mutex t.has_failed_cond
        done
      done ;
      Promise.resolve (snd t.closed_promise) () ) ;
  Switch.on_release sw (fun () -> close t) ;
  Fiber.yield () ;
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
  let c = create ~sw (fun () -> (!f :> Eio.Flow.two_way)) in
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

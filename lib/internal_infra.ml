open Eio.Std
open Types
module CMgr = Ocons_conn_mgr

module Ticker = struct
  type t = {mutable next_tick: float; period: float; clock: Eio.Time.clock}

  let create clock period =
    {next_tick= Core.Float.(Eio.Time.now clock + period); period; clock}

  let tick t f =
    let now = Eio.Time.now t.clock in
    if now > t.next_tick then (
      t.next_tick <- Core.Float.(now + t.period) ;
      f () )
end

module Make (C : Consensus_intf.S) = struct
  type t =
    { c_rx: command Eio.Stream.t
    ; c_tx: (command_id * op_result) Eio.Stream.t
    ; cmgr: C.message Ocons_conn_mgr.t
    ; state_machine: state_machine
    ; inflight_txns: command_id Core.Hash_set.t
    ; mutable cons: C.t
    ; ticker: Ticker.t
    ; mutable should_close: bool
    ; closed_p: unit Promise.t * unit Promise.u }

  let apply (t : t) (cmd : command) : op_result =
    Core.Hash_set.remove t.inflight_txns cmd.id ;
    update_state_machine t.state_machine cmd

  let handle_actions t actions =
    (* TODO move over to blit interface *)
    let f : C.action -> unit = function
      | C.Send (dst, msg) ->
          traceln "sending" ;
          CMgr.send_blit t.cmgr dst (C.serialise msg) ;
          traceln "sent"
      | C.Broadcast msg ->
          CMgr.broadcast_blit t.cmgr (C.serialise msg)
      | C.CommitCommands citer ->
          citer (fun cmd ->
              let res = apply t cmd in
              Eio.Stream.add t.c_tx (cmd.id, res) )
    in
    List.iter f actions

  (** If any msgs to internal port exist then read and apply them *)
  let internal_msgs t =
    let msg_iter = CMgr.recv_any t.cmgr in
    let iterf (src, msg) =
      let tcons, actions = C.advance t.cons (Recv (msg, src)) in
      t.cons <- tcons ;
      handle_actions t actions
    in
    msg_iter iterf

  (** Recv client msgs *)
  let admit_client_requests t =
    let num_to_take =
      min (C.available_space_for_commands t.cons) (Eio.Stream.length t.c_rx)
    in
    if num_to_take <= 0 then ()
    else
      let iter f =
        for _ = 1 to num_to_take do
          let v = Eio.Stream.take t.c_rx in
          if Core.Hash_set.mem t.inflight_txns v.id |> not then (
            Core.Hash_set.add t.inflight_txns v.id ;
            f v )
        done
      in
      let tcons, actions = C.advance t.cons (Commands iter) in
      t.cons <- tcons ;
      handle_actions t actions

  let ensure_sent t = Fiber.yield () ; CMgr.flush_all t.cmgr

  let tick t () =
    let tcons, actions = C.advance t.cons Tick in
    t.cons <- tcons ;
    handle_actions t actions

  let main_loop t =
    (* Do internal mostly non-blocking things *)
    internal_msgs t ;
    admit_client_requests t ;
    Ticker.tick t.ticker (tick t) ;
    (* Then block to send all things *)
    ensure_sent t

  let create ~sw config clock period resolvers client_msgs client_resps =
    let t_p, t_u = Promise.create () in
    Fiber.fork ~sw (fun () ->
        Switch.run
        @@ fun sw ->
        let cmgr = Ocons_conn_mgr.create ~sw resolvers C.parse in
        let cons = C.create_node config in
        let state_machine = Core.Hashtbl.create (module Core.String) in
        let ticker = Ticker.create (clock :> Eio.Time.clock) period in
        let t =
          { c_tx= client_resps
          ; c_rx= client_msgs
          ; cmgr
          ; cons
          ; state_machine
          ; inflight_txns= Core.Hash_set.create (module Core.Int)
          ; ticker
          ; should_close= false
          ; closed_p= Promise.create () }
        in
        Promise.resolve t_u t ;
        while not t.should_close do
          Fiber.check () ; main_loop t
        done ;
        Ocons_conn_mgr.close t.cmgr ;
        Promise.resolve (snd t.closed_p) () ) ;
    Promise.await t_p

  let close t =
    t.should_close <- true ;
    Promise.await (fst t.closed_p)
end

module Test = struct
  let w k n = Command.{op= Write (k, n); id= Core.Random.int Core.Int.max_value}

  let r n = Command.{op= Read n; id= Core.Random.int Core.Int.max_value}

  module CT = struct
    type message = Core.String.t [@@deriving sexp]

    let message_pp = Fmt.string

    (** All the events incomming into the advance function *)
    type event =
      | Tick
      | Recv of (message * node_id)
      | Commands of command Iter.t

    let event_pp ppf v =
      let open Fmt in
      match v with
      | Tick ->
          pf ppf "Tick"
      | Recv (m, src) ->
          pf ppf "Recv(%a, %d)" message_pp m src
      | Commands _ ->
          pf ppf "Commands"

    type action =
      | Send of int * message
      | Broadcast of message
      | CommitCommands of command Iter.t

    let action_pp ppf v =
      let open Fmt in
      match v with
      | Send (d, m) ->
          pf ppf "Send(%d, %a)" d message_pp m
      | Broadcast m ->
          pf ppf "Broadcast(%a)" message_pp m
      | CommitCommands _ ->
          pf ppf "CommitCommands"

    let parse buf =
      let r = Eio.Buf_read.line buf in
      traceln "read %s" r ; r

    let serialise v buf =
      Eio.Buf_write.string buf v ;
      Eio.Buf_write.char buf '\n'

    type config = Core.Unit.t [@@deriving sexp_of]

    let config_pp : config Fmt.t = Fmt.any "config"

    type t = {arr: command option Array.t; mutable len: int}

    let t_pp ppf _ = Fmt.pf ppf "T"

    let create_node () = {arr= Array.make 10 None; len= 0}

    let available_space_for_commands _t = 5

    let advance t e =
      match e with
      | Tick ->
          (t, [CommitCommands (fun f -> f (r "Tick"))])
      | Recv (c, s) ->
          (t, [CommitCommands (fun f -> f (w c c)); Send (s, c)])
      | Commands ci ->
          Iter.iteri
            (fun i c ->
              Array.set t.arr i (Some c) ;
              t.len <- t.len + 1 )
            ci ;
          let actions =
            [ CommitCommands
                (fun f ->
                  for i = 0 to t.len - 1 do
                    let v = Array.get t.arr i in
                    f (Option.get v)
                  done ) ]
          in
          (t, actions)
  end

  include Make (CT)

  let%expect_test "" =
    Eio_mock.Backend.run
    @@ fun () ->
    let clk = Eio_mock.Clock.make () in
    Switch.run
    @@ fun sw ->
    let c_rx = Eio.Stream.create 10 in
    let c_tx = Eio.Stream.create 10 in
    let f1 = Eio_mock.Flow.make "f1" in
    let resolvers = [(1, fun () -> (f1 :> Eio.Flow.two_way))] in
    (* Start of actual test *)
    let t = create ~sw () (clk :> Eio.Time.clock) 1. resolvers c_rx c_tx in
    Fiber.yield () ;
    (* No errors or msgs on startup *)
    [%expect {||}] ;
    (* Can take remote messages *)
    Eio_mock.Flow.on_read f1 [`Return "1\n"] ;
    Fiber.yield () ;
    Fiber.yield () ;
    traceln "Read from stream: %a"
      (Fmt.option ~none:(Fmt.any "None") Types.op_result_pp)
      (Option.map snd @@ Eio.Stream.take_nonblocking c_tx) ;
    traceln "Read from stream: %a"
      (Fmt.option ~none:(Fmt.any "None") Types.op_result_pp)
      (Option.map snd @@ Eio.Stream.take_nonblocking c_tx) ;
    Fiber.yield () ;
    [%expect
      {|
      +f1: read "1\n"
      +read 1
      +sending
      +sent
      +Read from stream: Success
      +Read from stream: None
      +f1: wrote "1\n" |}] ;
    (* Can take client requests successfully *)
    Eio.Stream.add c_rx (w "1" "1") ;
    Eio.Stream.add c_rx (r "1") ;
    Eio.Stream.add c_rx (r "2") ;
    Eio.Stream.add c_rx (w "Tick" "Tick") ;
    Fiber.yield () ;
    for _ = 0 to 4 do
      traceln "Read from stream: %a"
        (Fmt.option ~none:(Fmt.any "None") Types.op_result_pp)
        (Option.map snd @@ Eio.Stream.take_nonblocking c_tx)
    done ;
    [%expect
      {|
      +Read from stream: Success
      +Read from stream: ReadSuccess(1)
      +Read from stream: Failure(Key not found)
      +Read from stream: Success
      +Read from stream: None |}] ;
    (* Can tick *)
    Eio_mock.Clock.set_time clk 2. ;
    Fiber.yield () ;
    for _ = 1 to 2 do
      traceln "Read from stream: %a"
        (Fmt.option ~none:(Fmt.any "None") Types.op_result_pp)
        (Option.map snd @@ Eio.Stream.take_nonblocking c_tx)
    done ;
    [%expect
      {|
      +mock time is now 2
      +Read from stream: ReadSuccess(Tick)
      +Read from stream: None |}] ;
    close t
end

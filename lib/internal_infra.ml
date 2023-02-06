open Eio.Std
open Types
module CMgr = Ocons_conn_mgr
open Utils

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
    ; internal_streams: (node_id, C.message Eio.Stream.t) Hashtbl.t }

  let apply (t : t) (cmd : command) : op_result =
    (* TODO truncate more sensibly
          ie from each client record the high water mark of results and remove lower than that
       Core.Hash_set.remove t.inflight_txns cmd.id ;
    *)
    update_state_machine t.state_machine cmd

  let handle_actions t actions =
    let f : C.action -> unit = function
      | C.Send (dst, msg) ->
          CMgr.send_blit t.cmgr dst (C.serialise msg) ;
          dtraceln "Sent to %d: %a" dst C.message_pp msg
      | C.Broadcast msg ->
          CMgr.broadcast_blit t.cmgr (C.serialise msg) ;
          dtraceln "Broadcast %a" C.message_pp msg
      | C.CommitCommands citer ->
          citer (fun cmd ->
              let res = apply t cmd in
              Eio.Stream.add t.c_tx (cmd.id, res) ;
              dtraceln "Stored result of %d: %a" cmd.id op_result_pp res ) ;
          dtraceln "Committed %a"
            (Fmt.braces @@ Iter.pp_seq ~sep:", " Types.Command.pp)
            citer
    in
    List.iter f actions

  (** If any msgs to internal port exist then read and apply them *)
  let internal_msgs t =
    let iter_msg src msg =
      dtraceln "Receiving msg from %d: %a" src C.message_pp msg ;
      let tcons, actions = C.advance t.cons (Recv (msg, src)) in
      t.cons <- tcons ;
      handle_actions t actions
    in
    (fun k -> Hashtbl.iter (fun a b -> k (a, b)) t.internal_streams)
    |> Iter.iter (* Per stream *)
       @@ fun (src, s) ->
       Iter.of_gen (fun () -> Eio.Stream.take_nonblocking s)
       |> Iter.iter (iter_msg src)

  (** Recv client msgs *)
  let admit_client_requests t =
    (* length = 0 => num_to_take = 0
       By contrapositive: num_to_take > 0 then length > 0
    *)
    let num_to_take =
      min (C.available_space_for_commands t.cons) (Eio.Stream.length t.c_rx)
    in
    let iter =
      Iter.unfoldr
        (function
          | 0 ->
              None
          | rem ->
              let c_o = Eio.Stream.take_nonblocking t.c_rx in
              Option.bind c_o (function
                | c when Core.Hash_set.mem t.inflight_txns c.id ->
                    dtraceln "Already received %d" c.id ;
                    None
                | c ->
                    dtraceln "Received cmd %d" c.id ;
                    Core.Hash_set.add t.inflight_txns c.id ;
                    Some (c, rem - 1) ) )
        num_to_take
    in
    if num_to_take > 0 then (
      let p_iter = Iter.persistent iter in
      dtraceln "Passing commands: %a" (Iter.pp_seq ~sep:"," Command.pp) p_iter ;
      let tcons, actions = C.advance t.cons (Commands p_iter) in
      t.cons <- tcons ;
      handle_actions t actions )

  let ensure_sent t = CMgr.flush_all t.cmgr ; Fiber.yield ()

  let tick t () =
    dtraceln "Tick" ;
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

  let run_inter ~sw (clock : #Eio.Time.clock) config period resolvers
      client_msgs client_resps internal_streams =
    let cmgr =
      Ocons_conn_mgr.create ~sw resolvers C.parse (fun () ->
          Eio.Time.sleep clock 1. )
    in
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
      ; internal_streams }
    in
    while true do
      Fiber.yield () ; main_loop t
    done ;
    Ocons_conn_mgr.close t.cmgr

  let accept_handler internal_streams : Eio.Net.connection_handler =
   fun sock addr ->
    try
      let parse =
        let open Eio.Buf_read in
        let open Eio.Buf_read.Syntax in
        uint8 <*> seq C.parse
      in
      let id, msg_seq = Eio.Buf_read.parse_exn ~max_size:1_000_000 parse sock in
      dtraceln "Accepting connection from %a, node_id %d" Eio.Net.Sockaddr.pp
        addr id ;
      let str = Eio.Stream.create 16 in
      Hashtbl.add internal_streams id str ;
      Seq.iter (Eio.Stream.add str) msg_seq
    with e when Utils.is_not_cancel e ->
      dtraceln "Connection from %a failed with %a" Eio.Net.Sockaddr.pp addr
        Fmt.exn e

  let resolver_handshake node_id resolvers =
    let handshake r sw =
      let f = r sw in
      Eio.Buf_write.with_flow f (fun bw -> Eio.Buf_write.uint8 bw node_id) ;
      f
    in
    List.map (fun (id, r) -> (id, handshake r)) resolvers

  type 'a env = < clock: #Eio.Time.clock ; net: #Eio.Net.t ; .. > as 'a

  let run ~sw env node_id config period resolvers client_msgs client_resps port
      =
    let internal_streams = Hashtbl.create (List.length resolvers) in
    let closed_p = Promise.create () in
    Fiber.fork ~sw (fun () ->
        let addr = `Tcp (Eio.Net.Ipaddr.V4.any, port) in
        let sock = Eio.Net.listen ~backlog:4 ~sw env#net addr in
        traceln "Listening on %a" Eio.Net.Sockaddr.pp addr ;
        Eio.Net.run_server ~stop:(fst closed_p)
          ~on_error:(dtraceln "%a" Fmt.exn) sock
          (accept_handler internal_streams) ) ;
    let resolvers = resolver_handshake node_id resolvers in
    run_inter ~sw env#clock config period resolvers client_msgs client_resps
      internal_streams
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
      dtraceln "read %s" r ; r

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

  let%expect_test "test_client_request_path" =
    Eio_mock.Backend.run
    @@ fun () ->
    let clk = Eio_mock.Clock.make () in
    Switch.run
    @@ fun sw ->
    let c_rx = Eio.Stream.create 10 in
    let c_tx = Eio.Stream.create 10 in
    let f1 = Eio_mock.Flow.make "f1" in
    let resolvers = [(1, fun _ -> (f1 :> Eio.Flow.two_way))] in
    (* Start of actual test *)
    Fiber.fork ~sw (fun () ->
        run_inter ~sw
          (clk :> Eio.Time.clock)
          () 1. resolvers c_rx c_tx (Hashtbl.create 0) ) ;
    Fiber.yield () ;
    (* No errors or msgs on startup *)
    [%expect {||}] ;
    (* Can take remote messages *)
    Eio_mock.Flow.on_read f1 [`Return "1\n"] ;
    Fiber.yield () ;
    Fiber.yield () ;
    dtraceln "Read from stream: %a"
      (Fmt.option ~none:(Fmt.any "None") Types.op_result_pp)
      (Option.map snd @@ Eio.Stream.take_nonblocking c_tx) ;
    dtraceln "Read from stream: %a"
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
      dtraceln "Read from stream: %a"
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
      dtraceln "Read from stream: %a"
        (Fmt.option ~none:(Fmt.any "None") Types.op_result_pp)
        (Option.map snd @@ Eio.Stream.take_nonblocking c_tx)
    done ;
    [%expect
      {|
      +mock time is now 2
      +Read from stream: ReadSuccess(Tick)
      +Read from stream: None |}]
end

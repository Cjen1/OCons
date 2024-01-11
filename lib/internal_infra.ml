open Eio.Std
open Types
module CMgr = Ocons_conn_mgr
open Utils
open Consensus_intf

module Ticker = struct
  type t =
    { mutable next_tick: float
    ; period: float
    ; clock: float Eio.Time.clock_ty Eio.Resource.t }

  let create clock period =
    {next_tick= Core.Float.(Eio.Time.now clock + period); period; clock}

  let tick t f =
    let now = Eio.Time.now t.clock in
    if now > t.next_tick then (
      t.next_tick <- Core.Float.(now + t.period) ;
      f () )
end

module CommandQueue = RemovableQueue (struct
  module Index = Core.Int

  let pp_index = Fmt.int

  type t = Command.t [@@deriving sexp, show]

  let to_index t = t.Command.id
end)

module Make (C : Consensus_intf.S) = struct
  type debug =
    { command_length_reporter: int InternalReporter.reporter
    ; no_commands_reporter: unit InternalReporter.reporter
    ; request_reporter: unit InternalReporter.reporter
    ; no_space_reporter: unit InternalReporter.reporter
    ; commit_reporter: unit InternalReporter.reporter
    ; main_loop_length_reporter: float InternalReporter.reporter
    ; clock: float Eio.Time.clock_ty Eio.Resource.t }

  type t =
    { c_rx: command Eio.Stream.t
    ; c_tx: Line_prot.External_infra.response Eio.Stream.t
    ; cmgr: C.message Ocons_conn_mgr.t
    ; state_machine: state_machine
    ; mutable cons: C.t
    ; ticker: Ticker.t
    ; internal_streams: (node_id, C.message Eio.Stream.t) Hashtbl.t
    ; command_queue: CommandQueue.t
    ; applied_txns: Core.Hash_set.M(Int).t
    ; debug: debug }

  let apply (t : t) (cmd : command) : op_result =
    update_state_machine t.state_machine cmd

  let handle_actions t actions =
    let f : C.message action -> unit = function
      | Send (dst, msg) ->
          CMgr.send_blit t.cmgr dst (C.serialise msg) ;
          dtraceln "Sent to %d: %a" dst C.PP.message_pp msg
      | Broadcast msg ->
          CMgr.broadcast_blit t.cmgr (C.serialise msg) ;
          dtraceln "Broadcast %a" C.PP.message_pp msg
      | CommitCommands citer ->
          citer (fun cmd ->
              CommandQueue.remove t.command_queue cmd.id ;
              Core.Hash_set.add t.applied_txns cmd.id ;
              let res = apply t cmd in
              TRACE.commit cmd ;
              if C.should_ack_clients t.cons then (
                Eio.Stream.add t.c_tx (cmd.id, res, get_command_trace_time cmd) ;
                t.debug.commit_reporter () ;
                dtraceln "Stored result of %d: %a" cmd.id op_result_pp res ) ) ;
          dtraceln "Committed %a"
            (Fmt.braces @@ Iter.pp_seq ~sep:", " Types.Command.pp)
            citer
    in
    List.iter f actions

  (** If any msgs to internal port exist then read and apply them *)
  let internal_msgs t =
    let iter_msg src msg =
      dtraceln "Receiving msg from %d: %a" src C.PP.message_pp msg ;
      let tcons, actions = C.advance t.cons (Recv (msg, src)) in
      t.cons <- tcons ;
      handle_actions t actions
    in
    (fun k -> Hashtbl.iter (fun a b -> k (a, b)) t.internal_streams)
    |> Iter.iter (* Per stream *)
       @@ fun (src, s) ->
       Iter.of_gen (fun () -> Eio.Stream.take_nonblocking s)
       |> Iter.iter (iter_msg src)

  (* TODO just let other end add to the list maybe? *)
  let admit_client_requests t =
    let cmd_iter =
      match Eio.Stream.take_nonblocking t.c_rx with
      | None ->
          Iter.empty
      | Some c1 ->
          Iter.cons c1
            (Iter.from_fun (fun () -> Eio.Stream.take_nonblocking t.c_rx))
    in
    cmd_iter (fun c ->
        if not (Core.Hash_set.mem t.applied_txns c.Command.id) then
          CommandQueue.add t.command_queue c ) ;
    match C.available_space_for_commands t.cons with
    | space when space <= 0 ->
        t.debug.no_space_reporter ()
    | space ->
        let elts = CommandQueue.take t.command_queue space in
        if Core.Doubly_linked.is_empty elts then t.debug.no_commands_reporter ()
        else
          let iter =
            Core.Doubly_linked.iter elts
            |> Iter.from_labelled_iter
            |> Iter.map (fun c ->
                   TRACE.ex_in c ;
                   t.debug.request_reporter () ;
                   c )
          in
          let tcons, actions = C.advance t.cons (Commands iter) in
          t.cons <- tcons ;
          handle_actions t actions

  let ensure_sent _t =
    (* We should flush here to ensure queueus aren't building up.
       However in practise that results in about a 2x drop in highest throughput
       So we just yield to the scheduler. This should cause writes to still be
       flushed, but does not wait for confirmation that they have been flushed.

       This improves latency at high rates by ~1 order of magnitude

       Expected outcome at system capacity is for queuing on outbound network capacity
    *)
    (*CMgr.flush_all t.cmgr ;*)
    Fiber.yield ()

  let tick t () =
    dtraceln "Tick" ;
    let tcons, actions = C.advance t.cons Tick in
    t.cons <- tcons ;
    handle_actions t actions

  let main_loop t =
    (* Do internal mostly non-blocking things *)
    let st = Eio.Time.now t.debug.clock in
    internal_msgs t ;
    admit_client_requests t ;
    Ticker.tick t.ticker (tick t) ;
    (* Then block to send all things *)
    ensure_sent t ;
    let dur = (Eio.Time.now t.debug.clock -. st) *. 1000. in
    if dur > 0.01 then t.debug.main_loop_length_reporter dur ;
    if dur > 100. then Magic_trace.take_snapshot () ;
    ()

  let run_inter ~sw env node_id config period resolvers client_msgs client_resps
      internal_streams =
    let cmgr =
      Ocons_conn_mgr.create ~sw resolvers C.parse (fun () ->
          Eio.Time.sleep env 1. )
    in
    let cons = C.create_node node_id config in
    let state_machine = Core.Hashtbl.create (module Core.String) in
    let ticker = Ticker.create (env :> float Eio.Time.clock_ty r) period in
    let run (rep, should_run) =
      should_run := true ;
      rep
    in
    let debug =
      { command_length_reporter=
          InternalReporter.avg_reporter Int.to_float "cmd_len" |> run
      ; no_commands_reporter=
          InternalReporter.rate_reporter "no-commands" |> run
      ; request_reporter= InternalReporter.rate_reporter "request" |> run
      ; no_space_reporter= InternalReporter.rate_reporter "no_space" |> run
      ; commit_reporter= InternalReporter.rate_reporter "commit" |> run
      ; main_loop_length_reporter=
          InternalReporter.avg_reporter Fun.id "main_loop_delay" |> run
      ; clock= (env :> float Eio.Time.clock_ty r) }
    in
    let t =
      { c_tx= client_resps
      ; c_rx= client_msgs
      ; cmgr
      ; cons
      ; state_machine
      ; ticker
      ; internal_streams
      ; command_queue= CommandQueue.create ()
      ; applied_txns= Core.Hash_set.create (module Core.Int)
      ; debug }
    in
    while true do
      main_loop t
    done ;
    Ocons_conn_mgr.close t.cmgr

  let accept_handler internal_streams sock addr =
    Utils.set_nodelay sock ;
    try
      let parse =
        let open Eio.Buf_read in
        let open Eio.Buf_read.Syntax in
        uint8 <*> seq C.parse
      in
      let br = Eio.Buf_read.of_flow ~max_size:1_000_000 sock in
      let id, msg_seq = parse br in
      dtraceln "Accepting connection from %a, node_id %d" Eio.Net.Sockaddr.pp
        addr id ;
      let str = Eio.Stream.create 16 in
      Hashtbl.add internal_streams id str ;
      Seq.iter (Eio.Stream.add str) msg_seq
    with e when Utils.is_not_cancel e ->
      dtraceln "Connection from %a failed with %a" Eio.Net.Sockaddr.pp addr
        Fmt.exn_backtrace
        (e, Printexc.get_raw_backtrace ())

  let resolver_handshake node_id resolvers =
    let handshake r sw =
      let f = r sw in
      Eio.Buf_write.with_flow f (fun bw -> Eio.Buf_write.uint8 bw node_id) ;
      f
    in
    List.map (fun (id, r) -> (id, handshake r)) resolvers

  let run ~sw env node_id config period resolvers client_msgs client_resps port
      =
    TRACE.run_commit := true ;
    TRACE.run_ex_in := true ;
    let internal_streams = Hashtbl.create (List.length resolvers) in
    Fiber.fork ~sw (fun () ->
        Eio.Domain_manager.run (Eio.Stdenv.domain_mgr env) (fun () ->
            Switch.run
            @@ fun sw ->
            let addr = `Tcp (Eio.Net.Ipaddr.V4.any, port) in
            let sock =
              Eio.Net.listen ~reuse_addr:true ~backlog:4 ~sw
                (Eio.Stdenv.net env) addr
            in
            traceln "Listening on %a" Eio.Net.Sockaddr.pp addr ;
            Eio.Net.run_server ~on_error:(dtraceln "%a" Fmt.exn) sock
              (accept_handler internal_streams) ) ) ;
    let resolvers = resolver_handshake node_id resolvers in
    run_inter ~sw (Eio.Stdenv.clock env) node_id config period resolvers
      client_msgs client_resps internal_streams
end

module Test = struct
  let w k n =
    Command.
      { op= Write (k, n)
      ; id= Core.Random.int Core.Int.max_value
      ; trace_start= -1.
      ; submitted= -1. }

  let r n =
    Command.
      { op= Read n
      ; id= Core.Random.int Core.Int.max_value
      ; trace_start= -1.
      ; submitted= -1. }

  module CT : Consensus_intf.S = struct
    type message = Core.String.t [@@deriving sexp]

    let should_ack_clients _ = true

    let parse buf =
      let r = Eio.Buf_read.line buf in
      dtraceln "read %s" r ; r

    let serialise v buf =
      Eio.Buf_write.string buf v ;
      Eio.Buf_write.char buf '\n'

    type config = Core.Unit.t [@@deriving sexp_of]

    type t = {arr: command option Array.t; mutable len: int}

    module PP = struct
      let message_pp = Fmt.string

      let config_pp : config Fmt.t = Fmt.any "config"

      let t_pp ppf _ = Fmt.pf ppf "T"
    end

    let create_node _ () = {arr= Array.make 10 None; len= 0}

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
end

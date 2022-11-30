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
    ; cmgr: Ocons_conn_mgr.t
    ; state_machine: state_machine
    ; inflight_txns: command_id Core.Hash_set.t
    ; mutable cons: C.t
    ; ticker: Ticker.t }

  let apply (t : t) (cmd : command) : op_result =
    Core.Hash_set.remove t.inflight_txns cmd.id ;
    update_state_machine t.state_machine cmd

  let handle_actions t actions =
    let blitter _ = assert false in
    let f : C.action -> unit = function
      | C.Send (dst, msg) ->
          CMgr.send t.cmgr dst (blitter msg)
      | C.Broadcast msg ->
          CMgr.broadcast t.cmgr (blitter msg)
      | C.CommitCommands citer ->
          citer (fun cmd ->
              let res = apply t cmd in
              Eio.Stream.add t.c_tx (cmd.id, res) )
    in
    List.iter f actions

  (** If any msgs to internal port exist then read and apply them *)
  let internal_msgs t =
    let msg_iter = CMgr.recv_any t.cmgr in
    let iterf (cst, src) =
      let msg =
        C.bin_message.reader.read
          ~pos_ref:Cstruct.(ref cst.off)
          Cstruct.(cst.buffer)
      in
      let tcons, actions = C.advance t.cons (Recv (msg, src)) in
      t.cons <- tcons ;
      handle_actions t actions
    in
    msg_iter iterf

  (** Recv client msgs *)
  let admit_client_requests t =
    let num_to_take =
      min (C.remaining_inflight_entries t.cons) (Eio.Stream.length t.c_rx)
    in
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

  let send_all t = CMgr.flush t.cmgr

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
    send_all t

  let run config clock period resolvers client_msgs client_resps =
    Switch.run
    @@ fun sw ->
    let cmgr = Ocons_conn_mgr.create ~sw resolvers in
    let cons = C.create_node config in
    let state_machine = Core.Hashtbl.create (module Core.String) in
    let ticker = Ticker.create clock period in
    let t =
      { c_tx= client_resps
      ; c_rx= client_msgs
      ; cmgr
      ; cons
      ; state_machine
      ; inflight_txns= Core.Hash_set.create (module Uuid)
      ; ticker }
    in
    main_loop t
end

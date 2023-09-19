open! Types
open Eio.Std

type 'cons config =
  { cons_config: 'cons
  ; internal_port: int
  ; external_port: int
  ; stream_length: int
  ; tick_period: float
  ; nodes: (int * Eio.Net.Sockaddr.stream) list
  ; node_id: int
  ; stat_report: time }

module Make (C : Consensus_intf.S) = struct
  module Internal = Internal_infra.Make (C)
  module ExInfra = External_infra

  type 'a env =
    < clock: #Eio.Time.clock
    ; mono_clock: #Eio.Time.Mono.t
    ; net: #Eio.Net.t
    ; domain_mgr: #Eio.Domain_manager.t
    ; .. >
    as
    'a

  let run env config =
    Switch.run
    @@ fun sw ->
    let command_stream = Eio.Stream.create Int.max_int in
    Utils.InternalReporter.run ~sw env#clock config.stat_report ;
    let result_stream = Eio.Stream.create Int.max_int in
    let create_conn addr sw =
      let c = (Eio.Net.connect ~sw env#net addr :> Eio.Flow.two_way) in
      Utils.set_nodelay c ; c
    in
    let conns : connection_creater list =
      config.nodes
      |> List.filter (fun (id, _) -> id <> config.node_id)
      |> List.map (fun (id, addr) -> (id, create_conn addr))
    in
    Fiber.both
      (fun () ->
        try
          Internal.run ~sw env config.node_id config.cons_config
            config.tick_period conns command_stream result_stream
            config.internal_port
        with e when Utils.is_not_cancel e ->
          traceln "Internal infra failed" ;
          traceln "%a" Fmt.exn_backtrace (e, Printexc.get_raw_backtrace ()) ;
          exit (-1) )
      (fun () ->
        try
          (*
          Eio.Domain_manager.run env#domain_mgr (fun () ->
            *)
          ExInfra.run env#net env#clock config.external_port command_stream
            result_stream
        with e when Utils.is_not_cancel e ->
          traceln "External infra failed" ;
          traceln "%a" Fmt.exn_backtrace (e, Printexc.get_raw_backtrace ()) ;
          exit (-1) )
end

open! Types
open Eio.Std

module Make (C : Consensus_intf.S) = struct
  module Internal = Internal_infra.Make (C)
  module ExInfra = External_infra

  type config =
    { cons_config: C.config
    ; internal_port: int
    ; external_port: int
    ; stream_length: int
    ; tick_period: float
    ; nodes: (int * Eio.Net.Sockaddr.stream) list
    ; node_id: int }

  type 'a env = < clock: #Eio.Time.clock ; net: #Eio.Net.t ; .. > as 'a

  let run env config =
    Switch.run (fun sw ->
        let command_stream = Eio.Stream.create config.stream_length in
        let result_stream = Eio.Stream.create (config.stream_length * 64) in
        let create_conn addr sw =
          (Eio.Net.connect ~sw env#net addr :> Eio.Flow.two_way)
        in
        let conns : connection_creater list =
          config.nodes
          |> List.filter_map (function
               | id, _ when id = config.node_id ->
                   None
               | id, addr ->
                   Some (id, create_conn addr) )
        in
        let internal =
          Internal.create ~sw config.cons_config env#clock config.tick_period
            conns command_stream result_stream
        in
        ExInfra.run env#net config.external_port command_stream result_stream ;
        Internal.close internal )
end

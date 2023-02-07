open! Types

module Make (C : Consensus_intf.S) : sig
  type config =
    { cons_config: C.config
    ; internal_port: int
    ; external_port: int
    ; stream_length: int
    ; tick_period: float
    ; nodes: (int * Eio.Net.Sockaddr.stream) list
    ; node_id: int }

  type 'a env =
    < clock: #Eio.Time.clock
    ; net: #Eio.Net.t
    ; domain_mgr: #Eio.Domain_manager.t
    ; .. >
    as
    'a

  val run : _ env -> config -> unit
end

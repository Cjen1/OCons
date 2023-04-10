open Types

module Make (C : Consensus_intf.S) : sig
  type t

  type 'a env =
    < clock: #Eio.Time.clock
    ; mono_clock: #Eio.Time.Mono.t
    ; net: #Eio.Net.t
    ; domain_mgr: #Eio.Domain_manager.t
    ; .. >
    as
    'a

  val run :
       sw:Eio.Switch.t
    -> _ env
    -> node_id
    -> C.config
    -> time
    -> connection_creater list
    -> command Eio.Stream.t
    -> Line_prot.External_infra.response Eio.Stream.t
    -> int
    -> unit
  (** [run ~sw env id cfg t conns c_rx c_tx port] Runs the specified consensus protocol, 
      receiving requests from the [c_rx], and returning the committed result via [c_tx].
      Messages are sent to [conns] and received on 0.0.0.0:[port].
      Ticks are applied every [t] seconds.
      *)
end

open Types

module Make (C : Consensus_intf.S) : sig
  type t
  val create :
       sw: Eio.Switch.t
    -> C.config
    -> Eio.Time.clock
    -> time
    -> (connection_creater) list
    -> command Eio.Stream.t
    -> (command_id * op_result) Eio.Stream.t
    -> t
  (** [run cfg clk T conns c_rx c_tx] Runs the specified consensus protocol, 
      receiving requests from the [c_rx], and returning the committed result via [c_tx] 
      Ticks are applied every [T] seconds.
      *)

  val close : t -> unit
end

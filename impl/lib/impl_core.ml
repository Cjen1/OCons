open! Types
module Line_prot = Line_prot
module Types = Types

module Paxos : Ocons_core.Consensus_intf.S 
with 
type message = PaxosTypes.message and
type event = PaxosTypes.event 
and type config = Types.config
= struct
  include PaxosTypes
  include Paxos.Impl

  type config = Types.config
  let config_pp = Types.config_pp

  let create_node = create

  let available_space_for_commands t =
    let outstanding = Utils.SegmentLog.highest t.log - t.commit_index in
    assert (outstanding >= 0) ;
    if match t.node_state with Leader _ -> true | _ -> false then
      max (t.config.max_outstanding - outstanding) 0
    else 0

  let should_ack_clients t =
    match t.node_state with Leader _ -> true | _ -> false

  let parse = Line_prot.Paxos.parse

  let serialise = Line_prot.Paxos.serialise
end

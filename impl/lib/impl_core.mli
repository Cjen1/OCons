module Types = Types
module Line_prot = Line_prot

module Paxos : Ocons_core.Consensus_intf.S with type config = Types.config

module Raft : Ocons_core.Consensus_intf.S with type config = Types.config

module VPaxos : Ocons_core.Consensus_intf.S with type config = Types.config

module VRaft : Ocons_core.Consensus_intf.S with type config = Types.config

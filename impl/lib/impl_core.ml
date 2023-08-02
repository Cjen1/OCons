open! Types
module Line_prot = Line_prot
module Types = Types

module Paxos = struct
  include Paxos.Types
  include Paxos.Impl

  type config = Types.config

  module PP = struct
    include PP

    let config_pp = Types.config_pp
  end

  let create_node _ = create

  let available_space_for_commands t =
    let outstanding = Utils.SegmentLog.highest t.log - t.commit_index in
    assert (outstanding >= 0) ;
    if match t.node_state with Leader _ -> true | _ -> false then
      max (t.config.max_outstanding - outstanding) 0
    else 0

  let should_ack_clients _t = true
  (*    match t.node_state with Leader _ -> true | _ -> false*)

  let parse = Line_prot.Paxos.parse

  let serialise = Line_prot.Paxos.serialise
end

module Raft = struct
  include Raft.Types
  include Raft.Impl

  type config = Types.config

  module PP = struct
    include PP

    let config_pp = Types.config_pp
  end

  let create_node _ = create

  let available_space_for_commands t =
    let outstanding = Utils.SegmentLog.highest t.log - t.commit_index in
    assert (outstanding >= 0) ;
    if match t.node_state with Leader _ -> true | _ -> false then
      max (t.config.max_outstanding - outstanding) 0
    else 0

  let should_ack_clients t =
    match t.node_state with Leader _ -> true | _ -> false

  let parse = Line_prot.Raft.parse

  let serialise = Line_prot.Raft.serialise
end

module RaftSBN = struct
  include Raft_sbn.Types
  include Raft_sbn.Impl

  type config = Types.config

  module PP = struct
    include PP

    let config_pp = Types.config_pp
  end

  let create_node _ = create

  let available_space_for_commands t =
    let outstanding = Utils.SegmentLog.highest t.log - t.commit_index in
    assert (outstanding >= 0) ;
    if match t.node_state with Leader _ -> true | _ -> false then
      max (t.config.max_outstanding - outstanding) 0
    else 0

  let should_ack_clients t =
    match t.node_state with Leader _ -> true | _ -> false

  let parse = Line_prot.Raft.parse

  let serialise = Line_prot.Raft.serialise
end

module PrevoteRaft = struct
  include Prevote.Types
  include Prevote.Impl

  type config = Types.config

  module PP = struct
    include PP

    let config_pp = Types.config_pp
  end

  let create_node _ = create

  let available_space_for_commands t =
    let outstanding = Utils.SegmentLog.highest t.log - t.commit_index in
    assert (outstanding >= 0) ;
    if match t.node_state with Leader _ -> true | _ -> false then
      max (t.config.max_outstanding - outstanding) 0
    else 0

  let should_ack_clients t =
    match t.node_state with Leader _ -> true | _ -> false

  let parse = Line_prot.PrevoteRaft.parse

  let serialise = Line_prot.PrevoteRaft.serialise
end

module PrevoteRaftSBN = struct
  include Prevote_sbn.Types
  include Prevote_sbn.Impl

  type config = Types.config

  module PP = struct
    include PP

    let config_pp = Types.config_pp
  end

  let create_node _ = create

  let available_space_for_commands t =
    let outstanding = Utils.SegmentLog.highest t.log - t.commit_index in
    assert (outstanding >= 0) ;
    if match t.node_state with Leader _ -> true | _ -> false then
      max (t.config.max_outstanding - outstanding) 0
    else 0

  let should_ack_clients t =
    match t.node_state with Leader _ -> true | _ -> false

  let parse = Line_prot.PrevoteRaft.parse

  let serialise = Line_prot.PrevoteRaft.serialise
end

module ConspireSS = struct
  include Conspire_single_shot.Types
  include Conspire_single_shot.Impl

  let create_node _ = create

  let available_space_for_commands t =
    let outstanding = Utils.SegmentLog.highest t.prop_log - t.commit_index in
    assert (outstanding >= 0) ;
    max (t.config.max_outstanding - outstanding) 0

  let should_ack_clients _ = true

  let parse = Line_prot.ConspireSS.parse

  let serialise = Line_prot.ConspireSS.serialise
end

module Conspire = struct
  include Conspire.Types
  include Conspire.Impl
  let create_node _ = create

  let available_space_for_commands t =
    let outstanding = Utils.SegmentLog.highest t.state.local_state.vval - t.state.local_state.commit_index in
    assert (outstanding >= 0) ;
    max (t.config.max_outstanding - outstanding) 0

  let should_ack_clients _ = true

  let parse = Line_prot.Conspire.parse
  let serialise = Line_prot.Conspire.serialise
end

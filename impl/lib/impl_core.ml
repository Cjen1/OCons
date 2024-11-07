open! Types
module Utils = Utils
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
    if outstanding < 0 then
      Fmt.failwith "Outstanding invalid: {highest: %d, commit_index: %d}"
        (Utils.SegmentLog.highest t.log)
        t.commit_index ;
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
    if outstanding < 0 then
      Fmt.failwith "Outstanding invalid: {highest: %d, commit_index: %d}"
        (Utils.SegmentLog.highest t.log)
        t.commit_index ;
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
    if outstanding < 0 then
      Fmt.failwith "Outstanding invalid: {highest: %d, commit_index: %d}"
        (Utils.SegmentLog.highest t.log)
        t.commit_index ;
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
    if outstanding < 0 then
      Fmt.failwith "Outstanding invalid: {highest: %d, commit_index: %d}"
        (Utils.SegmentLog.highest t.log)
        t.commit_index ;
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
    if outstanding < 0 then
      Fmt.failwith "Outstanding invalid: {highest: %d, commit_index: %d}"
        (Utils.SegmentLog.highest t.log)
        t.commit_index ;
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
    if outstanding < 0 then
      Fmt.failwith "Outstanding invalid: {highest: %d, commit_index: %d}"
        (Utils.SegmentLog.highest t.prop_log)
        t.commit_index ;
    max (t.config.max_outstanding - outstanding) 0

  let should_ack_clients _ = true

  let parse = Line_prot.ConspireSS.parse

  let serialise = Line_prot.ConspireSS.serialise
end

module ConspireLeader = struct
  include Conspire_leader.Types
  include Conspire_leader.Impl

  let create_node _ = create

  let should_ack_clients _ = true

  let serialise m w =
    Line_prot.bin_io_write w bin_write_message bin_size_message m

  let parse r = Line_prot.bin_io_read bin_read_message r
end

module ConspireDC = struct
  include Conspire_dc.Types
  include Conspire_dc.Impl

  let create_node _ = create

  let should_ack_clients _ = true

  let serialise m w =
    Line_prot.bin_io_write w bin_write_message bin_size_message m

  let parse r = Line_prot.bin_io_read bin_read_message r
end

module ConspireLeaderDC = struct
  include Conspire_leader_dc.Types
  include Conspire_leader_dc.Impl

  let create_node _ = create

  let should_ack_clients _ = true

  let serialise m w =
    Line_prot.bin_io_write w bin_write_message bin_size_message m

  let parse r = Line_prot.bin_io_read bin_read_message r
end

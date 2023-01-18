open! Types
module Line_prot = Line_prot
module Types = Types

module Imperative : Ocons_core.Consensus_intf.S = struct
  include Sm.Impl
  include Types

  let create_node = Types.create

  let available_space_for_commands t =
    let outstanding = Utils.SegmentLog.highest t.log - t.commit_index in
    t.config.max_outstanding - outstanding

  let parse = Line_prot.parse

  let serialise = Line_prot.serialise
end

include Imperative

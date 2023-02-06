open! Types
module Line_prot = Line_prot
module Types = Types

module Imperative = struct
  include Sm.Impl
  include Types

  let create_node = Types.create

  let available_space_for_commands t =
    let outstanding = Utils.SegmentLog.highest t.log - t.commit_index in
    if match t.node_state with Leader _ -> true | _ -> false then
      t.config.max_outstanding - outstanding
    else 0

  let parse = Line_prot.parse

  let serialise = Line_prot.serialise
end

include Imperative

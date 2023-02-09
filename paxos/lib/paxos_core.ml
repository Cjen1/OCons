open! Types
module Line_prot = Line_prot
module Types = Types

module Imperative = struct
  include Sm.Impl
  include Types

  let create_node = Types.create

  let available_space_for_commands t =
    let outstanding = Utils.SegmentLog.highest t.log - t.commit_index in
    assert (outstanding >= 0);
    if match t.node_state with Leader _ -> true | _ -> false then
      max (t.config.max_outstanding - outstanding) 0
    else 0

  let should_ack_clients t =
    match t.node_state with Leader _-> true | _ -> false

  let parse = Line_prot.parse

  let serialise = Line_prot.serialise
end

include Imperative

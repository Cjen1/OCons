type t

(* C before means that it has changed *)
(* R before means receiving *)
type event =
  [ `Tick
  | `CLog
  | `CCommitIndex
  | `CNextIndex
  | `RRequestVote
  | `RRequestVoteResponse
  | `RAppendEntries
  | `RAppendEntiresResponse ]

type action =
  [ `SendRequestVote
  | `SendRequestVoteResponse
  | `SendAppendEntries
  | `SendAppendEntriesResponse ]

val handle : t -> event -> t * action list

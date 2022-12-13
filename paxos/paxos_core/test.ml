open! Types

module Impl = Functor.Make(ImperativeActions)

let%expect_test "creatable" =
  let msg = RequestVote{term=1; leader_commit=2} in
  Fmt.pr "%a\n" message_pp msg;
  [%expect {| RequestVote {term:1; leader_commit:2} |}]

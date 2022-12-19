open! Types
open! Utils
module Imp = ImperativeActions
module Impl = Sm.Make (ImperativeActions)

let c1 = make_config ~node_id:0 ~node_list:[0] ~election_timeout:5

let c3 = make_config ~node_id:0 ~node_list:[0; 1; 2] ~election_timeout:5

let%expect_test "transit_follower" =
  let t = create c1 in
  let t', actions =
    Imp.run_side_effects (fun () -> Impl.transit_follower 10) t
  in
  Fmt.pr "state: %a\n" node_state_pp t'.node_state ;
  Fmt.pr "term: %d\n" t'.current_term ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect {|
    state: Follower(0)
    term: 10
    actions: [] |}]

let%expect_test "transit_candidate" =
  let t = create c3 in
  Fmt.pr "t0: %a\n" t_pp t ;
  [%expect
    {| t0: {log: _; commit_index:-1; current_term: 0; node_state:Follower(0)} |}] ;
  let t', actions = Imp.run_side_effects Impl.transit_candidate t in
  Fmt.pr "t': %a\n" t_pp t' ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    t': {log: _; commit_index:-1; current_term: 3; node_state:Candidate{quorum:{threshold 2, elts:
    []}, timeout:0}}
    actions: [Broadcast(RequestVote {term:3; leader_commit:-1})] |}] ;
  let t', actions = Imp.run_side_effects Impl.transit_candidate t' in
  Fmt.pr "t': %a\n" t_pp t' ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    t': {log: _; commit_index:-1; current_term: 6; node_state:Candidate{quorum:{threshold 2, elts:
    []}, timeout:0}}
    actions: [Broadcast(RequestVote {term:6; leader_commit:-1})] |}]

let%expect_test "transit_leader" = 
  let t = create c3 in
  let t', _ = Imp.run_side_effects Impl.transit_candidate t in
  let t', actions = Imp.run_side_effects Impl.transit_leader t' in
  Fmt.pr "t': %a\n" t_pp t' ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect {|
    t': {log: _; commit_index:-1; current_term: 3; node_state:{heartbeat:0; rep_ackd:
    [{1, 0}, {2, 0}]; rep_sent:[{1, -1}, {2, -1}]}
    actions: [Send(1, AppendEntries {term: 3; leader_commit: -1; prev_log_index: -2; prev_log_term: 0; entries_length: 0})
    Send(2, AppendEntries {term: 3; leader_commit: -1; prev_log_index: -2; prev_log_term: 0; entries_length: 0})] |}]

let%expect_test "request vote from higher" =
  let t = create c3 in
  let rv = RequestVote {term= 10; leader_commit= -1} in
  (* from follower *)
  let t', actions = Impl.advance t (Recv (rv, 2)) in
  Fmt.pr "t': %a\n" t_pp t' ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    t': {log: _; commit_index:-1; current_term: 10; node_state:Follower(0)}
    actions:
    [Send(2, RequestVoteResponse {term:10; start_index:-1; entries_length:0})] |}] ;
  (* from candidate *)
  let t', _ = Imp.run_side_effects Impl.transit_candidate t in
  let t', actions = Impl.advance t' (Recv (rv, 2)) in
  Fmt.pr "t': %a\n" t_pp t' ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    t': {log: _; commit_index:-1; current_term: 10; node_state:Follower(0)}
    actions:
    [Send(2, RequestVoteResponse {term:10; start_index:-1; entries_length:0})] |}] ;
  (* from leader *)
  let t', _ = Imp.run_side_effects Impl.transit_candidate t in
  let t', _ = Imp.run_side_effects Impl.transit_leader t' in
  let t', actions = Impl.advance t' (Recv (rv, 2)) in
  Fmt.pr "t': %a\n" t_pp t' ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    t': {log: _; commit_index:-1; current_term: 10; node_state:Follower(0)}
    actions:
    [Send(2, RequestVoteResponse {term:10; start_index:-1; entries_length:0})] |}]

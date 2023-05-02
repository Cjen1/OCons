open! Types
open! Utils
open! Actions_f
module Imp = ImperativeActions (RaftTypes)
module Impl = Raft.Make (Imp)
open! RaftTypes
open! Impl
open Ocons_core.Consensus_intf

let action_pp = action_pp ~pp_msg:message_pp

let c1 = make_config ~node_id:0 ~node_list:[0] ~election_timeout:5 ()

let c3 node_id =
  make_config ~node_id ~node_list:[0; 1; 2] ~election_timeout:5 ()

let%expect_test "transit_follower" =
  reset_make_command_state ();
  let t = create c1 in
  let t', actions =
    Imp.run_side_effects (fun () -> Impl.transit_follower 10) t
  in
  Fmt.pr "state: %a\n" node_state_pp t'.node_state ;
  Fmt.pr "term: %d\n" t'.current_term ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    +Follower for term 10
    state: Follower{timeout:5; voted_for:None}
    term: 10
    actions: [] |}]

let%expect_test "transit_candidate" =
  reset_make_command_state ();
  let t = create (c3 0) in
  Fmt.pr "t0: %a\n" t_pp t ;
  [%expect
    {| t0: {log: []; commit_index:-1; current_term: 0; node_state:Follower{timeout:0; voted_for:None}} |}] ;
  let t', actions = Imp.run_side_effects Impl.transit_candidate t in
  Fmt.pr "t': %a\n" t_pp t' ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    +Candidate for term 1
    t': {log: []; commit_index:-1; current_term: 1; node_state:Candidate{quorum:{threshold 1, elts:
    []}, timeout:4, repeat:1}}
    actions: [Broadcast(RequestVote {term:1; lastIndex:-1; lastTerm:0})] |}] ;
  let t', actions = Imp.run_side_effects Impl.transit_candidate t' in
  Fmt.pr "t': %a\n" t_pp t' ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    +Candidate for term 2
    t': {log: []; commit_index:-1; current_term: 2; node_state:Candidate{quorum:{threshold 1, elts:
    []}, timeout:5, repeat:1}}
    actions: [Broadcast(RequestVote {term:2; lastIndex:-1; lastTerm:0})] |}]

let%expect_test "transit_leader" =
  reset_make_command_state ();
  let t = create (c3 0) in
  let t', _ = Imp.run_side_effects Impl.transit_candidate t in
  let t', actions = Imp.run_side_effects Impl.transit_leader t' in
  Fmt.pr "t': %a\n" t_pp t' ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    +Candidate for term 1
    +Leader for term 1
    t': {log: [{command: Command(NoOp, -1); term : 1}]; commit_index:-1; current_term: 1; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, -1}, {2, -1}]; rep_sent:[{1, 0}, {2, 0}]}
    actions: [Send(1, AppendEntries {term: 1; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 1; entries:
                                                             [{command: Command(NoOp, -1); term : 1}]})
    Send(2, AppendEntries {term: 1; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 1; entries:
                                                             [{command: Command(NoOp, -1); term : 1}]})] |}]

let%expect_test "request vote from higher" =
  reset_make_command_state ();
  let t = create (c3 0) in
  let rv = RequestVote {term= 10; lastIndex= 1; lastTerm= 5} in
  (* from follower *)
  let t', actions = Impl.advance t (Recv (rv, 2)) in
  Fmt.pr "t': %a\n" t_pp t' ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    +Follower for term 10
    t': {log: []; commit_index:-1; current_term: 10; node_state:Follower{timeout:5; voted_for:2}}
    actions:
    [Send(2, RequestVoteResponse {term:10; success:true})] |}] ;
  (* from candidate *)
  let t', _ = Imp.run_side_effects Impl.transit_candidate t in
  let t', actions = Impl.advance t' (Recv (rv, 2)) in
  Fmt.pr "t': %a\n" t_pp t' ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    +Candidate for term 1
    +Follower for term 10
    t': {log: []; commit_index:-1; current_term: 10; node_state:Follower{timeout:5; voted_for:2}}
    actions:
    [Send(2, RequestVoteResponse {term:10; success:true})] |}] ;
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
    +Candidate for term 1
    +Leader for term 1
    +Follower for term 10
    t': {log: [{command: Command(NoOp, -1); term : 1}]; commit_index:-1; current_term: 10; node_state:Follower{timeout:5; voted_for:2}}
    actions:
    [Send(2, RequestVoteResponse {term:10; success:true})] |}]

let%expect_test "append_entries from other leader" =
  reset_make_command_state ();
  let t = create (c3 0) in
  let t, _ = Imp.run_side_effects Impl.transit_candidate t in
  Fmt.pr "t0: %a\n" t_pp t ;
  [%expect
    {|
    +Candidate for term 1
    t0: {log: []; commit_index:-1; current_term: 1; node_state:Candidate{quorum:{threshold 1, elts:
    []}, timeout:4, repeat:1}} |}] ;
  let t, _ =
    Imp.run_side_effects
      (fun () ->
        resolve_event
          (Recv
             ( AppendEntries
                 { term= 1
                 ; leader_commit= -1
                 ; prev_log_index= -1
                 ; prev_log_term= 0
                 ; entries= (Iter.empty, 0) }
             , 1 ) ) )
      t
  in
  Fmt.pr "t1: %a\n" t_pp t ;
  [%expect {|
    +Follower for term 1
    t1: {log: []; commit_index:-1; current_term: 1; node_state:Follower{timeout:5; voted_for:1}} |}] ;
  let t, _ =
    Impl.advance t
      (Recv
         ( AppendEntries
             { term= 1
             ; leader_commit= -1
             ; prev_log_index= -1
             ; prev_log_term= 0
             ; entries= (Iter.empty, 0) }
         , 1 ) )
  in
  Fmt.pr "t1: %a\n" t_pp t ;
  [%expect
    {|
    t1: {log: []; commit_index:-1; current_term: 1; node_state:Follower{timeout:5; voted_for:1}} |}]

let pp_res t actions =
  Fmt.pr "t: %a\n" t_pp t ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions

let%expect_test "Loop" =
  reset_make_command_state ();
  let t = create (c3 0) in
  let rv = RequestVote {term= 10; lastIndex= 5; lastTerm= 5} in
  (* --------- Get t to start election --------- *)
  let t, _ = Impl.advance t (Recv (rv, 2)) in
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  Fmt.pr "node_state: %a\n" node_state_pp (A.get node_state t) ;
  [%expect
    {|
    +Follower for term 10
    node_state: Follower{timeout:1; voted_for:2} |}] ;
  let t, actions = Impl.advance t Tick in
  Fmt.pr "node_state: %a\n" t_pp t ;
  [%expect
    {|
    +Candidate for term 11
    node_state: {log: []; commit_index:-1; current_term: 11; node_state:Candidate{quorum:{threshold 1, elts:
    []}, timeout:4, repeat:1}} |}] ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    actions: [Broadcast(RequestVote {term:11; lastIndex:-1; lastTerm:0})] |}] ;
  let rv = RequestVote {term= 11; lastIndex= -1; lastTerm= 0} in
  let t1 = create (c3 1) in
  let t1, a1 = Impl.advance t1 (Recv (rv, 2)) in
  Fmt.pr "a1: %a\n" Fmt.(brackets @@ list ~sep:(const string "\n") action_pp) a1 ;
  [%expect
    {|
    +Follower for term 11
    a1: [Send(2, RequestVoteResponse {term:11; success:true})] |}] ;
  let t2 = create (c3 2) in
  let t2, a2 = Impl.advance t2 (Recv (rv, 2)) in
  Fmt.pr "a2: %a\n" Fmt.(brackets @@ list ~sep:(const string "\n") action_pp) a2 ;
  [%expect
    {|
    +Follower for term 11
    a2: [Send(2, RequestVoteResponse {term:11; success:true})] |}] ;
  let rvr = RequestVoteResponse {term= 11; success= true} in
  let t, actions = Impl.advance t (Recv (rvr, 1)) in
  Fmt.pr "t: %a\n" t_pp t ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    +Leader for term 11
    t: {log: [{command: Command(NoOp, -1); term : 11}]; commit_index:-1; current_term: 11; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, -1}, {2, -1}]; rep_sent:[{1, 0}, {2, 0}]}
    actions: [Send(1, AppendEntries {term: 11; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 1; entries:
                                                             [{command: Command(NoOp, -1); term : 11}]})
    Send(2, AppendEntries {term: 11; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 1; entries:
                                                             [{command: Command(NoOp, -1); term : 11}]})] |}] ;
  let t, actions = Impl.advance t (Recv (rvr, 2)) in
  Fmt.pr "t: %a\n" t_pp t ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    t: {log: [{command: Command(NoOp, -1); term : 11}]; commit_index:-1; current_term: 11; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, -1}, {2, -1}]; rep_sent:[{1, 0}, {2, 0}]}
    actions: [] |}] ;
  (* ------------ Add m1 ----------------- *)
  let m1 = C.Types.(make_command (Read "m1")) in
  let t, actions = Impl.advance t (Commands (m1 |> Iter.singleton)) in
  Fmt.pr "t: %a\n" t_pp t ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11}]; commit_index:-1; current_term: 11; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, -1}, {2, -1}]; rep_sent:[{1, 1}, {2, 1}]}
    actions: [Send(1, AppendEntries {term: 11; leader_commit: -1; prev_log_index: 0; prev_log_term: 11; entries_length: 1; entries:
                                                             [{command: Command(Read m1, 1); term : 11}]})
    Send(2, AppendEntries {term: 11; leader_commit: -1; prev_log_index: 0; prev_log_term: 11; entries_length: 1; entries:
                                                             [{command: Command(Read m1, 1); term : 11}]})] |}] ;
  let aem1 =
    AppendEntries
      { term= 11
      ; leader_commit= -1
      ; prev_log_index= -1
      ; prev_log_term= 0
      ; entries=
          ( C.Types.[{command= empty_command; term= 11}; {command= m1; term= 11}]
            |> Iter.of_list
          , 2 ) }
  in
  let t1, a1 = Impl.advance t1 (Recv (aem1, 0)) in
  Fmt.pr "t1: %a\n" t_pp t1 ;
  Fmt.pr "a1: %a\n" Fmt.(brackets @@ list ~sep:(const string "\n") action_pp) a1 ;
  [%expect
    {|
    t1: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11}]; commit_index:-1; current_term: 11; node_state:Follower{timeout:5; voted_for:0}}
    a1:
    [Send(0, AppendEntriesResponse {term: 11; success: Ok: 1})] |}] ;
  let t2, a2 = Impl.advance t2 (Recv (aem1, 0)) in
  Fmt.pr "t2: %a\n" t_pp t2 ;
  Fmt.pr "a2: %a\n" Fmt.(brackets @@ list ~sep:(const string "\n") action_pp) a2 ;
  [%expect
    {|
    t2: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11}]; commit_index:-1; current_term: 11; node_state:Follower{timeout:5; voted_for:0}}
    a2:
    [Send(0, AppendEntriesResponse {term: 11; success: Ok: 1})] |}] ;
  let aer = AppendEntriesResponse {term= 11; success= Ok 1} in
  let t, _ = Impl.advance t (Recv (aer, 1)) in
  let t, actions = Impl.advance t (Recv (aer, 2)) in
  Fmt.pr "t: %a\n" t_pp t ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11}]; commit_index:1; current_term: 11; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, 1}, {2, 1}]; rep_sent:[{1, 1}, {2, 1}]}
    actions: [] |}] ;
  (* ------------ Add m2 ----------------- *)
  let m2 = C.Types.(make_command (Read "m2")) in
  let t, actions = Impl.advance t (Commands (m2 |> Iter.singleton)) in
  Fmt.pr "t: %a\n" t_pp t ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(Read m2, 2); term : 11}]; commit_index:1; current_term: 11; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, 1}, {2, 1}]; rep_sent:[{1, 2}, {2, 2}]}
    actions: [Send(1, AppendEntries {term: 11; leader_commit: 1; prev_log_index: 1; prev_log_term: 11; entries_length: 1; entries:
                                                           [{command: Command(Read m2, 2); term : 11}]})
    Send(2, AppendEntries {term: 11; leader_commit: 1; prev_log_index: 1; prev_log_term: 11; entries_length: 1; entries:
                                                           [{command: Command(Read m2, 2); term : 11}]})] |}] ;
  (* ------------ elect 1 ----------------- *)
  let t1, _ = Impl.advance t1 Tick in
  let t1, _ = Impl.advance t1 Tick in
  let t1, _ = Impl.advance t1 Tick in
  let t1, _ = Impl.advance t1 Tick in
  let t1, actions = Impl.advance t1 Tick in
  pp_res t1 actions ;
  [%expect
    {|
    +Candidate for term 12
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11}]; commit_index:-1; current_term: 12; node_state:Candidate{quorum:{threshold 1, elts:
    []}, timeout:5, repeat:1}}
    actions: [Broadcast(RequestVote {term:12; lastIndex:1; lastTerm:11})] |}] ;
  let rv = RequestVote {term= 12; lastIndex= 1; lastTerm= 11} in
  let t2, actions = Impl.advance t2 (Recv (rv, 1)) in
  pp_res t2 actions ;
  [%expect
    {|
    +Follower for term 12
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11}]; commit_index:-1; current_term: 12; node_state:Follower{timeout:5; voted_for:1}}
    actions:
    [Send(1, RequestVoteResponse {term:12; success:true})] |}] ;
  let rvr = RequestVoteResponse {term= 12; success= true} in
  let t1, actions = Impl.advance t1 (Recv (rvr, 2)) in
  pp_res t1 actions ;
  [%expect
    {|
    +Leader for term 12
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(NoOp, -1); term : 12}]; commit_index:-1; current_term: 12; node_state:Leader{heartbeat:0; rep_ackd:
    [{0, -1}, {2, -1}]; rep_sent:[{0, 2}, {2, 2}]}
    actions: [Send(0, AppendEntries {term: 12; leader_commit: -1; prev_log_index: 1; prev_log_term: 11; entries_length: 1; entries:
                                                             [{command: Command(NoOp, -1); term : 12}]})
    Send(2, AppendEntries {term: 12; leader_commit: -1; prev_log_index: 1; prev_log_term: 11; entries_length: 1; entries:
                                                             [{command: Command(NoOp, -1); term : 12}]})] |}] ;
  (* ------------ Add m4 ----------------- *)
  let m4 = C.Types.(make_command (Read "m4")) in
  let t1, actions = Impl.advance t1 (Commands (m4 |> Iter.singleton)) in
  pp_res t1 actions ;
  [%expect
    {|
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(NoOp, -1); term : 12},{command: Command(Read m4, 3); term : 12}]; commit_index:-1; current_term: 12; node_state:Leader{heartbeat:0; rep_ackd:
    [{0, -1}, {2, -1}]; rep_sent:[{0, 3}, {2, 3}]}
    actions: [Send(0, AppendEntries {term: 12; leader_commit: -1; prev_log_index: 2; prev_log_term: 12; entries_length: 1; entries:
                                                             [{command: Command(Read m4, 3); term : 12}]})
    Send(2, AppendEntries {term: 12; leader_commit: -1; prev_log_index: 2; prev_log_term: 12; entries_length: 1; entries:
                                                             [{command: Command(Read m4, 3); term : 12}]})] |}] ;
  let aem4 =
    AppendEntries
      { term= 12
      ; leader_commit= -1
      ; prev_log_index= 1
      ; prev_log_term= 11
      ; entries=
          ( C.Types.[{command= empty_command; term= 12}; {command= m4; term= 12}]
            |> Iter.of_list
          , 2 ) }
  in
  let t2, actions = Impl.advance t2 (Recv (aem4, 1)) in
  pp_res t2 actions ;
  [%expect
    {|
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(NoOp, -1); term : 12},{command: Command(Read m4, 3); term : 12}]; commit_index:-1; current_term: 12; node_state:Follower{timeout:5; voted_for:1}}
    actions:
    [Send(1, AppendEntriesResponse {term: 12; success: Ok: 3})] |}] ;
  let aerm4 = AppendEntriesResponse {term= 12; success= Ok 3} in
  let t1, actions = Impl.advance t1 (Recv (aerm4, 2)) in
  pp_res t1 actions ;
  [%expect
    {|
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(NoOp, -1); term : 12},{command: Command(Read m4, 3); term : 12}]; commit_index:3; current_term: 12; node_state:Leader{heartbeat:0; rep_ackd:
    [{0, -1}, {2, 3}]; rep_sent:[{0, 3}, {2, 3}]}
    actions: [CommitCommands(Command(NoOp, -1),
                                                            Command(Read m1, 1),
                                                            Command(NoOp, -1),
                                                            Command(Read m4, 3))] |}] ;
  (* ------------ Add m5,m6 ----------------- *)
  let m5 = C.Types.(make_command (Read "m5")) in
  let m6 = C.Types.(make_command (Read "m6")) in
  let t1, _ = Impl.advance t1 (Commands (Iter.of_list [m5; m6])) in
  Fmt.pr "t: %a\n" t_pp t1 ;
  [%expect
    {|
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(NoOp, -1); term : 12},{command: Command(Read m4, 3); term : 12},{command: Command(Read m5, 4); term : 12},{command: Command(Read m6, 5); term : 12}]; commit_index:3; current_term: 12; node_state:Leader{heartbeat:0; rep_ackd:
    [{0, -1}, {2, 3}]; rep_sent:[{0, 5}, {2, 5}]} |}] ;
  (* ------------ elect 2 with 0 ----------------- *)
  let t2, _ = Impl.advance t2 Tick in
  let t2, _ = Impl.advance t2 Tick in
  let t2, _ = Impl.advance t2 Tick in
  let t2, _ = Impl.advance t2 Tick in
  let t2, actions = Impl.advance t2 Tick in
  pp_res t2 actions ;
  [%expect
    {|
    +Candidate for term 13
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(NoOp, -1); term : 12},{command: Command(Read m4, 3); term : 12}]; commit_index:-1; current_term: 13; node_state:Candidate{quorum:{threshold 1, elts:
    []}, timeout:5, repeat:1}}
    actions: [Broadcast(RequestVote {term:13; lastIndex:3; lastTerm:12})] |}] ;
  let rv = RequestVote {term= 13; lastIndex= 3; lastTerm= 12} in
  let t, actions = Impl.advance t (Recv (rv, 2)) in
  pp_res t actions ;
  [%expect
    {|
    +Follower for term 13
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(Read m2, 2); term : 11}]; commit_index:1; current_term: 13; node_state:Follower{timeout:5; voted_for:2}}
    actions:
    [Send(2, RequestVoteResponse {term:13; success:true})] |}] ;
  let rvr = RequestVoteResponse {term= 13; success= true} in
  let t2, actions = Impl.advance t2 (Recv (rvr, 0)) in
  pp_res t2 actions ;
  [%expect
    {|
    +Leader for term 13
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(NoOp, -1); term : 12},{command: Command(Read m4, 3); term : 12},{command: Command(NoOp, -1); term : 13}]; commit_index:-1; current_term: 13; node_state:Leader{heartbeat:0; rep_ackd:
    [{0, -1}, {1, -1}]; rep_sent:[{0, 4}, {1, 4}]}
    actions: [Send(0, AppendEntries {term: 13; leader_commit: -1; prev_log_index: 3; prev_log_term: 12; entries_length: 1; entries:
                                                             [{command: Command(NoOp, -1); term : 13}]})
    Send(1, AppendEntries {term: 13; leader_commit: -1; prev_log_index: 3; prev_log_term: 12; entries_length: 1; entries:
                                                             [{command: Command(NoOp, -1); term : 13}]})] |}] ;
  (* ------------ Add m7 ----------------- *)
  let m7 = C.Types.(make_command (Read "m7")) in
  let t2, _ = Impl.advance t2 (Commands (Iter.singleton m7)) in
  Fmt.pr "t: %a\n" t_pp t2 ;
  [%expect
    {|
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(NoOp, -1); term : 12},{command: Command(Read m4, 3); term : 12},{command: Command(NoOp, -1); term : 13},{command: Command(Read m7, 6); term : 13}]; commit_index:-1; current_term: 13; node_state:Leader{heartbeat:0; rep_ackd:
    [{0, -1}, {1, -1}]; rep_sent:[{0, 5}, {1, 5}]} |}] ;
  (* ---------- Elect all nodes using votes from other 2 faked out ---------- *)
  let t, _ =
    Impl.advance t (Recv (RequestVote {term= 100; lastIndex= 5; lastTerm= 5}, 1))
  in
  Fmt.pr "t0:\n%a\n\n" t_pp t ;
  let t1, _ =
    Impl.advance t1
      (Recv (RequestVote {term= 100; lastIndex= 5; lastTerm= 5}, 2))
  in
  Fmt.pr "t1:\n%a\n\n" t_pp t1 ;
  let t2, _ =
    Impl.advance t2
      (Recv (RequestVote {term= 100; lastIndex= 5; lastTerm= 5}, 2))
  in
  Fmt.pr "t2:\n%a\n" t_pp t2 ;
  [%expect
    {|
    +Follower for term 100
    +Follower for term 100
    +Follower for term 100
    t0:
    {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(Read m2, 2); term : 11}]; commit_index:1; current_term: 100; node_state:Follower{timeout:5; voted_for:None}}

    t1:
    {log:
    [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(NoOp, -1); term : 12},{command: Command(Read m4, 3); term : 12},{command: Command(Read m5, 4); term : 12},{command: Command(Read m6, 5); term : 12}]; commit_index:3; current_term: 100; node_state:Follower{timeout:5; voted_for:None}}

    t2:
    {log:
    [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(NoOp, -1); term : 12},{command: Command(Read m4, 3); term : 12},{command: Command(NoOp, -1); term : 13},{command: Command(Read m7, 6); term : 13}]; commit_index:-1; current_term: 100; node_state:Follower{timeout:5; voted_for:None}} |}] ;
  let rvr term = RequestVoteResponse {term; success= true} in
  (* elect 0 *)
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  let t, actions = Impl.advance t Tick in
  pp_res t actions ;
  [%expect
    {|
    +Candidate for term 101
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(Read m2, 2); term : 11}]; commit_index:1; current_term: 101; node_state:Candidate{quorum:{threshold 1, elts:
    []}, timeout:5, repeat:1}}
    actions: [Broadcast(RequestVote {term:101; lastIndex:2; lastTerm:11})] |}] ;
  let t, actions = Impl.advance t (Recv (rvr 101, 1)) in
  pp_res t actions ;
  [%expect
    {|
    +Leader for term 101
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(Read m2, 2); term : 11},{command: Command(NoOp, -1); term : 101}]; commit_index:1; current_term: 101; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, -1}, {2, -1}]; rep_sent:[{1, 3}, {2, 3}]}
    actions: [Send(1, AppendEntries {term: 101; leader_commit: 1; prev_log_index: 2; prev_log_term: 11; entries_length: 1; entries:
                                                             [{command: Command(NoOp, -1); term : 101}]})
    Send(2, AppendEntries {term: 101; leader_commit: 1; prev_log_index: 2; prev_log_term: 11; entries_length: 1; entries:
                                                             [{command: Command(NoOp, -1); term : 101}]})] |}] ;
  (* elect 1 *)
  let t1, _ = Impl.advance t1 Tick in
  let t1, _ = Impl.advance t1 Tick in
  let t1, _ = Impl.advance t1 Tick in
  let t1, _ = Impl.advance t1 Tick in
  let t1, actions = Impl.advance t1 Tick in
  pp_res t1 actions ;
  [%expect
    {|
    +Candidate for term 101
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(NoOp, -1); term : 12},{command: Command(Read m4, 3); term : 12},{command: Command(Read m5, 4); term : 12},{command: Command(Read m6, 5); term : 12}]; commit_index:3; current_term: 101; node_state:Candidate{quorum:{threshold 1, elts:
    []}, timeout:5, repeat:1}}
    actions: [Broadcast(RequestVote {term:101; lastIndex:5; lastTerm:12})] |}] ;
  let t1, actions = Impl.advance t1 (Recv (rvr 101, 2)) in
  pp_res t1 actions ;
  [%expect
    {|
    +Leader for term 101
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(NoOp, -1); term : 12},{command: Command(Read m4, 3); term : 12},{command: Command(Read m5, 4); term : 12},{command: Command(Read m6, 5); term : 12},{command: Command(NoOp, -1); term : 101}]; commit_index:3; current_term: 101; node_state:Leader{heartbeat:0; rep_ackd:
    [{0, -1}, {2, -1}]; rep_sent:[{0, 6}, {2, 6}]}
    actions: [Send(0, AppendEntries {term: 101; leader_commit: 3; prev_log_index: 5; prev_log_term: 12; entries_length: 1; entries:
                                                             [{command: Command(NoOp, -1); term : 101}]})
    Send(2, AppendEntries {term: 101; leader_commit: 3; prev_log_index: 5; prev_log_term: 12; entries_length: 1; entries:
                                                             [{command: Command(NoOp, -1); term : 101}]})] |}] ;
  (* elect 2 *)
  let t2, _ = Impl.advance t2 Tick in
  let t2, _ = Impl.advance t2 Tick in
  let t2, _ = Impl.advance t2 Tick in
  let t2, _ = Impl.advance t2 Tick in
  let t2, actions = Impl.advance t2 Tick in
  pp_res t2 actions ;
  [%expect
    {|
    +Candidate for term 101
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(NoOp, -1); term : 12},{command: Command(Read m4, 3); term : 12},{command: Command(NoOp, -1); term : 13},{command: Command(Read m7, 6); term : 13}]; commit_index:-1; current_term: 101; node_state:Candidate{quorum:{threshold 1, elts:
    []}, timeout:5, repeat:1}}
    actions: [Broadcast(RequestVote {term:101; lastIndex:5; lastTerm:13})] |}] ;
  let t2, actions = Impl.advance t2 (Recv (rvr 101, 0)) in
  pp_res t2 actions ;
  [%expect
    {|
    +Leader for term 101
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(NoOp, -1); term : 12},{command: Command(Read m4, 3); term : 12},{command: Command(NoOp, -1); term : 13},{command: Command(Read m7, 6); term : 13},{command: Command(NoOp, -1); term : 101}]; commit_index:-1; current_term: 101; node_state:Leader{heartbeat:0; rep_ackd:
    [{0, -1}, {1, -1}]; rep_sent:[{0, 6}, {1, 6}]}
    actions: [Send(0, AppendEntries {term: 101; leader_commit: -1; prev_log_index: 5; prev_log_term: 13; entries_length: 1; entries:
                                                             [{command: Command(NoOp, -1); term : 101}]})
    Send(1, AppendEntries {term: 101; leader_commit: -1; prev_log_index: 5; prev_log_term: 13; entries_length: 1; entries:
                                                             [{command: Command(NoOp, -1); term : 101}]})] |}] ;
  ignore (t, t1, t2) ;
  ()

let%expect_test "Missing elements" =
  reset_make_command_state ();
  (* --------- elect t and add 2 commands to log then send out later one --------- *)
  let t = create (c3 0) in
  let t1 = create (c3 1) in
  let rv = RequestVote {term= 10; lastIndex= 5; lastTerm= 5} in
  let t, _ = Impl.advance t (Recv (rv, 2)) in
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  let rv = RequestVote {term= 11; lastIndex= 5; lastTerm= 5} in
  let t1, _ = Impl.advance t1 (Recv (rv, 2)) in
  let rvr = RequestVoteResponse {term= 11; success= true} in
  let t, actions = Impl.advance t (Recv (rvr, 1)) in
  pp_res t actions ;
  [%expect
    {|
    +Follower for term 10
    +Candidate for term 11
    +Follower for term 11
    +Leader for term 11
    t: {log: [{command: Command(NoOp, -1); term : 11}]; commit_index:-1; current_term: 11; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, -1}, {2, -1}]; rep_sent:[{1, 0}, {2, 0}]}
    actions: [Send(1, AppendEntries {term: 11; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 1; entries:
                                                             [{command: Command(NoOp, -1); term : 11}]})
    Send(2, AppendEntries {term: 11; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 1; entries:
                                                             [{command: Command(NoOp, -1); term : 11}]})] |}] ;
  (* ------------ Add commands ----------------- *)
  let m1 = C.Types.(make_command (Read "m1")) in
  let t, _ = Impl.advance t (Commands (m1 |> Iter.singleton)) in
  (* -- We don't deliver m1 -- *)
  let m2 = C.Types.(make_command (Read "m2")) in
  let t, actions = Impl.advance t (Commands (m2 |> Iter.singleton)) in
  pp_res t actions ;
  [%expect
    {|
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(Read m2, 2); term : 11}]; commit_index:-1; current_term: 11; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, -1}, {2, -1}]; rep_sent:[{1, 2}, {2, 2}]}
    actions: [Send(1, AppendEntries {term: 11; leader_commit: -1; prev_log_index: 1; prev_log_term: 11; entries_length: 1; entries:
                                                             [{command: Command(Read m2, 2); term : 11}]})
    Send(2, AppendEntries {term: 11; leader_commit: -1; prev_log_index: 1; prev_log_term: 11; entries_length: 1; entries:
                                                             [{command: Command(Read m2, 2); term : 11}]})] |}] ;
  let aem2 =
    AppendEntries
      { term= 11
      ; leader_commit= -1
      ; prev_log_index= 0
      ; prev_log_term= 11
      ; entries= (C.Types.{command= m2; term= 11} |> Iter.singleton, 1) }
  in
  pp_res t1 [] ;
  [%expect
    {|
    t: {log: []; commit_index:-1; current_term: 11; node_state:Follower{timeout:5; voted_for:2}}
    actions:
    [] |}] ;
  let t1, actions = Impl.advance t1 (Recv (aem2, 0)) in
  pp_res t1 actions ;
  [%expect
    {|
    t: {log: []; commit_index:-1; current_term: 11; node_state:Follower{timeout:5; voted_for:0}}
    actions:
    [Send(0, AppendEntriesResponse {term: 11; success: Error: -1})] |}] ;
  let aer = AppendEntriesResponse {term= 11; success= Error (-1)} in
  let t, actions = Impl.advance t (Recv (aer, 1)) in
  pp_res t actions ;
  [%expect
    {|
    t: {log: [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(Read m2, 2); term : 11}]; commit_index:-1; current_term: 11; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, -1}, {2, -1}]; rep_sent:[{1, 2}, {2, 2}]}
    actions: [Send(1, AppendEntries {term: 11; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 3; entries:
                                                             [{command: Command(NoOp, -1); term : 11},{command: Command(Read m1, 1); term : 11},{command: Command(Read m2, 2); term : 11}]})] |}]

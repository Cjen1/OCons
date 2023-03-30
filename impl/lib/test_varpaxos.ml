open! Types
open! Utils
open! Actions_f
module Impl = Var_paxos.MakePaxos (ImperativeActions)
module Imp = Impl.AT
open! Impl.PaxosTypes
open! Impl

let c1 = make_config ~node_id:0 ~node_list:[0] ~election_timeout:5 ()

let c3 node_id =
  make_config ~node_id ~node_list:[0; 1; 2] ~election_timeout:5 ()

let make_command_id c id : command = Command.{op= c; id; trace_start = -1.}

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
  [%expect
    {|
    +Follower for term 10
    state: Follower{timeout:0; voted_for:None}
    term: 10
    actions: [] |}]

let%expect_test "transit_candidate" =
  let t = create (c3 0) in
  Fmt.pr "t0: %a\n" t_pp t ;
  [%expect
    {| t0: {log: []; commit_index:-1; current_term: 0; node_state:Follower{timeout:5; voted_for:None}} |}] ;
  let t', actions = Imp.run_side_effects Impl.transit_candidate t in
  Fmt.pr "t': %a\n" t_pp t' ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    +Candidate for term 3
    t': {log: []; commit_index:-1; current_term: 3; node_state:Candidate{quorum:{threshold 1, elts:
    []}, timeout:0}}
    actions: [Broadcast(RequestVote {term:3; leader_commit:-1})] |}] ;
  let t', actions = Imp.run_side_effects Impl.transit_candidate t' in
  Fmt.pr "t': %a\n" t_pp t' ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    +Candidate for term 6
    t': {log: []; commit_index:-1; current_term: 6; node_state:Candidate{quorum:{threshold 1, elts:
    []}, timeout:0}}
    actions: [Broadcast(RequestVote {term:6; leader_commit:-1})] |}]

let%expect_test "transit_leader" =
  let t = create (c3 0) in
  let t', _ = Imp.run_side_effects Impl.transit_candidate t in
  let t', actions = Imp.run_side_effects Impl.transit_leader t' in
  Fmt.pr "t': %a\n" t_pp t' ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    +Candidate for term 3
    +Leader for term 3
    t': {log: []; commit_index:-1; current_term: 3; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, -1}, {2, -1}]; rep_sent:[{1, -1}, {2, -1}]}
    actions: [Send(1, AppendEntries {term: 3; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 0; entries:
                                                               []})
    Send(2, AppendEntries {term: 3; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 0; entries:
                                                               []})] |}]

let%expect_test "request vote from higher" =
  let t = create (c3 0) in
  let rv = RequestVote {term= 10; leader_commit= -1} in
  (* from follower *)
  let t', actions = Impl.advance t (Recv (rv, 2)) in
  Fmt.pr "t': %a\n" t_pp t' ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    +Follower for term 10
    t': {log: []; commit_index:-1; current_term: 10; node_state:Follower{timeout:0; voted_for:None}}
    actions:
    [Send(2, RequestVoteResponse {term:10; start_index:0; entries_length:0; entries:
     []})] |}] ;
  (* from candidate *)
  let t', _ = Imp.run_side_effects Impl.transit_candidate t in
  let t', actions = Impl.advance t' (Recv (rv, 2)) in
  Fmt.pr "t': %a\n" t_pp t' ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    +Candidate for term 3
    +Follower for term 10
    t': {log: []; commit_index:-1; current_term: 10; node_state:Follower{timeout:0; voted_for:None}}
    actions:
    [Send(2, RequestVoteResponse {term:10; start_index:0; entries_length:0; entries:
     []})] |}] ;
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
    +Candidate for term 3
    +Leader for term 3
    +Follower for term 10
    t': {log: []; commit_index:-1; current_term: 10; node_state:Follower{timeout:0; voted_for:None}}
    actions:
    [Send(2, RequestVoteResponse {term:10; start_index:0; entries_length:0; entries:
     []})] |}]

let pp_res t actions =
  Fmt.pr "t: %a\n" t_pp t ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions

let%expect_test "Loop" =
  let t = create (c3 0) in
  let rv = RequestVote {term= 10; leader_commit= -1} in
  (* --------- Get t to start election --------- *)
  let t, _ = Impl.advance t (Recv (rv, 2)) in
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  Fmt.pr "node_state: %a\n" node_state_pp (A.get node_state t) ;
  [%expect {|
    +Follower for term 10
    node_state: Follower{timeout:4; voted_for:None} |}] ;
  let t, actions = Impl.advance t Tick in
  Fmt.pr "node_state: %a\n" t_pp t ;
  [%expect
    {|
    +Candidate for term 12
    node_state: {log: []; commit_index:-1; current_term: 12; node_state:Candidate{quorum:{threshold 1, elts:
    []}, timeout:0}} |}] ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    actions: [Broadcast(RequestVote {term:12; leader_commit:-1})] |}] ;
  let rv = RequestVote {term= 12; leader_commit= -1} in
  let t1 = create (c3 1) in
  let t1, a1 = Impl.advance t1 (Recv (rv, 2)) in
  Fmt.pr "a1: %a\n" Fmt.(brackets @@ list ~sep:(const string "\n") action_pp) a1 ;
  [%expect
    {|
    +Follower for term 12
    a1: [Send(2, RequestVoteResponse {term:12; start_index:0; entries_length:0; entries:
         []})] |}] ;
  let t2 = create (c3 2) in
  let t2, a2 = Impl.advance t2 (Recv (rv, 2)) in
  Fmt.pr "a2: %a\n" Fmt.(brackets @@ list ~sep:(const string "\n") action_pp) a2 ;
  [%expect
    {|
    +Follower for term 12
    a2: [Send(2, RequestVoteResponse {term:12; start_index:0; entries_length:0; entries:
         []})] |}] ;
  let rvr e =
    RequestVoteResponse
      {term= 12; start_index= 0; entries= (e |> Iter.of_list, List.length e)}
  in
  let t, actions = Impl.advance t (Recv (rvr [], 1)) in
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    +Leader for term 12
    actions: [Send(1, AppendEntries {term: 12; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 0; entries:
              []})
    Send(2, AppendEntries {term: 12; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 0; entries:
              []})] |}] ;
  let t', actions =
    Imp.run_side_effects (fun () -> Impl.resolve_event (Recv (rvr [], 2))) t
  in
  Fmt.pr "t: %a\n" t_pp t' ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    t: {log: []; commit_index:-1; current_term: 12; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, -1}, {2, -1}]; rep_sent:[{1, -1}, {2, -1}]}
    actions: [] |}] ;
  let t, actions = Impl.advance t (Recv (rvr [], 2)) in
  Fmt.pr "t: %a\n" t_pp t ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    t: {log: []; commit_index:-1; current_term: 12; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, -1}, {2, -1}]; rep_sent:[{1, -1}, {2, -1}]}
    actions: [] |}] ;
  (* ------------ Add m1 ----------------- *)
  let m1 = C.Types.(make_command_id (Read "m1") 1) in
  let t, actions = Impl.advance t (Commands (m1 |> Iter.singleton)) in
  Fmt.pr "t: %a\n" t_pp t ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    t: {log: [{command: Command(Read m1, 1); term : 12}]; commit_index:-1; current_term: 12; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, -1}, {2, -1}]; rep_sent:[{1, 0}, {2, 0}]}
    actions: [Send(1, AppendEntries {term: 12; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 1; entries:
                                                             [{command: Command(Read m1, 1); term : 12}]})
    Send(2, AppendEntries {term: 12; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 1; entries:
                                                             [{command: Command(Read m1, 1); term : 12}]})] |}] ;
  let aem1 =
    AppendEntries
      { term= 12
      ; leader_commit= -1
      ; prev_log_index= -1
      ; prev_log_term= 0
      ; entries= (C.Types.{command= m1; term= 12} |> Iter.singleton, 1) }
  in
  let t1, a1 = Impl.advance t1 (Recv (aem1, 0)) in
  Fmt.pr "t1: %a\n" t_pp t1 ;
  Fmt.pr "a1: %a\n" Fmt.(brackets @@ list ~sep:(const string "\n") action_pp) a1 ;
  [%expect
    {|
    t1: {log: [{command: Command(Read m1, 1); term : 12}]; commit_index:-1; current_term: 12; node_state:Follower{timeout:0; voted_for:0}}
    a1:
    [Send(0, AppendEntriesResponse {term: 12; success: Ok: 0})] |}] ;
  let t2, a2 = Impl.advance t2 (Recv (aem1, 0)) in
  Fmt.pr "t2: %a\n" t_pp t2 ;
  Fmt.pr "a2: %a\n" Fmt.(brackets @@ list ~sep:(const string "\n") action_pp) a2 ;
  [%expect
    {|
    t2: {log: [{command: Command(Read m1, 1); term : 12}]; commit_index:-1; current_term: 12; node_state:Follower{timeout:0; voted_for:0}}
    a2:
    [Send(0, AppendEntriesResponse {term: 12; success: Ok: 0})] |}] ;
  let aer = AppendEntriesResponse {term= 12; success= Ok 0} in
  let t, _ = Impl.advance t (Recv (aer, 1)) in
  let t, actions = Impl.advance t (Recv (aer, 2)) in
  Fmt.pr "t: %a\n" t_pp t ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    t: {log: [{command: Command(Read m1, 1); term : 12}]; commit_index:0; current_term: 12; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, 0}, {2, 0}]; rep_sent:[{1, 0}, {2, 0}]}
    actions: [] |}] ;
  (* ------------ Add m2 ----------------- *)
  let m2 = C.Types.(make_command_id (Read "m2") 2) in
  let t, actions = Impl.advance t (Commands (m2 |> Iter.singleton)) in
  Fmt.pr "t: %a\n" t_pp t ;
  Fmt.pr "actions: %a\n"
    Fmt.(brackets @@ list ~sep:(const string "\n") action_pp)
    actions ;
  [%expect
    {|
    t: {log: [{command: Command(Read m1, 1); term : 12},{command: Command(Read m2, 2); term : 12}]; commit_index:0; current_term: 12; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, 0}, {2, 0}]; rep_sent:[{1, 1}, {2, 1}]}
    actions: [Send(1, AppendEntries {term: 12; leader_commit: 0; prev_log_index: 0; prev_log_term: 12; entries_length: 1; entries:
                                                           [{command: Command(Read m2, 2); term : 12}]})
    Send(2, AppendEntries {term: 12; leader_commit: 0; prev_log_index: 0; prev_log_term: 12; entries_length: 1; entries:
                                                           [{command: Command(Read m2, 2); term : 12}]})] |}] ;
  (* ------------ elect 1 ----------------- *)
  let t1, _ = Impl.advance t1 Tick in
  let t1, _ = Impl.advance t1 Tick in
  let t1, _ = Impl.advance t1 Tick in
  let t1, _ = Impl.advance t1 Tick in
  let t1, actions = Impl.advance t1 Tick in
  pp_res t1 actions ;
  [%expect
    {|
    +Candidate for term 13
    t: {log: [{command: Command(Read m1, 1); term : 12}]; commit_index:-1; current_term: 13; node_state:Candidate{quorum:{threshold 1, elts:
    []}, timeout:0}}
    actions: [Broadcast(RequestVote {term:13; leader_commit:-1})] |}] ;
  let rv = RequestVote {term= 13; leader_commit= -1} in
  let t2, actions = Impl.advance t2 (Recv (rv, 1)) in
  pp_res t2 actions ;
  [%expect
    {|
    +Follower for term 13
    t: {log: [{command: Command(Read m1, 1); term : 12}]; commit_index:-1; current_term: 13; node_state:Follower{timeout:0; voted_for:None}}
    actions:
    [Send(1, RequestVoteResponse {term:13; start_index:0; entries_length:1; entries:
     [{command: Command(Read m1, 1); term : 12}]})] |}] ;
  let rvr =
    RequestVoteResponse
      { term= 13
      ; start_index= 0
      ; entries= (Iter.singleton C.Types.{command= m1; term= 12}, 1) }
  in
  let t1, actions = Impl.advance t1 (Recv (rvr, 2)) in
  pp_res t1 actions ;
  [%expect
    {|
    +Leader for term 13
    t: {log: [{command: Command(Read m1, 1); term : 13}]; commit_index:-1; current_term: 13; node_state:Leader{heartbeat:0; rep_ackd:
    [{0, -1}, {2, -1}]; rep_sent:[{0, 0}, {2, 0}]}
    actions: [Send(0, AppendEntries {term: 13; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 1; entries:
                                                             [{command: Command(Read m1, 1); term : 13}]})
    Send(2, AppendEntries {term: 13; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 1; entries:
                                                             [{command: Command(Read m1, 1); term : 13}]})] |}] ;
  (* ------------ Add m4 ----------------- *)
  let m4 = C.Types.(make_command_id (Read "m4") 4) in
  let t1, actions = Impl.advance t1 (Commands (m4 |> Iter.singleton)) in
  pp_res t1 actions ;
  [%expect
    {|
    t: {log: [{command: Command(Read m1, 1); term : 13},{command: Command(Read m4, 4); term : 13}]; commit_index:-1; current_term: 13; node_state:Leader{heartbeat:0; rep_ackd:
    [{0, -1}, {2, -1}]; rep_sent:[{0, 1}, {2, 1}]}
    actions: [Send(0, AppendEntries {term: 13; leader_commit: -1; prev_log_index: 0; prev_log_term: 13; entries_length: 1; entries:
                                                             [{command: Command(Read m4, 4); term : 13}]})
    Send(2, AppendEntries {term: 13; leader_commit: -1; prev_log_index: 0; prev_log_term: 13; entries_length: 1; entries:
                                                             [{command: Command(Read m4, 4); term : 13}]})] |}] ;
  let aem4 =
    AppendEntries
      { term= 13
      ; leader_commit= -1
      ; prev_log_index= 0
      ; prev_log_term= 12
      ; entries= (Iter.singleton C.Types.{command= m4; term= 13}, 1) }
  in
  let t2, actions = Impl.advance t2 (Recv (aem4, 1)) in
  pp_res t2 actions ;
  [%expect
    {|
    t: {log: [{command: Command(Read m1, 1); term : 12},{command: Command(Read m4, 4); term : 13}]; commit_index:-1; current_term: 13; node_state:Follower{timeout:0; voted_for:1}}
    actions:
    [Send(1, AppendEntriesResponse {term: 13; success: Ok: 1})] |}] ;
  let aerm4 = AppendEntriesResponse {term= 13; success= Ok 1} in
  let t1, actions = Impl.advance t1 (Recv (aerm4, 2)) in
  pp_res t1 actions ;
  [%expect
    {|
    t: {log: [{command: Command(Read m1, 1); term : 13},{command: Command(Read m4, 4); term : 13}]; commit_index:1; current_term: 13; node_state:Leader{heartbeat:0; rep_ackd:
    [{0, -1}, {2, 1}]; rep_sent:[{0, 1}, {2, 1}]}
    actions: [CommitCommands[Command(Read m1, 1)
                                                            Command(Read m4, 4)]] |}] ;
  (* ------------ Add m5,m6 ----------------- *)
  let m5 = C.Types.(make_command_id (Read "m5") 5) in
  let m6 = C.Types.(make_command_id (Read "m6") 6) in
  let t1, _ = Impl.advance t1 (Commands (Iter.of_list [m5; m6])) in
  Fmt.pr "t: %a\n" t_pp t1 ;
  [%expect
    {|
    t: {log: [{command: Command(Read m1, 1); term : 13},{command: Command(Read m4, 4); term : 13},{command: Command(Read m5, 5); term : 13},{command: Command(Read m6, 6); term : 13}]; commit_index:1; current_term: 13; node_state:Leader{heartbeat:0; rep_ackd:
    [{0, -1}, {2, 1}]; rep_sent:[{0, 3}, {2, 3}]} |}] ;
  (* ------------ elect 2 with 0 ----------------- *)
  let t2, _ = Impl.advance t2 Tick in
  let t2, _ = Impl.advance t2 Tick in
  let t2, _ = Impl.advance t2 Tick in
  let t2, _ = Impl.advance t2 Tick in
  let t2, actions = Impl.advance t2 Tick in
  pp_res t2 actions ;
  [%expect
    {|
    +Candidate for term 14
    t: {log: [{command: Command(Read m1, 1); term : 12},{command: Command(Read m4, 4); term : 13}]; commit_index:-1; current_term: 14; node_state:Candidate{quorum:{threshold 1, elts:
    []}, timeout:0}}
    actions: [Broadcast(RequestVote {term:14; leader_commit:-1})] |}] ;
  let rv = RequestVote {term= 14; leader_commit= -1} in
  let t, actions = Impl.advance t (Recv (rv, 2)) in
  pp_res t actions ;
  [%expect
    {|
    +Follower for term 14
    t: {log: [{command: Command(Read m1, 1); term : 12},{command: Command(Read m2, 2); term : 12}]; commit_index:0; current_term: 14; node_state:Follower{timeout:0; voted_for:None}}
    actions:
    [Send(2, RequestVoteResponse {term:14; start_index:0; entries_length:2; entries:
     [{command: Command(Read m1, 1); term : 12},{command: Command(Read m2, 2); term : 12}]})] |}] ;
  let rvr =
    RequestVoteResponse
      { term= 14
      ; start_index= 0
      ; entries=
          ( Iter.of_list
              [C.Types.{command= m1; term= 12}; C.Types.{command= m2; term= 12}]
          , 2 ) }
  in
  let t2, actions = Impl.advance t2 (Recv (rvr, 0)) in
  pp_res t2 actions ;
  [%expect
    {|
    +Leader for term 14
    t: {log: [{command: Command(Read m1, 1); term : 14},{command: Command(Read m4, 4); term : 14}]; commit_index:-1; current_term: 14; node_state:Leader{heartbeat:0; rep_ackd:
    [{0, -1}, {1, -1}]; rep_sent:[{0, 1}, {1, 1}]}
    actions: [Send(0, AppendEntries {term: 14; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 2; entries:
                                                             [{command: Command(Read m1, 1); term : 14},{command: Command(Read m4, 4); term : 14}]})
    Send(1, AppendEntries {term: 14; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 2; entries:
                                                             [{command: Command(Read m1, 1); term : 14},{command: Command(Read m4, 4); term : 14}]})] |}] ;
  (* ------------ Add m7 ----------------- *)
  let m7 = C.Types.(make_command_id (Read "m7") 7) in
  let t2, _ = Impl.advance t2 (Commands (Iter.singleton m7)) in
  Fmt.pr "t: %a\n" t_pp t2 ;
  [%expect
    {|
    t: {log: [{command: Command(Read m1, 1); term : 14},{command: Command(Read m4, 4); term : 14},{command: Command(Read m7, 7); term : 14}]; commit_index:-1; current_term: 14; node_state:Leader{heartbeat:0; rep_ackd:
    [{0, -1}, {1, -1}]; rep_sent:[{0, 2}, {1, 2}]} |}] ;
  (* ---------- Elect all nodes using votes from other 2 faked out ---------- *)
  let t, _ =
    Impl.advance t (Recv (RequestVote {term= 100; leader_commit= -1}, 1))
  in
  Fmt.pr "t0:\n%a\n\n" t_pp t ;
  let t1, _ =
    Impl.advance t1 (Recv (RequestVote {term= 100; leader_commit= -1}, 2))
  in
  Fmt.pr "t1:\n%a\n\n" t_pp t1 ;
  let t2, _ =
    Impl.advance t2 (Recv (RequestVote {term= 100; leader_commit= -1}, 2))
  in
  Fmt.pr "t2:\n%a\n" t_pp t2 ;
  [%expect
    {|
    +Follower for term 100
    +Follower for term 100
    +Follower for term 100
    t0:
    {log: [{command: Command(Read m1, 1); term : 12},{command: Command(Read m2, 2); term : 12}]; commit_index:0; current_term: 100; node_state:Follower{timeout:0; voted_for:None}}

    t1:
    {log:
    [{command: Command(Read m1, 1); term : 13},{command: Command(Read m4, 4); term : 13},{command: Command(Read m5, 5); term : 13},{command: Command(Read m6, 6); term : 13}]; commit_index:1; current_term: 100; node_state:Follower{timeout:0; voted_for:None}}

    t2:
    {log:
    [{command: Command(Read m1, 1); term : 14},{command: Command(Read m4, 4); term : 14},{command: Command(Read m7, 7); term : 14}]; commit_index:-1; current_term: 100; node_state:Follower{timeout:0; voted_for:None}} |}] ;
  let rvr0 term =
    RequestVoteResponse
      { term
      ; start_index= 0
      ; entries=
          ( Iter.of_list
              [C.Types.{command= m1; term= 12}; C.Types.{command= m2; term= 12}]
          , 2 ) }
  in
  let rvr1 term =
    RequestVoteResponse
      { term
      ; start_index= 0
      ; entries=
          ( Iter.of_list
              C.Types.
                [ {command= m1; term= 12}
                ; {command= m4; term= 13}
                ; {command= m5; term= 13}
                ; {command= m6; term= 13} ]
          , 4 ) }
  in
  let rvr2 term =
    RequestVoteResponse
      { term
      ; start_index= 0
      ; entries=
          ( Iter.of_list
              C.Types.
                [ {command= m1; term= 12}
                ; {command= m4; term= 13}
                ; {command= m7; term= 14} ]
          , 3 ) }
  in
  (* elect 0 *)
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  let t, actions = Impl.advance t Tick in
  pp_res t actions ;
  [%expect
    {|
    +Candidate for term 102
    t: {log: [{command: Command(Read m1, 1); term : 12},{command: Command(Read m2, 2); term : 12}]; commit_index:0; current_term: 102; node_state:Candidate{quorum:{threshold 1, elts:
    []}, timeout:0}}
    actions: [Broadcast(RequestVote {term:102; leader_commit:0})] |}] ;
  let t, actions = Impl.advance t (Recv (rvr1 102, 1)) in
  pp_res t actions ;
  [%expect
    {|
    +Leader for term 102
    t: {log: [{command: Command(Read m1, 1); term : 12},{command: Command(Read m4, 4); term : 102},{command: Command(Read m5, 5); term : 102},{command: Command(Read m6, 6); term : 102}]; commit_index:0; current_term: 102; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, -1}, {2, -1}]; rep_sent:[{1, 3}, {2, 3}]}
    actions: [Send(1, AppendEntries {term: 102; leader_commit: 0; prev_log_index: 0; prev_log_term: 12; entries_length: 3; entries:
                                                             [{command: Command(Read m4, 4); term : 102},{command: Command(Read m5, 5); term : 102},{command: Command(Read m6, 6); term : 102}]})
    Send(2, AppendEntries {term: 102; leader_commit: 0; prev_log_index: 0; prev_log_term: 12; entries_length: 3; entries:
                                                             [{command: Command(Read m4, 4); term : 102},{command: Command(Read m5, 5); term : 102},{command: Command(Read m6, 6); term : 102}]})] |}] ;
  (* elect 1 *)
  let t1, _ = Impl.advance t1 Tick in
  let t1, _ = Impl.advance t1 Tick in
  let t1, _ = Impl.advance t1 Tick in
  let t1, _ = Impl.advance t1 Tick in
  let t1, actions = Impl.advance t1 Tick in
  pp_res t1 actions ;
  [%expect
    {|
    +Candidate for term 103
    t: {log: [{command: Command(Read m1, 1); term : 13},{command: Command(Read m4, 4); term : 13},{command: Command(Read m5, 5); term : 13},{command: Command(Read m6, 6); term : 13}]; commit_index:1; current_term: 103; node_state:Candidate{quorum:{threshold 1, elts:
    []}, timeout:0}}
    actions: [Broadcast(RequestVote {term:103; leader_commit:1})] |}] ;
  let t1, actions = Impl.advance t1 (Recv (rvr2 103, 2)) in
  pp_res t1 actions ;
  [%expect
    {|
    +Leader for term 103
    t: {log: [{command: Command(Read m1, 1); term : 13},{command: Command(Read m4, 4); term : 13},{command: Command(Read m7, 7); term : 103},{command: Command(Read m6, 6); term : 103}]; commit_index:1; current_term: 103; node_state:Leader{heartbeat:0; rep_ackd:
    [{0, -1}, {2, -1}]; rep_sent:[{0, 3}, {2, 3}]}
    actions: [Send(0, AppendEntries {term: 103; leader_commit: 1; prev_log_index: 1; prev_log_term: 13; entries_length: 2; entries:
                                                             [{command: Command(Read m7, 7); term : 103},{command: Command(Read m6, 6); term : 103}]})
    Send(2, AppendEntries {term: 103; leader_commit: 1; prev_log_index: 1; prev_log_term: 13; entries_length: 2; entries:
                                                             [{command: Command(Read m7, 7); term : 103},{command: Command(Read m6, 6); term : 103}]})] |}] ;
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
    t: {log: [{command: Command(Read m1, 1); term : 14},{command: Command(Read m4, 4); term : 14},{command: Command(Read m7, 7); term : 14}]; commit_index:-1; current_term: 101; node_state:Candidate{quorum:{threshold 1, elts:
    []}, timeout:0}}
    actions: [Broadcast(RequestVote {term:101; leader_commit:-1})] |}] ;
  let t2, actions = Impl.advance t2 (Recv (rvr0 101, 0)) in
  pp_res t2 actions ;
  [%expect
    {|
    +Leader for term 101
    t: {log: [{command: Command(Read m1, 1); term : 101},{command: Command(Read m4, 4); term : 101},{command: Command(Read m7, 7); term : 101}]; commit_index:-1; current_term: 101; node_state:Leader{heartbeat:0; rep_ackd:
    [{0, -1}, {1, -1}]; rep_sent:[{0, 2}, {1, 2}]}
    actions: [Send(0, AppendEntries {term: 101; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 3; entries:
                                                             [{command: Command(Read m1, 1); term : 101},{command: Command(Read m4, 4); term : 101},{command: Command(Read m7, 7); term : 101}]})
    Send(1, AppendEntries {term: 101; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 3; entries:
                                                             [{command: Command(Read m1, 1); term : 101},{command: Command(Read m4, 4); term : 101},{command: Command(Read m7, 7); term : 101}]})] |}] ;
  ignore (t, t1, t2) ;
  ()

let%expect_test "Missing elements" =
  (* --------- elect t and add 2 commands to log then send out later one --------- *)
  let t = create (c3 0) in
  let t1 = create (c3 1) in
  let rv = RequestVote {term= 10; leader_commit= -1} in
  let t, _ = Impl.advance t (Recv (rv, 2)) in
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  let t, _ = Impl.advance t Tick in
  let rv = RequestVote {term= 12; leader_commit= -1} in
  let t1, _ = Impl.advance t1 (Recv (rv, 2)) in
  let rvr =
    RequestVoteResponse {term= 12; start_index= 0; entries= (Iter.empty, 0)}
  in
  let t, actions = Impl.advance t (Recv (rvr, 1)) in
  pp_res t actions ;
  [%expect
    {|
    +Follower for term 10
    +Candidate for term 12
    +Follower for term 12
    +Leader for term 12
    t: {log: []; commit_index:-1; current_term: 12; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, -1}, {2, -1}]; rep_sent:[{1, -1}, {2, -1}]}
    actions: [Send(1, AppendEntries {term: 12; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 0; entries:
                                                               []})
    Send(2, AppendEntries {term: 12; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 0; entries:
                                                               []})] |}] ;
  (* ------------ Add commands ----------------- *)
  let m1 = C.Types.(make_command_id (Read "m1") 1) in
  let t, _ = Impl.advance t (Commands (m1 |> Iter.singleton)) in
  (* -- We don't deliver m1 -- *)
  let m2 = C.Types.(make_command_id (Read "m2") 2) in
  let t, actions = Impl.advance t (Commands (m2 |> Iter.singleton)) in
  pp_res t actions ;
  [%expect
    {|
    t: {log: [{command: Command(Read m1, 1); term : 12},{command: Command(Read m2, 2); term : 12}]; commit_index:-1; current_term: 12; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, -1}, {2, -1}]; rep_sent:[{1, 1}, {2, 1}]}
    actions: [Send(1, AppendEntries {term: 12; leader_commit: -1; prev_log_index: 0; prev_log_term: 12; entries_length: 1; entries:
                                                             [{command: Command(Read m2, 2); term : 12}]})
    Send(2, AppendEntries {term: 12; leader_commit: -1; prev_log_index: 0; prev_log_term: 12; entries_length: 1; entries:
                                                             [{command: Command(Read m2, 2); term : 12}]})] |}] ;
  let aem2 =
    AppendEntries
      { term= 12
      ; leader_commit= -1
      ; prev_log_index= 0
      ; prev_log_term= 12
      ; entries= (C.Types.{command= m2; term= 12} |> Iter.singleton, 1) }
  in
  pp_res t1 [] ;
  [%expect
    {|
    t: {log: []; commit_index:-1; current_term: 12; node_state:Follower{timeout:0; voted_for:None}}
    actions:
    [] |}] ;
  let t1, actions = Impl.advance t1 (Recv (aem2, 0)) in
  pp_res t1 actions ;
  [%expect
    {|
    t: {log: []; commit_index:-1; current_term: 12; node_state:Follower{timeout:0; voted_for:0}}
    actions:
    [Send(0, AppendEntriesResponse {term: 12; success: Error: -1})] |}] ;
  let aer = AppendEntriesResponse {term= 12; success= Error (-1)} in
  let t, actions = Impl.advance t (Recv (aer, 1)) in
  pp_res t actions ;
  [%expect
    {|
    t: {log: [{command: Command(Read m1, 1); term : 12},{command: Command(Read m2, 2); term : 12}]; commit_index:-1; current_term: 12; node_state:Leader{heartbeat:0; rep_ackd:
    [{1, -1}, {2, -1}]; rep_sent:[{1, 1}, {2, 1}]}
    actions: [Send(1, AppendEntries {term: 12; leader_commit: -1; prev_log_index: -1; prev_log_term: 0; entries_length: 2; entries:
                                                             [{command: Command(Read m1, 1); term : 12},{command: Command(Read m2, 2); term : 12}]})] |}]

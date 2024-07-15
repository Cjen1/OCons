open! Impl_core__Types
open! Impl_core__Utils
open! Impl_core__Actions_f
module Imp = ImperativeActions (Impl_core__Conspire_single_shot.Types)
module Impl = Impl_core__Conspire_single_shot.Make (Imp)
open! Impl_core__Conspire_single_shot.Types
open! Impl
open Ocons_core.Consensus_intf

let action_pp = action_pp ~pp_msg:PP.message_pp

let c1 = make_config ~node_id:0 ~replica_ids:[0] ~fd_timeout:2 ()

let c4 node_id = make_config ~node_id ~replica_ids:[0; 1; 2; 3] ~fd_timeout:2 ()

let print t acts =
  Fmt.pr "t: @[<v>%a@]@." PP.t_pp t ;
  Fmt.pr "actions: @[%a@]@." Fmt.(brackets @@ list action_pp) acts

let%expect_test "local_commit" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t = create c1 in
  let c1 = make_command [|Read "c1"|] in
  let t, actions = Impl.advance t (Commands (c1 |> Iter.singleton)) in
  print t actions ;
  [%expect
    {|
    t: config: node_id: 0
               quorum_side: 1
               fd_timeout: 2
               replica_ids: [0]
       commit_index: 0
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command((Read c1), 1)]}]
       prop_log:
        [state: COMMITTED
         term: 0
         value: [Command((Read c1), 1)]
         votes: [(0: term: 0
                     vterm: 0
                     vvalue: [Command((Read c1), 1)])]
         max_term: correct
         max_term_count: correct
         match_vote_count: correct]
       fd: state: [(0: 2)]
    actions: [CommitCommands(Command((Read c1), 1))
              Broadcast(Sync(idx: 0
                             term: 0
                             value: [Command((Read c1), 1)]))] |}] ;
  let c2, c3 = (make_command [|Read "c2"|], make_command [|Read "c3"|]) in
  let t, actions = Impl.advance t (Commands (Iter.of_list [c2; c3])) in
  print t actions ;
  [%expect
    {|
    t: config: node_id: 0
               quorum_side: 1
               fd_timeout: 2
               replica_ids: [0]
       commit_index: 2
       rep_log:
        [{term: 0
          vterm: 0
          vvalue: [Command((Read c1), 1)]}
         {term: 0
          vterm: 0
          vvalue: [Command((Read c2), 3)]}
         {term: 0
          vterm: 0
          vvalue: [Command((Read c3), 2)]}]
       prop_log:
        [state: COMMITTED
         term: 0
         value: [Command((Read c1), 1)]
         votes: [(0: term: 0
                     vterm: 0
                     vvalue: [Command((Read c1), 1)])]
         max_term: correct
         max_term_count: correct
         match_vote_count: correct
         state: COMMITTED
         term: 0
         value: [Command((Read c2), 3)]
         votes: [(0: term: 0
                     vterm: 0
                     vvalue: [Command((Read c2), 3)])]
         max_term: correct
         max_term_count: correct
         match_vote_count: correct
         state: COMMITTED
         term: 0
         value: [Command((Read c3), 2)]
         votes: [(0: term: 0
                     vterm: 0
                     vvalue: [Command((Read c3), 2)])]
         max_term: correct
         max_term_count: correct
         match_vote_count: correct]
       fd: state: [(0: 2)]
    actions: [CommitCommands(Command((Read c3), 2))
              CommitCommands(Command((Read c2), 3))
              Broadcast(Sync(idx: 2
                             term: 0
                             value: [Command((Read c3), 2)]))
              Broadcast(Sync(idx: 1
                             term: 0
                             value: [Command((Read c2), 3)]))] |}]

let%expect_test "e2e commit" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t1 = create (c4 1) in
  let t2 = create (c4 2) in
  let t3 = create (c4 3) in
  let c1 = make_command [|Read "c1"|] in
  let t1, actions = Impl.advance t1 (Commands (Iter.of_list [c1])) in
  print t1 actions ;
  [%expect
    {|
    t: config: node_id: 1
               quorum_side: 3
               fd_timeout: 2
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command((Read c1), 1)]}]
       prop_log:
        [state: UNDECIDED
         term: 0
         value: [Command((Read c1), 1)]
         votes:
          [(0: term: -1
               vterm: -1
               vvalue: []);
           (1: term: 0
               vterm: 0
               vvalue: [Command((Read c1), 1)]);
           (2: term: -1
               vterm: -1
               vvalue: []);
           (3: term: -1
               vterm: -1
               vvalue: [])]
         max_term: correct
         max_term_count: correct
         match_vote_count: correct]
       fd: state: [(0: 2); (1: 2); (2: 2); (3: 2)]
    actions: [Broadcast(Sync(idx: 0
                             term: 0
                             value: [Command((Read c1), 1)]))] |}] ;
  let recv_c1 = Recv (Sync (0, {term= 0; value= [c1]}), 1) in
  let t2, actions = Impl.advance t2 recv_c1 in
  print t2 actions ;
  [%expect
    {|
    t: config: node_id: 2
               quorum_side: 3
               fd_timeout: 2
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command((Read c1), 1)]}]
       prop_log: []
       fd: state: [(0: 2); (1: 2); (2: 2); (3: 2)]
    actions: [Send(1,SyncResp(0,term: 0
                                vterm: 0
                                vvalue: [Command((Read c1), 1)]))] |}] ;
  let t3, actions = Impl.advance t3 recv_c1 in
  print t3 actions ;
  [%expect
    {|
    t: config: node_id: 3
               quorum_side: 3
               fd_timeout: 2
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command((Read c1), 1)]}]
       prop_log: []
       fd: state: [(0: 2); (1: 2); (2: 2); (3: 2)]
    actions: [Send(1,SyncResp(0,term: 0
                                vterm: 0
                                vvalue: [Command((Read c1), 1)]))] |}] ;
  let recv i = Recv (SyncResp (0, {term= 0; vterm= 0; vvalue= [c1]}), i) in
  let t1, actions = Impl.advance t1 (recv 2) in
  print t1 actions ;
  [%expect
    {|
    t: config: node_id: 1
               quorum_side: 3
               fd_timeout: 2
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command((Read c1), 1)]}]
       prop_log:
        [state: UNDECIDED
         term: 0
         value: [Command((Read c1), 1)]
         votes:
          [(0: term: -1
               vterm: -1
               vvalue: []);
           (1: term: 0
               vterm: 0
               vvalue: [Command((Read c1), 1)]);
           (2: term: 0
               vterm: 0
               vvalue: [Command((Read c1), 1)]);
           (3: term: -1
               vterm: -1
               vvalue: [])]
         max_term: correct
         max_term_count: correct
         match_vote_count: correct]
       fd: state: [(0: 2); (1: 2); (2: 2); (3: 2)]
    actions: [] |}] ;
  let t1, actions = Impl.advance t1 (recv 3) in
  print t1 actions ;
  [%expect
    {|
    t: config: node_id: 1
               quorum_side: 3
               fd_timeout: 2
               replica_ids: [0, 1, 2, 3]
       commit_index: 0
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command((Read c1), 1)]}]
       prop_log:
        [state: COMMITTED
         term: 0
         value: [Command((Read c1), 1)]
         votes:
          [(0: term: -1
               vterm: -1
               vvalue: []);
           (1: term: 0
               vterm: 0
               vvalue: [Command((Read c1), 1)]);
           (2: term: 0
               vterm: 0
               vvalue: [Command((Read c1), 1)]);
           (3: term: 0
               vterm: 0
               vvalue: [Command((Read c1), 1)])]
         max_term: correct
         max_term_count: correct
         match_vote_count: correct]
       fd: state: [(0: 2); (1: 2); (2: 2); (3: 2)]
    actions: [CommitCommands(Command((Read c1), 1))] |}]

let%expect_test "e2e conflict re-propose" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t1 = create (c4 1) in
  let t2 = create (c4 2) in
  let t3 = create (c4 3) in
  let c1 = make_command [|Read "c1"|] in
  let t1, actions = Impl.advance t1 (Commands (Iter.of_list [c1])) in
  print t1 actions ;
  [%expect
    {|
    t: config: node_id: 1
               quorum_side: 3
               fd_timeout: 2
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command((Read c1), 1)]}]
       prop_log:
        [state: UNDECIDED
         term: 0
         value: [Command((Read c1), 1)]
         votes:
          [(0: term: -1
               vterm: -1
               vvalue: []);
           (1: term: 0
               vterm: 0
               vvalue: [Command((Read c1), 1)]);
           (2: term: -1
               vterm: -1
               vvalue: []);
           (3: term: -1
               vterm: -1
               vvalue: [])]
         max_term: correct
         max_term_count: correct
         match_vote_count: correct]
       fd: state: [(0: 2); (1: 2); (2: 2); (3: 2)]
    actions: [Broadcast(Sync(idx: 0
                             term: 0
                             value: [Command((Read c1), 1)]))] |}] ;
  let t1, _ = Impl.advance t1 Tick in
  let t1, actions = Impl.advance t1 Tick in
  print t1 actions ;
  [%expect
    {|
    t: config: node_id: 1
               quorum_side: 3
               fd_timeout: 2
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command((Read c1), 1)]}]
       prop_log:
        [state: UNDECIDED
         term: 0
         value: [Command((Read c1), 1)]
         votes:
          [(0: term: -1
               vterm: -1
               vvalue: []);
           (1: term: 0
               vterm: 0
               vvalue: [Command((Read c1), 1)]);
           (2: term: -1
               vterm: -1
               vvalue: []);
           (3: term: -1
               vterm: -1
               vvalue: [])]
         max_term: correct
         max_term_count: correct
         match_vote_count: correct]
       fd: state: [(0: 0); (1: 2); (2: 0); (3: 0)]
    actions: [Broadcast(Heartbeat)] |}] ;
  let c2 = make_command [|Read "c2"|] in
  let t2, actions = Impl.advance t2 (Commands (Iter.of_list [c2])) in
  print t2 actions ;
  [%expect
    {|
    t: config: node_id: 2
               quorum_side: 3
               fd_timeout: 2
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command((Read c2), 2)]}]
       prop_log:
        [state: UNDECIDED
         term: 0
         value: [Command((Read c2), 2)]
         votes:
          [(0: term: -1
               vterm: -1
               vvalue: []);
           (1: term: -1
               vterm: -1
               vvalue: []);
           (2: term: 0
               vterm: 0
               vvalue: [Command((Read c2), 2)]);
           (3: term: -1
               vterm: -1
               vvalue: [])]
         max_term: correct
         max_term_count: correct
         match_vote_count: correct]
       fd: state: [(0: 2); (1: 2); (2: 2); (3: 2)]
    actions: [Broadcast(Sync(idx: 0
                             term: 0
                             value: [Command((Read c2), 2)]))] |}] ;
  let t3, actions =
    Impl.advance t3 (Recv (Sync (0, {term= 0; value= [c1]}), 1))
  in
  print t3 actions ;
  [%expect
    {|
    t: config: node_id: 3
               quorum_side: 3
               fd_timeout: 2
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command((Read c1), 1)]}]
       prop_log: []
       fd: state: [(0: 2); (1: 2); (2: 2); (3: 2)]
    actions: [Send(1,SyncResp(0,term: 0
                                vterm: 0
                                vvalue: [Command((Read c1), 1)]))] |}] ;
  (* Conflict from t1 *)
  let t2, actions =
    Impl.advance t2 (Recv (Sync (0, {term= 0; value= [c1]}), 1))
  in
  print t2 actions ;
  [%expect
    {|
    t: config: node_id: 2
               quorum_side: 3
               fd_timeout: 2
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 1
                  vterm: 0
                  vvalue: [Command((Read c2), 2)]}]
       prop_log:
        [state: UNDECIDED
         term: 0
         value: [Command((Read c2), 2)]
         votes:
          [(0: term: -1
               vterm: -1
               vvalue: []);
           (1: term: -1
               vterm: -1
               vvalue: []);
           (2: term: 1
               vterm: 0
               vvalue: [Command((Read c2), 2)]);
           (3: term: -1
               vterm: -1
               vvalue: [])]
         max_term: correct
         max_term_count: correct
         match_vote_count: correct]
       fd: state: [(0: 2); (1: 2); (2: 2); (3: 2)]
    actions: [Broadcast(SyncResp(0,term: 1
                                   vterm: 0
                                   vvalue: [Command((Read c2), 2)]))] |}] ;
  (* Conflict from t2 *)
  let t1, actions =
    Impl.advance t1 (Recv (Sync (0, {term= 0; value= [c2]}), 2))
  in
  print t1 actions ;
  [%expect
    {|
    t: config: node_id: 1
               quorum_side: 3
               fd_timeout: 2
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 1
                  vterm: 0
                  vvalue: [Command((Read c1), 1)]}]
       prop_log:
        [state: UNDECIDED
         term: 0
         value: [Command((Read c1), 1)]
         votes:
          [(0: term: -1
               vterm: -1
               vvalue: []);
           (1: term: 1
               vterm: 0
               vvalue: [Command((Read c1), 1)]);
           (2: term: -1
               vterm: -1
               vvalue: []);
           (3: term: -1
               vterm: -1
               vvalue: [])]
         max_term: correct
         max_term_count: correct
         match_vote_count: correct]
       fd: state: [(0: 0); (1: 2); (2: 2); (3: 0)]
    actions: [Broadcast(SyncResp(0,term: 1
                                   vterm: 0
                                   vvalue: [Command((Read c1), 1)]))] |}] ;
  let t3, actions =
    Impl.advance t3 (Recv (Sync (0, {term= 0; value= [c2]}), 2))
  in
  print t3 actions ;
  [%expect
    {|
    t: config: node_id: 3
               quorum_side: 3
               fd_timeout: 2
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 1
                  vterm: 0
                  vvalue: [Command((Read c1), 1)]}]
       prop_log:
        [state: UNDECIDED
         term: 0
         value: []
         votes:
          [(0: term: -1
               vterm: -1
               vvalue: []);
           (1: term: -1
               vterm: -1
               vvalue: []);
           (2: term: -1
               vterm: -1
               vvalue: []);
           (3: term: 1
               vterm: 0
               vvalue: [Command((Read c1), 1)])]
         max_term: correct
         max_term_count: correct
         match_vote_count: correct]
       fd: state: [(0: 2); (1: 2); (2: 2); (3: 2)]
    actions: [Broadcast(SyncResp(0,term: 1
                                   vterm: 0
                                   vvalue: [Command((Read c1), 1)]))] |}] ;
  (* Conflict result *)
  let t1, actions =
    Impl.advance t1 (Recv (SyncResp (0, {term= 1; vterm= 0; vvalue= [c1]}), 3))
  in
  print t1 actions ;
  [%expect
    {|
    t: config: node_id: 1
               quorum_side: 3
               fd_timeout: 2
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 1
                  vterm: 0
                  vvalue: [Command((Read c1), 1)]}]
       prop_log:
        [state: UNDECIDED
         term: 0
         value: [Command((Read c1), 1)]
         votes:
          [(0: term: -1
               vterm: -1
               vvalue: []);
           (1: term: 1
               vterm: 0
               vvalue: [Command((Read c1), 1)]);
           (2: term: -1
               vterm: -1
               vvalue: []);
           (3: term: 1
               vterm: 0
               vvalue: [Command((Read c1), 1)])]
         max_term: correct
         max_term_count: correct
         match_vote_count: correct]
       fd: state: [(0: 0); (1: 2); (2: 2); (3: 2)]
    actions: [] |}] ;
  let t1, actions =
    Impl.advance t1 (Recv (SyncResp (0, {term= 1; vterm= 0; vvalue= [c2]}), 2))
  in
  print t1 actions ;
  [%expect
    {|
    t: config: node_id: 1
               quorum_side: 3
               fd_timeout: 2
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 1
                  vterm: 1
                  vvalue: [Command((Read c1), 1)]}]
       prop_log:
        [state: UNDECIDED
         term: 1
         value: [Command((Read c1), 1)]
         votes:
          [(0: term: -1
               vterm: -1
               vvalue: []);
           (1: term: 1
               vterm: 1
               vvalue: [Command((Read c1), 1)]);
           (2: term: 1
               vterm: 0
               vvalue: [Command((Read c2), 2)]);
           (3: term: 1
               vterm: 0
               vvalue: [Command((Read c1), 1)])]
         max_term: correct
         max_term_count: correct
         match_vote_count: correct]
       fd: state: [(0: 0); (1: 2); (2: 2); (3: 2)]
    actions: [Broadcast(Sync(idx: 0
                             term: 1
                             value: [Command((Read c1), 1)]))] |}] ;
  ignore (t2, t3)

let%expect_test "e2e conflict merge" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t1 = create (c4 1) in
  let c1 = make_command [|Read "c1"|] in
  let c2 = make_command [|Read "c2"|] in
  let t1, actions = Impl.advance t1 (Commands (Iter.of_list [c1])) in
  print t1 actions ;
  [%expect
    {|
    t: config: node_id: 1
               quorum_side: 3
               fd_timeout: 2
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command((Read c1), 1)]}]
       prop_log:
        [state: UNDECIDED
         term: 0
         value: [Command((Read c1), 1)]
         votes:
          [(0: term: -1
               vterm: -1
               vvalue: []);
           (1: term: 0
               vterm: 0
               vvalue: [Command((Read c1), 1)]);
           (2: term: -1
               vterm: -1
               vvalue: []);
           (3: term: -1
               vterm: -1
               vvalue: [])]
         max_term: correct
         max_term_count: correct
         match_vote_count: correct]
       fd: state: [(0: 2); (1: 2); (2: 2); (3: 2)]
    actions: [Broadcast(Sync(idx: 0
                             term: 0
                             value: [Command((Read c1), 1)]))] |}] ;
  let t1, _ =
    Impl.advance t1 (Recv (SyncResp (0, {term= 1; vterm= 0; vvalue= [c1]}), 0))
  in
  let t1, _ =
    Impl.advance t1 (Recv (SyncResp (0, {term= 1; vterm= 0; vvalue= [c1]}), 1))
  in
  let t1, actions =
    Impl.advance t1 (Recv (SyncResp (0, {term= 1; vterm= 0; vvalue= [c2]}), 2))
  in
  print t1 actions ;
  [%expect
    {|
    t: config: node_id: 1
               quorum_side: 3
               fd_timeout: 2
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command((Read c1), 1)]}]
       prop_log:
        [state: UNDECIDED
         term: 0
         value: [Command((Read c1), 1)]
         votes:
          [(0: term: 1
               vterm: 0
               vvalue: [Command((Read c1), 1)]);
           (1: term: 1
               vterm: 0
               vvalue: [Command((Read c1), 1)]);
           (2: term: 1
               vterm: 0
               vvalue: [Command((Read c2), 2)]);
           (3: term: -1
               vterm: -1
               vvalue: [])]
         max_term: correct
         max_term_count: correct
         match_vote_count: correct]
       fd: state: [(0: 2); (1: 2); (2: 2); (3: 2)]
    actions: [] |}] ;
  let t1, actions =
    Impl.advance t1 (Recv (SyncResp (0, {term= 1; vterm= 0; vvalue= [c2]}), 3))
  in
  print t1 actions ;
  [%expect
    {|
    t: config: node_id: 1
               quorum_side: 3
               fd_timeout: 2
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log:
        [{term: 1
          vterm: 1
          vvalue: [Command((Read c1), 1), Command((Read c2), 2)]}]
       prop_log:
        [state: UNDECIDED
         term: 1
         value: [Command((Read c1), 1), Command((Read c2), 2)]
         votes:
          [(0: term: 1
               vterm: 0
               vvalue: [Command((Read c1), 1)]);
           (1:
            term: 1
            vterm: 1
            vvalue: [Command((Read c1), 1), Command((Read c2), 2)]);
           (2: term: 1
               vterm: 0
               vvalue: [Command((Read c2), 2)]);
           (3: term: 1
               vterm: 0
               vvalue: [Command((Read c2), 2)])]
         max_term: correct
         max_term_count: correct
         match_vote_count: correct]
       fd: state: [(0: 2); (1: 2); (2: 2); (3: 2)]
    actions: [Broadcast(Sync(idx: 0
                             term: 1
                             value:
                              [Command((Read c1), 1), Command((Read c2), 2)]))] |}]

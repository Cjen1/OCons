open! Types
open! Utils
open! Actions_f
module Imp = ImperativeActions (Conspire_single_shot.Types)
module Impl = Conspire_single_shot.Make (Imp)
open! Conspire_single_shot.Types
open! Impl
open Ocons_core.Consensus_intf

let action_pp = action_pp ~pp_msg:PP.message_pp

let c1 = make_config ~node_id:0 ~replica_ids:[0]

let c4 node_id = make_config ~node_id ~replica_ids:[0; 1; 2; 3]

let print t acts =
  Fmt.pr "t: @[<v>%a@]@," PP.t_pp t ;
  Fmt.pr "actions:@[%a@]" Fmt.(brackets @@ Iter.pp_seq action_pp) acts

let%expect_test "local_commit" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t = create c1 in
  let c1 = make_command (Read "c1") in
  let t, actions = Impl.advance t (Commands (c1 |> Iter.singleton)) in
  print t actions ;
  [%expect
    {|
    t: config: node_id: 0
               quorum_side: 1
               replica_ids: [0]
       commit_index: 0
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command(Read c1, 1)]}]
       prop_log: [Committed(value: [Command(Read c1, 1)])]
    actions:[Broadcast(Sync(idx: 0
                            term: 0
                            value: [Command(Read c1, 1)])),
             CommitCommands(Command(Read c1, 1))] |}] ;
  let c2, c3 = (make_command (Read "c2"), make_command (Read "c3")) in
  let t, actions = Impl.advance t (Commands (Iter.of_list [c2; c3])) in
  print t actions ;
  [%expect
    {|
    t: config: node_id: 0
               quorum_side: 1
               replica_ids: [0]
       commit_index: 2
       rep_log:
        [{term: 0
          vterm: 0
          vvalue: [Command(Read c1, 1)]}
         {term: 0
          vterm: 0
          vvalue: [Command(Read c2, 3)]}
         {term: 0
          vterm: 0
          vvalue: [Command(Read c3, 2)]}]
       prop_log:
        [Committed(value: [Command(Read c1, 1)])
         Committed(value: [Command(Read c2, 3)])
         Committed(value: [Command(Read c3, 2)])]
    actions:[Broadcast(Sync(idx: 1
                            term: 0
                            value: [Command(Read c2, 3)])),
             Broadcast(Sync(idx: 2
                            term: 0
                            value: [Command(Read c3, 2)])),
             CommitCommands(Command(Read c2, 3)),
             CommitCommands(Command(Read c3, 2))] |}]

let%expect_test "e2e commit" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t1 = create (c4 1) in
  let t2 = create (c4 2) in
  let t3 = create (c4 3) in
  let c1 = make_command (Read "c1") in
  let t1, actions = Impl.advance t1 (Commands (Iter.of_list [c1])) in
  print t1 actions ;
  [%expect
    {|
    t: config: node_id: 1
               quorum_side: 3
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command(Read c1, 1)]}]
       prop_log:
        [Undecided(term: 0
                   sent: true
                   value: [Command(Read c1, 1)]
                   votes:
                    [(1: idx: 0
                         term: 0
                         vterm: 0
                         vvalue: [Command(Read c1, 1)])])]
    actions:[Broadcast(Sync(idx: 0
                            term: 0
                            value: [Command(Read c1, 1)]))] |}] ;
  let recv_c1 = Recv (Sync {idx= 0; term= 0; value= [c1]}, 1) in
  let t2, actions = Impl.advance t2 recv_c1 in
  print t2 actions ;
  [%expect
    {|
    t: config: node_id: 2
               quorum_side: 3
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command(Read c1, 1)]}]
       prop_log: []
    actions:[Send(1,SyncResp(idx: 0
                             term: 0
                             vterm: 0
                             vvalue: [Command(Read c1, 1)]))] |}] ;
  let t3, actions = Impl.advance t3 recv_c1 in
  print t3 actions ;
  [%expect
    {|
    t: config: node_id: 3
               quorum_side: 3
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command(Read c1, 1)]}]
       prop_log: []
    actions:[Send(1,SyncResp(idx: 0
                             term: 0
                             vterm: 0
                             vvalue: [Command(Read c1, 1)]))] |}] ;
  let recv i = Recv (SyncResp {idx= 0; term= 0; vterm= 0; vvalue= [c1]}, i) in
  let t1, actions = Impl.advance t1 (recv 2) in
  print t1 actions ;
  [%expect
    {|
    t: config: node_id: 1
               quorum_side: 3
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command(Read c1, 1)]}]
       prop_log:
        [Undecided(term: 0
                   sent: true
                   value: [Command(Read c1, 1)]
                   votes:
                    [(1: idx: 0
                         term: 0
                         vterm: 0
                         vvalue: [Command(Read c1, 1)]);
                     (2: idx: 0
                         term: 0
                         vterm: 0
                         vvalue: [Command(Read c1, 1)])])]
    actions:[] |}] ;
  let t1, actions = Impl.advance t1 (recv 3) in
  print t1 actions ;
  [%expect
    {|
    t: config: node_id: 1
               quorum_side: 3
               replica_ids: [0, 1, 2, 3]
       commit_index: 0
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command(Read c1, 1)]}]
       prop_log: [Committed(value: [Command(Read c1, 1)])]
    actions:[CommitCommands(Command(Read c1, 1))] |}]

let%expect_test "e2e conflict" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t1 = create (c4 1) in
  let t2 = create (c4 2) in
  let t3 = create (c4 3) in
  let c1 = make_command (Read "c1") in
  let t1, actions = Impl.advance t1 (Commands (Iter.of_list [c1])) in
  print t1 actions ;
  [%expect
    {|
    t: config: node_id: 1
               quorum_side: 3
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command(Read c1, 1)]}]
       prop_log:
        [Undecided(term: 0
                   sent: true
                   value: [Command(Read c1, 1)]
                   votes:
                    [(1: idx: 0
                         term: 0
                         vterm: 0
                         vvalue: [Command(Read c1, 1)])])]
    actions:[Broadcast(Sync(idx: 0
                            term: 0
                            value: [Command(Read c1, 1)]))] |}] ;
  let c2 = make_command (Read "c2") in
  let t2, actions = Impl.advance t2 (Commands (Iter.of_list [c2])) in
  print t2 actions ;
  [%expect
    {|
    t: config: node_id: 2
               quorum_side: 3
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command(Read c2, 2)]}]
       prop_log:
        [Undecided(term: 0
                   sent: true
                   value: [Command(Read c2, 2)]
                   votes:
                    [(2: idx: 0
                         term: 0
                         vterm: 0
                         vvalue: [Command(Read c2, 2)])])]
    actions:[Broadcast(Sync(idx: 0
                            term: 0
                            value: [Command(Read c2, 2)]))] |}] ;
  let t3, actions =
    Impl.advance t3 (Recv (Sync {idx= 0; term= 0; value= [c1]}, 1))
  in
  print t3 actions ;
  [%expect
    {|
    t: config: node_id: 3
               quorum_side: 3
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 0
                  vterm: 0
                  vvalue: [Command(Read c1, 1)]}]
       prop_log: []
    actions:[Send(1,SyncResp(idx: 0
                             term: 0
                             vterm: 0
                             vvalue: [Command(Read c1, 1)]))] |}] ;
  (* Conflict from t1 *)
  let t2, actions =
    Impl.advance t2 (Recv (Sync {idx= 0; term= 0; value= [c1]}, 1))
  in
  print t2 actions ;
  [%expect
    {|
    t: config: node_id: 2
               quorum_side: 3
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 1
                  vterm: 0
                  vvalue: [Command(Read c2, 2)]}]
       prop_log:
        [Undecided(term: 0
                   sent: true
                   value: [Command(Read c2, 2)]
                   votes:
                    [(2: idx: 0
                         term: 1
                         vterm: 0
                         vvalue: [Command(Read c2, 2)])])]
    actions:[Broadcast(SyncResp(idx: 0
                                term: 1
                                vterm: 0
                                vvalue: [Command(Read c2, 2)]))] |}] ;
  (* Conflict from t2 *)
  let t1, actions =
    Impl.advance t1 (Recv (Sync {idx= 0; term= 0; value= [c2]}, 2))
  in
  print t1 actions ;
  [%expect
    {|
    t: config: node_id: 1
               quorum_side: 3
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 1
                  vterm: 0
                  vvalue: [Command(Read c1, 1)]}]
       prop_log:
        [Undecided(term: 0
                   sent: true
                   value: [Command(Read c1, 1)]
                   votes:
                    [(1: idx: 0
                         term: 1
                         vterm: 0
                         vvalue: [Command(Read c1, 1)])])]
    actions:[Broadcast(SyncResp(idx: 0
                                term: 1
                                vterm: 0
                                vvalue: [Command(Read c1, 1)]))] |}] ;
  let t3, actions =
    Impl.advance t3 (Recv (Sync {idx= 0; term= 0; value= [c2]}, 2))
  in
  print t3 actions ;
  [%expect
    {|
    t: config: node_id: 3
               quorum_side: 3
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 1
                  vterm: 0
                  vvalue: [Command(Read c1, 1)]}]
       prop_log:
        [Undecided(term: 0
                   sent: true
                   value: []
                   votes:
                    [(3: idx: 0
                         term: 1
                         vterm: 0
                         vvalue: [Command(Read c1, 1)])])]
    actions:[Broadcast(SyncResp(idx: 0
                                term: 1
                                vterm: 0
                                vvalue: [Command(Read c1, 1)])),
             Broadcast(Sync(idx: 0
                            term: 0
                            value: []))] |}] ;
  (* Conflict result *)
  let t1, actions =
    Impl.advance t1
      (Recv (SyncResp {idx= 0; term= 1; vterm= 0; vvalue= [c1]}, 3))
  in
  print t1 actions ;
  [%expect
    {|
    t: config: node_id: 1
               quorum_side: 3
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 1
                  vterm: 0
                  vvalue: [Command(Read c1, 1)]}]
       prop_log:
        [Undecided(term: 0
                   sent: true
                   value: [Command(Read c1, 1)]
                   votes:
                    [(1: idx: 0
                         term: 1
                         vterm: 0
                         vvalue: [Command(Read c1, 1)]);
                     (3: idx: 0
                         term: 1
                         vterm: 0
                         vvalue: [Command(Read c1, 1)])])]
    actions:[] |}] ;
  let t1, actions =
    Impl.advance t1
      (Recv (SyncResp {idx= 0; term= 1; vterm= 0; vvalue= [c2]}, 2))
  in
  print t1 actions ;
  [%expect
    {|
    t: config: node_id: 1
               quorum_side: 3
               replica_ids: [0, 1, 2, 3]
       commit_index: -1
       rep_log: [{term: 1
                  vterm: 1
                  vvalue: [Command(Read c1, 1)]}]
       prop_log:
        [Undecided(term: 1
                   sent: true
                   value: [Command(Read c1, 1)]
                   votes:
                    [(1: idx: 0
                         term: 1
                         vterm: 1
                         vvalue: [Command(Read c1, 1)])])]
    actions:[Broadcast(Sync(idx: 0
                            term: 1
                            value: [Command(Read c1, 1)]))] |}] ;
  ()

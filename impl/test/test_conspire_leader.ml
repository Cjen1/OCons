open! Core
module MP = Impl_core__Conspire_leader
module Imp = Impl_core__Actions_f.ImperativeActions (MP.Types)
module Impl = MP.Make (Imp)
module Rep = MP.Conspire.Rep
open Impl
open Impl_core__Types
open MP.Types

let action_pp = Ocons_core.Consensus_intf.action_pp ~pp_msg:pp_message

let c1 = make_config ~node_id:0 ~replica_ids:[0] ~fd_timeout:2 ()

let c4 node_id = make_config ~node_id ~replica_ids:[0; 1; 2; 3] ~fd_timeout:2 ()

let print t acts =
  Fmt.pr "t: @[<v>%a@]@." PP.t_pp t ;
  Fmt.pr "actions: @[%a@]@." Fmt.(brackets @@ list action_pp) acts

let%expect_test "local_commit" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t = create c1 in
  print t [] ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree = [(d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state = []; config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_leader.FailureDetector.state = []; timeout = 2 } }
    actions: [] |}] ;
  let c1 = make_command [|Read "c1"|] in
  let t, actions = Impl.advance t (Commands (c1 |> Iter.singleton)) in
  print t actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 7f163aa6885a86940b22582a376a48f8; vterm = 0; term = 0;
               commit_index = 7f163aa6885a86940b22582a376a48f8 };
             store =
             { ctree =
               [(7f163aa6885a86940b22582a376a48f8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c1), 1)]);
                   parent = <opaque>; key = 7f163aa6885a86940b22582a376a48f8 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state = []; config = <opaque>;
           commit_log = [[Command((Read c1), 1)]] };
         failure_detector =
         { Conspire_leader.FailureDetector.state = []; timeout = 2 } }
    actions: [CommitCommands(Command((Read c1), 1))] |}] ;
  let c2, c3 = (make_command [|Read "c2"|], make_command [|Read "c3"|]) in
  let t, actions = Impl.advance t (Commands (Iter.of_list [c2; c3])) in
  print t actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 05a727ebfe41dd9a9c34a061bb746527; vterm = 0; term = 0;
               commit_index = 05a727ebfe41dd9a9c34a061bb746527 };
             store =
             { ctree =
               [(05a727ebfe41dd9a9c34a061bb746527:
                 { node =
                   (2, 7f163aa6885a86940b22582a376a48f8,
                    [Command((Read c2), 3); Command((Read c3), 2)]);
                   parent = <opaque>; key = 05a727ebfe41dd9a9c34a061bb746527 });
                (7f163aa6885a86940b22582a376a48f8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c1), 1)]);
                   parent = <opaque>; key = 7f163aa6885a86940b22582a376a48f8 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state = []; config = <opaque>;
           commit_log =
           [[Command((Read c1), 1)]
            [Command((Read c2), 3); Command((Read c3), 2)]]
           };
         failure_detector =
         { Conspire_leader.FailureDetector.state = []; timeout = 2 } }
    actions: [CommitCommands(Command((Read c2), 3), Command((Read c3), 2))] |}]

let%expect_test "e2e commit" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t0 = create (c4 0) in
  let root_hd = t0.conspire.rep.store.root in
  let t1 = create (c4 1) in
  let c1 = make_command [|Read "c1"|] in
  let t0, actions = Impl.advance t0 (Commands (Iter.of_list [c1])) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 7f163aa6885a86940b22582a376a48f8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(7f163aa6885a86940b22582a376a48f8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c1), 1)]);
                   parent = <opaque>; key = 7f163aa6885a86940b22582a376a48f8 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_leader.FailureDetector.state = [(1: 2)(2: 2)(3: 2)];
           timeout = 2 }
         }
    actions: [Send(1,{ ctree =
                       (Some { new_head = 7f163aa6885a86940b22582a376a48f8;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command((Read c1), 1)])]
                               });
                       cons =
                       (Some { vval = 7f163aa6885a86940b22582a376a48f8;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(2,{ ctree =
                       (Some { new_head = 7f163aa6885a86940b22582a376a48f8;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command((Read c1), 1)])]
                               });
                       cons =
                       (Some { vval = 7f163aa6885a86940b22582a376a48f8;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(3,{ ctree =
                       (Some { new_head = 7f163aa6885a86940b22582a376a48f8;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command((Read c1), 1)])]
                               });
                       cons =
                       (Some { vval = 7f163aa6885a86940b22582a376a48f8;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })] |}] ;
  let hd1 = MP.Conspire.CTree.make_key root_hd [c1] in
  let update =
    MP.Conspire.Rep.
      {ctree= Some {new_head= hd1; extension= [(1, root_hd, [c1])]}; cons= None}
  in
  let t1, actions = Impl.advance t1 (Recv (Ok update, 0)) in
  print t1 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(7f163aa6885a86940b22582a376a48f8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c1), 1)]);
                   parent = <opaque>; key = 7f163aa6885a86940b22582a376a48f8 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(0:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_leader.FailureDetector.state = [(0: 2)(2: 2)(3: 2)];
           timeout = 2 }
         }
    actions: [] |}] ;
  let t0_vote =
    Rep.
      { ctree= None
      ; cons= Some {term= 0; vterm= 0; vval= hd1; commit_index= root_hd} }
  in
  let t1, actions = Impl.advance t1 (Recv (Ok t0_vote, 0)) in
  print t1 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 7f163aa6885a86940b22582a376a48f8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(7f163aa6885a86940b22582a376a48f8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c1), 1)]);
                   parent = <opaque>; key = 7f163aa6885a86940b22582a376a48f8 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(0:
             { vval = 7f163aa6885a86940b22582a376a48f8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_leader.FailureDetector.state = [(0: 2)(2: 2)(3: 2)];
           timeout = 2 }
         }
    actions: [Send(0,{ ctree = None;
                       cons =
                       (Some { vval = 7f163aa6885a86940b22582a376a48f8;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })] |}] ;
  let t0, actions = Impl.advance t0 (Recv (Ok t0_vote, 1)) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 7f163aa6885a86940b22582a376a48f8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(7f163aa6885a86940b22582a376a48f8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c1), 1)]);
                   parent = <opaque>; key = 7f163aa6885a86940b22582a376a48f8 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 7f163aa6885a86940b22582a376a48f8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_leader.FailureDetector.state = [(1: 2)(2: 2)(3: 2)];
           timeout = 2 }
         }
    actions: [] |}] ;
  let t0, actions = Impl.advance t0 (Recv (Ok t0_vote, 2)) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 7f163aa6885a86940b22582a376a48f8; vterm = 0; term = 0;
               commit_index = 7f163aa6885a86940b22582a376a48f8 };
             store =
             { ctree =
               [(7f163aa6885a86940b22582a376a48f8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c1), 1)]);
                   parent = <opaque>; key = 7f163aa6885a86940b22582a376a48f8 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 7f163aa6885a86940b22582a376a48f8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = 7f163aa6885a86940b22582a376a48f8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [[Command((Read c1), 1)]] };
         failure_detector =
         { Conspire_leader.FailureDetector.state = [(1: 2)(2: 2)(3: 2)];
           timeout = 2 }
         }
    actions: [CommitCommands(Command((Read c1), 1))
              Send(1,{ ctree = None;
                       cons =
                       (Some { vval = 7f163aa6885a86940b22582a376a48f8;
                               vterm = 0; term = 0;
                               commit_index = 7f163aa6885a86940b22582a376a48f8 })
                       })
              Send(2,{ ctree = None;
                       cons =
                       (Some { vval = 7f163aa6885a86940b22582a376a48f8;
                               vterm = 0; term = 0;
                               commit_index = 7f163aa6885a86940b22582a376a48f8 })
                       })
              Send(3,{ ctree = None;
                       cons =
                       (Some { vval = 7f163aa6885a86940b22582a376a48f8;
                               vterm = 0; term = 0;
                               commit_index = 7f163aa6885a86940b22582a376a48f8 })
                       })] |}] ;
  ignore (t0, t1)

let%expect_test "e2e conflict resolution" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t0 = create (c4 0) in
  let t1 = create (c4 1) in
  let c0 = make_command [|Read "c0"|] in
  let c1 = make_command [|Read "c1"|] in
  let t1, _ = Impl.advance t1 Tick in
  let t1, _ = Impl.advance t1 Tick in
  let t1, actions = Impl.advance t1 (Commands (Iter.of_list [c1])) in
  print t1 actions ;
  [%expect
    {|
    +Now leader for 0
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 4d2d9d467930f0019ff9f71b2713b9d8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(4d2d9d467930f0019ff9f71b2713b9d8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c1), 2)]);
                   parent = <opaque>; key = 4d2d9d467930f0019ff9f71b2713b9d8 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(0:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_leader.FailureDetector.state = [(0: 0)(2: 0)(3: 0)];
           timeout = 2 }
         }
    actions: [Send(0,{ ctree =
                       (Some { new_head = 4d2d9d467930f0019ff9f71b2713b9d8;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command((Read c1), 2)])]
                               });
                       cons =
                       (Some { vval = 4d2d9d467930f0019ff9f71b2713b9d8;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(2,{ ctree =
                       (Some { new_head = 4d2d9d467930f0019ff9f71b2713b9d8;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command((Read c1), 2)])]
                               });
                       cons =
                       (Some { vval = 4d2d9d467930f0019ff9f71b2713b9d8;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(3,{ ctree =
                       (Some { new_head = 4d2d9d467930f0019ff9f71b2713b9d8;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command((Read c1), 2)])]
                               });
                       cons =
                       (Some { vval = 4d2d9d467930f0019ff9f71b2713b9d8;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })] |}] ;
  let t0, actions = Impl.advance t0 (Commands (Iter.of_list [c0])) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = ce7fc373c8675fa471e0d03f3b4eaaa8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(ce7fc373c8675fa471e0d03f3b4eaaa8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c0), 1)]);
                   parent = <opaque>; key = ce7fc373c8675fa471e0d03f3b4eaaa8 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_leader.FailureDetector.state = [(1: 2)(2: 2)(3: 2)];
           timeout = 2 }
         }
    actions: [Send(1,{ ctree =
                       (Some { new_head = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command((Read c0), 1)])]
                               });
                       cons =
                       (Some { vval = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(2,{ ctree =
                       (Some { new_head = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command((Read c0), 1)])]
                               });
                       cons =
                       (Some { vval = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(3,{ ctree =
                       (Some { new_head = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command((Read c0), 1)])]
                               });
                       cons =
                       (Some { vval = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })] |}] ;
  let root_clock = t0.conspire.rep.store.root in
  let c0_node = (1, root_clock, [c0]) in
  let c0_clock = MP.Conspire.CTree.make_key root_clock [c0] in
  let c1_node = (1, root_clock, [c1]) in
  let c1_clock = MP.Conspire.CTree.make_key root_clock [c1] in
  (* ---- Replicate tree ---- *)
  let t0, actions =
    Impl.advance t0
      (Recv
         ( Ok
             Rep.
               { ctree= Some {new_head= c1_clock; extension= [c1_node]}
               ; cons= None }
         , 1 ) )
  in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = ce7fc373c8675fa471e0d03f3b4eaaa8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(4d2d9d467930f0019ff9f71b2713b9d8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c1), 2)]);
                   parent = <opaque>; key = 4d2d9d467930f0019ff9f71b2713b9d8 });
                (ce7fc373c8675fa471e0d03f3b4eaaa8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c0), 1)]);
                   parent = <opaque>; key = ce7fc373c8675fa471e0d03f3b4eaaa8 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_leader.FailureDetector.state = [(1: 2)(2: 2)(3: 2)];
           timeout = 2 }
         }
    actions: [] |}] ;
  (* ---- Votes for values ---- *)
  let t0, _ =
    Impl.advance t0
      (Recv
         ( Ok
             Rep.
               { ctree= None
               ; cons=
                   Some
                     { vval= c0_clock
                     ; vterm= 0
                     ; term= 0
                     ; commit_index= root_clock } }
         , 2 ) )
  in
  let t0, actions =
    Impl.advance t0
      (Recv
         ( Ok
             Rep.
               { ctree= None
               ; cons=
                   Some
                     { vval= c1_clock
                     ; vterm= 0
                     ; term= 0
                     ; commit_index= root_clock } }
         , 1 ) )
  in
  (* NOTE does not replicate c1 branch of ctree since it is not in vval *)
  print t0 actions ;
  [%expect
    {|
    +Conflict, term=1
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = ce7fc373c8675fa471e0d03f3b4eaaa8; vterm = 0; term = 1;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(4d2d9d467930f0019ff9f71b2713b9d8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c1), 2)]);
                   parent = <opaque>; key = 4d2d9d467930f0019ff9f71b2713b9d8 });
                (ce7fc373c8675fa471e0d03f3b4eaaa8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c0), 1)]);
                   parent = <opaque>; key = ce7fc373c8675fa471e0d03f3b4eaaa8 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 4d2d9d467930f0019ff9f71b2713b9d8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = ce7fc373c8675fa471e0d03f3b4eaaa8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_leader.FailureDetector.state = [(1: 2)(2: 2)(3: 2)];
           timeout = 2 }
         }
    actions: [Send(1,{ ctree = None;
                       cons =
                       (Some { vval = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               vterm = 0; term = 1;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(2,{ ctree = None;
                       cons =
                       (Some { vval = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               vterm = 0; term = 1;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(3,{ ctree = None;
                       cons =
                       (Some { vval = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               vterm = 0; term = 1;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })] |}] ;
  (* conflict term responses *)
  let t0, _ =
    Impl.advance t0
      (Recv
         ( Ok
             Rep.
               { ctree= None
               ; cons=
                   Some
                     { vval= c0_clock
                     ; vterm= 0
                     ; term= 1
                     ; commit_index= root_clock } }
         , 2 ) )
  in
  let t0, actions =
    Impl.advance t0
      (Recv
         ( Ok
             Rep.
               { ctree= None
               ; cons=
                   Some
                     { vval= c1_clock
                     ; vterm= 0
                     ; term= 1
                     ; commit_index= root_clock } }
         , 3 ) )
  in
  (* ---- Now sufficient conflicts to recover *)
  print t0 actions ;
  [%expect
    {|
    +Recovery complete term: {t:1,vt:1}
    +Recovery to 1
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = ce7fc373c8675fa471e0d03f3b4eaaa8; vterm = 1; term = 1;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(4d2d9d467930f0019ff9f71b2713b9d8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c1), 2)]);
                   parent = <opaque>; key = 4d2d9d467930f0019ff9f71b2713b9d8 });
                (ce7fc373c8675fa471e0d03f3b4eaaa8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c0), 1)]);
                   parent = <opaque>; key = ce7fc373c8675fa471e0d03f3b4eaaa8 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 4d2d9d467930f0019ff9f71b2713b9d8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = ce7fc373c8675fa471e0d03f3b4eaaa8; vterm = 0; term = 1;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = 4d2d9d467930f0019ff9f71b2713b9d8; vterm = 0; term = 1;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_leader.FailureDetector.state = [(1: 2)(2: 2)(3: 2)];
           timeout = 2 }
         }
    actions: [Send(1,{ ctree = None;
                       cons =
                       (Some { vval = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               vterm = 1; term = 1;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(2,{ ctree = None;
                       cons =
                       (Some { vval = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               vterm = 1; term = 1;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(3,{ ctree = None;
                       cons =
                       (Some { vval = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               vterm = 1; term = 1;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })] |}] ;
  (* ---- T1 will not recover since it believes T0 is the leader *)
  (* Conflict from 0 *)
  let t1, _ =
    Impl.advance t1
      (Recv
         ( Ok
             Rep.
               { ctree= Some {new_head= c0_clock; extension= [c0_node]}
               ; cons=
                   Some
                     { vval= c0_clock
                     ; vterm= 0
                     ; term= 1
                     ; commit_index= root_clock } }
         , 0 ) )
  in
  let t1, actions =
    Impl.advance t1
      (Recv
         ( Ok
             Rep.
               { ctree= None
               ; cons=
                   Some
                     { vval= c1_clock
                     ; vterm= 0
                     ; term= 1
                     ; commit_index= root_clock } }
         , 3 ) )
  in
  print t1 actions ;
  [%expect
    {|
    +No longer leader for 1
    +Conflict, term=1
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 4d2d9d467930f0019ff9f71b2713b9d8; vterm = 0; term = 1;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(4d2d9d467930f0019ff9f71b2713b9d8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c1), 2)]);
                   parent = <opaque>; key = 4d2d9d467930f0019ff9f71b2713b9d8 });
                (ce7fc373c8675fa471e0d03f3b4eaaa8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c0), 1)]);
                   parent = <opaque>; key = ce7fc373c8675fa471e0d03f3b4eaaa8 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(0:
             { vval = ce7fc373c8675fa471e0d03f3b4eaaa8; vterm = 0; term = 1;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = 4d2d9d467930f0019ff9f71b2713b9d8; vterm = 0; term = 1;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_leader.FailureDetector.state = [(0: 2)(2: 0)(3: 2)];
           timeout = 2 }
         }
    actions: [] |}] ;
  ignore (t0, t1, c0_node, c1_node)

let%expect_test "message loss" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t0 = create (c4 0) in
  let t1 = create (c4 1) in
  let c0 = make_command [|Read "c0"|] in
  let c1 = make_command [|Read "c1"|] in
  (* this message is lost *)
  let t0, actions = Impl.advance t0 (Commands (Iter.of_list [c0])) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = ce7fc373c8675fa471e0d03f3b4eaaa8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(ce7fc373c8675fa471e0d03f3b4eaaa8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c0), 1)]);
                   parent = <opaque>; key = ce7fc373c8675fa471e0d03f3b4eaaa8 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_leader.FailureDetector.state = [(1: 2)(2: 2)(3: 2)];
           timeout = 2 }
         }
    actions: [Send(1,{ ctree =
                       (Some { new_head = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command((Read c0), 1)])]
                               });
                       cons =
                       (Some { vval = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(2,{ ctree =
                       (Some { new_head = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command((Read c0), 1)])]
                               });
                       cons =
                       (Some { vval = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(3,{ ctree =
                       (Some { new_head = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command((Read c0), 1)])]
                               });
                       cons =
                       (Some { vval = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })] |}] ;
  let root_clock = t0.conspire.rep.store.root in
  let c0_clock = MP.Conspire.CTree.make_key root_clock [c0] in
  let t0, _ =
    Impl.advance t0
      (Recv
         ( Ok
             Rep.
               { ctree= None
               ; cons=
                   Some
                     { vval= c0_clock
                     ; vterm= 0
                     ; term= 0
                     ; commit_index= root_clock } }
         , 2 ) )
  in
  let t0, actions =
    Impl.advance t0
      (Recv
         ( Ok
             Rep.
               { ctree= None
               ; cons=
                   Some
                     { vval= c0_clock
                     ; vterm= 0
                     ; term= 0
                     ; commit_index= root_clock } }
         , 3 ) )
  in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = ce7fc373c8675fa471e0d03f3b4eaaa8; vterm = 0; term = 0;
               commit_index = ce7fc373c8675fa471e0d03f3b4eaaa8 };
             store =
             { ctree =
               [(ce7fc373c8675fa471e0d03f3b4eaaa8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c0), 1)]);
                   parent = <opaque>; key = ce7fc373c8675fa471e0d03f3b4eaaa8 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = ce7fc373c8675fa471e0d03f3b4eaaa8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = ce7fc373c8675fa471e0d03f3b4eaaa8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [[Command((Read c0), 1)]] };
         failure_detector =
         { Conspire_leader.FailureDetector.state = [(1: 2)(2: 2)(3: 2)];
           timeout = 2 }
         }
    actions: [CommitCommands(Command((Read c0), 1))
              Send(1,{ ctree = None;
                       cons =
                       (Some { vval = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               vterm = 0; term = 0;
                               commit_index = ce7fc373c8675fa471e0d03f3b4eaaa8 })
                       })
              Send(2,{ ctree = None;
                       cons =
                       (Some { vval = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               vterm = 0; term = 0;
                               commit_index = ce7fc373c8675fa471e0d03f3b4eaaa8 })
                       })
              Send(3,{ ctree = None;
                       cons =
                       (Some { vval = ce7fc373c8675fa471e0d03f3b4eaaa8;
                               vterm = 0; term = 0;
                               commit_index = ce7fc373c8675fa471e0d03f3b4eaaa8 })
                       })] |}] ;
  let t0, actions = Impl.advance t0 (Commands (c1 |> Iter.singleton)) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = abdba393eeeea0a5e0c9d59098ee62a1; vterm = 0; term = 0;
               commit_index = ce7fc373c8675fa471e0d03f3b4eaaa8 };
             store =
             { ctree =
               [(abdba393eeeea0a5e0c9d59098ee62a1:
                 { node =
                   (2, ce7fc373c8675fa471e0d03f3b4eaaa8, [Command((Read c1), 2)]);
                   parent = <opaque>; key = abdba393eeeea0a5e0c9d59098ee62a1 });
                (ce7fc373c8675fa471e0d03f3b4eaaa8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c0), 1)]);
                   parent = <opaque>; key = ce7fc373c8675fa471e0d03f3b4eaaa8 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = ce7fc373c8675fa471e0d03f3b4eaaa8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = ce7fc373c8675fa471e0d03f3b4eaaa8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [[Command((Read c0), 1)]] };
         failure_detector =
         { Conspire_leader.FailureDetector.state = [(1: 2)(2: 2)(3: 2)];
           timeout = 2 }
         }
    actions: [Send(1,{ ctree =
                       (Some { new_head = abdba393eeeea0a5e0c9d59098ee62a1;
                               extension =
                               [(2, ce7fc373c8675fa471e0d03f3b4eaaa8,
                                 [Command((Read c1), 2)])]
                               });
                       cons =
                       (Some { vval = abdba393eeeea0a5e0c9d59098ee62a1;
                               vterm = 0; term = 0;
                               commit_index = ce7fc373c8675fa471e0d03f3b4eaaa8 })
                       })
              Send(2,{ ctree =
                       (Some { new_head = abdba393eeeea0a5e0c9d59098ee62a1;
                               extension =
                               [(2, ce7fc373c8675fa471e0d03f3b4eaaa8,
                                 [Command((Read c1), 2)])]
                               });
                       cons =
                       (Some { vval = abdba393eeeea0a5e0c9d59098ee62a1;
                               vterm = 0; term = 0;
                               commit_index = ce7fc373c8675fa471e0d03f3b4eaaa8 })
                       })
              Send(3,{ ctree =
                       (Some { new_head = abdba393eeeea0a5e0c9d59098ee62a1;
                               extension =
                               [(2, ce7fc373c8675fa471e0d03f3b4eaaa8,
                                 [Command((Read c1), 2)])]
                               });
                       cons =
                       (Some { vval = abdba393eeeea0a5e0c9d59098ee62a1;
                               vterm = 0; term = 0;
                               commit_index = ce7fc373c8675fa471e0d03f3b4eaaa8 })
                       })] |}] ;
  let c1_clock = MP.Conspire.CTree.make_key root_clock [c1] in
  let update =
    Ok
      MP.Conspire.Rep.
        { ctree= Some {new_head= c1_clock; extension= [(1, c0_clock, [c1])]}
        ; cons= Some {term= 0; vterm= 0; vval= c1_clock; commit_index= c0_clock}
        }
  in
  let t1, actions = Impl.advance t1 (Recv (update, 0)) in
  print t1 actions ;
  [%expect
    {|
    +Nack for 0: Update is not rooted
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree = [(d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(0:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_leader.FailureDetector.state = [(0: 2)(2: 2)(3: 2)];
           timeout = 2 }
         }
    actions: [Send(0,{ commit = d41d8cd98f00b204e9800998ecf8427e })] |}] ;
  let t0, actions =
    Impl.advance t0 (Recv (Error MP.Conspire.Rep.{commit= root_clock}, 1))
  in
  print t0 actions ;
  (* note extension goes from the erroneous commit index *)
  [%expect
    {|
    +Acking 1
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = abdba393eeeea0a5e0c9d59098ee62a1; vterm = 0; term = 0;
               commit_index = ce7fc373c8675fa471e0d03f3b4eaaa8 };
             store =
             { ctree =
               [(abdba393eeeea0a5e0c9d59098ee62a1:
                 { node =
                   (2, ce7fc373c8675fa471e0d03f3b4eaaa8, [Command((Read c1), 2)]);
                   parent = <opaque>; key = abdba393eeeea0a5e0c9d59098ee62a1 });
                (ce7fc373c8675fa471e0d03f3b4eaaa8:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command((Read c0), 1)]);
                   parent = <opaque>; key = ce7fc373c8675fa471e0d03f3b4eaaa8 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = ce7fc373c8675fa471e0d03f3b4eaaa8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = ce7fc373c8675fa471e0d03f3b4eaaa8; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [[Command((Read c0), 1)]] };
         failure_detector =
         { Conspire_leader.FailureDetector.state = [(1: 2)(2: 2)(3: 2)];
           timeout = 2 }
         }
    actions: [Send(1,{ ctree =
                       (Some { new_head = abdba393eeeea0a5e0c9d59098ee62a1;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command((Read c0), 1)]);
                                 (2, ce7fc373c8675fa471e0d03f3b4eaaa8,
                                  [Command((Read c1), 2)])
                                 ]
                               });
                       cons = None })] |}]

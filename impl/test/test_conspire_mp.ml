open! Core
module MP = Impl_core__Conspire_mp
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
           { Conspire_mp.FailureDetector.state = []; timeout = 2 };
           stall_checker = <opaque> }
      actions: [] |}] ;
  let c1 = make_command (Read "c1") in
  let t, actions = Impl.advance t (Commands (c1 |> Iter.singleton)) in
  print t actions ;
  [%expect
    {|
      t: { config = <opaque>;
           conspire =
           { rep =
             { state =
               { vval = 1183a904cd1a3b8f3cf219be9367701f; vterm = 0; term = 0;
                 commit_index = 1183a904cd1a3b8f3cf219be9367701f };
               store =
               { ctree =
                 [(1183a904cd1a3b8f3cf219be9367701f:
                   { node =
                     (1, d41d8cd98f00b204e9800998ecf8427e, [Command(Read c1, 1)]);
                     parent = <opaque>; key = 1183a904cd1a3b8f3cf219be9367701f });
                  (d41d8cd98f00b204e9800998ecf8427e: Root)];
                 root = d41d8cd98f00b204e9800998ecf8427e };
               remotes = <opaque> };
             other_nodes_state = []; config = <opaque>;
             commit_log = [[Command(Read c1, 1)]] };
           failure_detector =
           { Conspire_mp.FailureDetector.state = []; timeout = 2 };
           stall_checker = <opaque> }
      actions: [CommitCommands(Command(Read c1, 1))] |}] ;
  let c2, c3 = (make_command (Read "c2"), make_command (Read "c3")) in
  let t, actions = Impl.advance t (Commands (Iter.of_list [c2; c3])) in
  print t actions ;
  [%expect
    {|
      t: { config = <opaque>;
           conspire =
           { rep =
             { state =
               { vval = 7f2c0aae94bf199f9b303480537af547; vterm = 0; term = 0;
                 commit_index = 7f2c0aae94bf199f9b303480537af547 };
               store =
               { ctree =
                 [(1183a904cd1a3b8f3cf219be9367701f:
                   { node =
                     (1, d41d8cd98f00b204e9800998ecf8427e, [Command(Read c1, 1)]);
                     parent = <opaque>; key = 1183a904cd1a3b8f3cf219be9367701f });
                  (7f2c0aae94bf199f9b303480537af547:
                   { node =
                     (2, 1183a904cd1a3b8f3cf219be9367701f,
                      [Command(Read c2, 3); Command(Read c3, 2)]);
                     parent = <opaque>; key = 7f2c0aae94bf199f9b303480537af547 });
                  (d41d8cd98f00b204e9800998ecf8427e: Root)];
                 root = d41d8cd98f00b204e9800998ecf8427e };
               remotes = <opaque> };
             other_nodes_state = []; config = <opaque>;
             commit_log =
             [[Command(Read c1, 1)][Command(Read c2, 3); Command(Read c3, 2)]] };
           failure_detector =
           { Conspire_mp.FailureDetector.state = []; timeout = 2 };
           stall_checker = <opaque> }
      actions: [CommitCommands(Command(Read c2, 3), Command(Read c3, 2))] |}]

let%expect_test "e2e commit" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t0 = create (c4 0) in
  let root_hd = t0.conspire.rep.store.root in
  let t1 = create (c4 1) in
  let c1 = make_command (Read "c1") in
  let t0, actions = Impl.advance t0 (Commands (Iter.of_list [c1])) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 1183a904cd1a3b8f3cf219be9367701f; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(1183a904cd1a3b8f3cf219be9367701f:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command(Read c1, 1)]);
                   parent = <opaque>; key = 1183a904cd1a3b8f3cf219be9367701f });
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
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [Send(1,{ ctree =
                       (Some { new_head = 1183a904cd1a3b8f3cf219be9367701f;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command(Read c1, 1)])]
                               });
                       cons =
                       (Some { vval = 1183a904cd1a3b8f3cf219be9367701f;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(2,{ ctree =
                       (Some { new_head = 1183a904cd1a3b8f3cf219be9367701f;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command(Read c1, 1)])]
                               });
                       cons =
                       (Some { vval = 1183a904cd1a3b8f3cf219be9367701f;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(3,{ ctree =
                       (Some { new_head = 1183a904cd1a3b8f3cf219be9367701f;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command(Read c1, 1)])]
                               });
                       cons =
                       (Some { vval = 1183a904cd1a3b8f3cf219be9367701f;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })] |}] ;
  let hd1 = Md5.of_hex_exn "1183a904cd1a3b8f3cf219be9367701f" in
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
               [(1183a904cd1a3b8f3cf219be9367701f:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command(Read c1, 1)]);
                   parent = <opaque>; key = 1183a904cd1a3b8f3cf219be9367701f });
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
         { Conspire_mp.FailureDetector.state = [(0: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
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
             { vval = 1183a904cd1a3b8f3cf219be9367701f; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(1183a904cd1a3b8f3cf219be9367701f:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command(Read c1, 1)]);
                   parent = <opaque>; key = 1183a904cd1a3b8f3cf219be9367701f });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(0:
             { vval = 1183a904cd1a3b8f3cf219be9367701f; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(0: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [Send(0,{ ctree = None;
                       cons =
                       (Some { vval = 1183a904cd1a3b8f3cf219be9367701f;
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
             { vval = 1183a904cd1a3b8f3cf219be9367701f; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(1183a904cd1a3b8f3cf219be9367701f:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command(Read c1, 1)]);
                   parent = <opaque>; key = 1183a904cd1a3b8f3cf219be9367701f });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 1183a904cd1a3b8f3cf219be9367701f; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [] |}] ;
  let t0, actions = Impl.advance t0 (Recv (Ok t0_vote, 2)) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 1183a904cd1a3b8f3cf219be9367701f; vterm = 0; term = 0;
               commit_index = 1183a904cd1a3b8f3cf219be9367701f };
             store =
             { ctree =
               [(1183a904cd1a3b8f3cf219be9367701f:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command(Read c1, 1)]);
                   parent = <opaque>; key = 1183a904cd1a3b8f3cf219be9367701f });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 1183a904cd1a3b8f3cf219be9367701f; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = 1183a904cd1a3b8f3cf219be9367701f; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [[Command(Read c1, 1)]] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [CommitCommands(Command(Read c1, 1))
              Send(1,{ ctree = None;
                       cons =
                       (Some { vval = 1183a904cd1a3b8f3cf219be9367701f;
                               vterm = 0; term = 0;
                               commit_index = 1183a904cd1a3b8f3cf219be9367701f })
                       })
              Send(2,{ ctree = None;
                       cons =
                       (Some { vval = 1183a904cd1a3b8f3cf219be9367701f;
                               vterm = 0; term = 0;
                               commit_index = 1183a904cd1a3b8f3cf219be9367701f })
                       })
              Send(3,{ ctree = None;
                       cons =
                       (Some { vval = 1183a904cd1a3b8f3cf219be9367701f;
                               vterm = 0; term = 0;
                               commit_index = 1183a904cd1a3b8f3cf219be9367701f })
                       })] |}] ;
  ignore (t0, t1)

let%expect_test "e2e conflict resolution" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t0 = create (c4 0) in
  let _t1 = create (c4 1) in
  let c0 = make_command (Read "c0") in
  let c1 = make_command (Read "c1") in
  let t0, actions = Impl.advance t0 (Commands (Iter.of_list [c0])) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 1fddcd0db3e43a000153d0c4de56a7cc; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(1fddcd0db3e43a000153d0c4de56a7cc:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command(Read c0, 1)]);
                   parent = <opaque>; key = 1fddcd0db3e43a000153d0c4de56a7cc });
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
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [Send(1,{ ctree =
                       (Some { new_head = 1fddcd0db3e43a000153d0c4de56a7cc;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command(Read c0, 1)])]
                               });
                       cons =
                       (Some { vval = 1fddcd0db3e43a000153d0c4de56a7cc;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(2,{ ctree =
                       (Some { new_head = 1fddcd0db3e43a000153d0c4de56a7cc;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command(Read c0, 1)])]
                               });
                       cons =
                       (Some { vval = 1fddcd0db3e43a000153d0c4de56a7cc;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(3,{ ctree =
                       (Some { new_head = 1fddcd0db3e43a000153d0c4de56a7cc;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command(Read c0, 1)])]
                               });
                       cons =
                       (Some { vval = 1fddcd0db3e43a000153d0c4de56a7cc;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })] |}] ;
  let root_clock = t0.conspire.rep.store.root in
  let c0_node = (1, root_clock, [c0]) in
  let c0_clock = Md5.of_hex_exn "1fddcd0db3e43a000153d0c4de56a7cc" in
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
             { vval = 1fddcd0db3e43a000153d0c4de56a7cc; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(1fddcd0db3e43a000153d0c4de56a7cc:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command(Read c0, 1)]);
                   parent = <opaque>; key = 1fddcd0db3e43a000153d0c4de56a7cc });
                (620122743bc84de6b418bd632ea0cdc2:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command(Read c1, 2)]);
                   parent = <opaque>; key = 620122743bc84de6b418bd632ea0cdc2 });
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
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
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
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 1fddcd0db3e43a000153d0c4de56a7cc; vterm = 0; term = 1;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(1fddcd0db3e43a000153d0c4de56a7cc:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command(Read c0, 1)]);
                   parent = <opaque>; key = 1fddcd0db3e43a000153d0c4de56a7cc });
                (620122743bc84de6b418bd632ea0cdc2:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command(Read c1, 2)]);
                   parent = <opaque>; key = 620122743bc84de6b418bd632ea0cdc2 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 620122743bc84de6b418bd632ea0cdc2; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = 1fddcd0db3e43a000153d0c4de56a7cc; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [Send(1,{ ctree = None;
                       cons =
                       (Some { vval = 1fddcd0db3e43a000153d0c4de56a7cc;
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
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 1fddcd0db3e43a000153d0c4de56a7cc; vterm = 1; term = 1;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(1fddcd0db3e43a000153d0c4de56a7cc:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command(Read c0, 1)]);
                   parent = <opaque>; key = 1fddcd0db3e43a000153d0c4de56a7cc });
                (620122743bc84de6b418bd632ea0cdc2:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command(Read c1, 2)]);
                   parent = <opaque>; key = 620122743bc84de6b418bd632ea0cdc2 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 620122743bc84de6b418bd632ea0cdc2; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = 1fddcd0db3e43a000153d0c4de56a7cc; vterm = 0; term = 1;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = 620122743bc84de6b418bd632ea0cdc2; vterm = 0; term = 1;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [Send(1,{ ctree = None;
                       cons =
                       (Some { vval = 1fddcd0db3e43a000153d0c4de56a7cc;
                               vterm = 1; term = 1;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(2,{ ctree = None;
                       cons =
                       (Some { vval = 1fddcd0db3e43a000153d0c4de56a7cc;
                               vterm = 1; term = 1;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(3,{ ctree = None;
                       cons =
                       (Some { vval = 1fddcd0db3e43a000153d0c4de56a7cc;
                               vterm = 1; term = 1;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })] |}] ;
  ignore (t0, _t1, c0_node, c1_node)

let%expect_test "message loss" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t0 = create (c4 0) in
  let t1 = create (c4 1) in
  let c0 = make_command (Read "c0") in
  let c1 = make_command (Read "c1") in
  (* this message is lost *)
  let t0, actions = Impl.advance t0 (Commands (Iter.of_list [c0])) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 1fddcd0db3e43a000153d0c4de56a7cc; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(1fddcd0db3e43a000153d0c4de56a7cc:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command(Read c0, 1)]);
                   parent = <opaque>; key = 1fddcd0db3e43a000153d0c4de56a7cc });
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
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [Send(1,{ ctree =
                       (Some { new_head = 1fddcd0db3e43a000153d0c4de56a7cc;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command(Read c0, 1)])]
                               });
                       cons =
                       (Some { vval = 1fddcd0db3e43a000153d0c4de56a7cc;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(2,{ ctree =
                       (Some { new_head = 1fddcd0db3e43a000153d0c4de56a7cc;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command(Read c0, 1)])]
                               });
                       cons =
                       (Some { vval = 1fddcd0db3e43a000153d0c4de56a7cc;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })
              Send(3,{ ctree =
                       (Some { new_head = 1fddcd0db3e43a000153d0c4de56a7cc;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command(Read c0, 1)])]
                               });
                       cons =
                       (Some { vval = 1fddcd0db3e43a000153d0c4de56a7cc;
                               vterm = 0; term = 0;
                               commit_index = d41d8cd98f00b204e9800998ecf8427e })
                       })] |}] ;
  let root_clock = t0.conspire.rep.store.root in
  let c0_clock = Md5.of_hex_exn "1fddcd0db3e43a000153d0c4de56a7cc" in
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
             { vval = 1fddcd0db3e43a000153d0c4de56a7cc; vterm = 0; term = 0;
               commit_index = 1fddcd0db3e43a000153d0c4de56a7cc };
             store =
             { ctree =
               [(1fddcd0db3e43a000153d0c4de56a7cc:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command(Read c0, 1)]);
                   parent = <opaque>; key = 1fddcd0db3e43a000153d0c4de56a7cc });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = 1fddcd0db3e43a000153d0c4de56a7cc; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = 1fddcd0db3e43a000153d0c4de56a7cc; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [[Command(Read c0, 1)]] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [CommitCommands(Command(Read c0, 1))
              Send(1,{ ctree = None;
                       cons =
                       (Some { vval = 1fddcd0db3e43a000153d0c4de56a7cc;
                               vterm = 0; term = 0;
                               commit_index = 1fddcd0db3e43a000153d0c4de56a7cc })
                       })
              Send(2,{ ctree = None;
                       cons =
                       (Some { vval = 1fddcd0db3e43a000153d0c4de56a7cc;
                               vterm = 0; term = 0;
                               commit_index = 1fddcd0db3e43a000153d0c4de56a7cc })
                       })
              Send(3,{ ctree = None;
                       cons =
                       (Some { vval = 1fddcd0db3e43a000153d0c4de56a7cc;
                               vterm = 0; term = 0;
                               commit_index = 1fddcd0db3e43a000153d0c4de56a7cc })
                       })] |}] ;
  let t0, actions = Impl.advance t0 (Commands (c1 |> Iter.singleton)) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = b1d8bad167372c336ae91f91677feca1; vterm = 0; term = 0;
               commit_index = 1fddcd0db3e43a000153d0c4de56a7cc };
             store =
             { ctree =
               [(1fddcd0db3e43a000153d0c4de56a7cc:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command(Read c0, 1)]);
                   parent = <opaque>; key = 1fddcd0db3e43a000153d0c4de56a7cc });
                (b1d8bad167372c336ae91f91677feca1:
                 { node =
                   (2, 1fddcd0db3e43a000153d0c4de56a7cc, [Command(Read c1, 2)]);
                   parent = <opaque>; key = b1d8bad167372c336ae91f91677feca1 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = 1fddcd0db3e43a000153d0c4de56a7cc; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = 1fddcd0db3e43a000153d0c4de56a7cc; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [[Command(Read c0, 1)]] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [Send(1,{ ctree =
                       (Some { new_head = b1d8bad167372c336ae91f91677feca1;
                               extension =
                               [(2, 1fddcd0db3e43a000153d0c4de56a7cc,
                                 [Command(Read c1, 2)])]
                               });
                       cons =
                       (Some { vval = b1d8bad167372c336ae91f91677feca1;
                               vterm = 0; term = 0;
                               commit_index = 1fddcd0db3e43a000153d0c4de56a7cc })
                       })
              Send(2,{ ctree =
                       (Some { new_head = b1d8bad167372c336ae91f91677feca1;
                               extension =
                               [(2, 1fddcd0db3e43a000153d0c4de56a7cc,
                                 [Command(Read c1, 2)])]
                               });
                       cons =
                       (Some { vval = b1d8bad167372c336ae91f91677feca1;
                               vterm = 0; term = 0;
                               commit_index = 1fddcd0db3e43a000153d0c4de56a7cc })
                       })
              Send(3,{ ctree =
                       (Some { new_head = b1d8bad167372c336ae91f91677feca1;
                               extension =
                               [(2, 1fddcd0db3e43a000153d0c4de56a7cc,
                                 [Command(Read c1, 2)])]
                               });
                       cons =
                       (Some { vval = b1d8bad167372c336ae91f91677feca1;
                               vterm = 0; term = 0;
                               commit_index = 1fddcd0db3e43a000153d0c4de56a7cc })
                       })] |}] ;
  let c1_clock = Md5.of_hex_exn "b1d8bad167372c336ae91f91677feca1" in
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
         { Conspire_mp.FailureDetector.state = [(0: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [Send(0,{ commit = d41d8cd98f00b204e9800998ecf8427e })] |}] ;
  let t0, actions =
    Impl.advance t0 (Recv (Error MP.Conspire.Rep.{commit= root_clock}, 1))
  in
  print t0 actions ;
  (* note extension goes from the erroneous commit index *)
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = b1d8bad167372c336ae91f91677feca1; vterm = 0; term = 0;
               commit_index = 1fddcd0db3e43a000153d0c4de56a7cc };
             store =
             { ctree =
               [(1fddcd0db3e43a000153d0c4de56a7cc:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e, [Command(Read c0, 1)]);
                   parent = <opaque>; key = 1fddcd0db3e43a000153d0c4de56a7cc });
                (b1d8bad167372c336ae91f91677feca1:
                 { node =
                   (2, 1fddcd0db3e43a000153d0c4de56a7cc, [Command(Read c1, 2)]);
                   parent = <opaque>; key = b1d8bad167372c336ae91f91677feca1 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = 1fddcd0db3e43a000153d0c4de56a7cc; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = 1fddcd0db3e43a000153d0c4de56a7cc; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [[Command(Read c0, 1)]] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [Send(1,{ ctree =
                       (Some { new_head = b1d8bad167372c336ae91f91677feca1;
                               extension =
                               [(1, d41d8cd98f00b204e9800998ecf8427e,
                                 [Command(Read c0, 1)]);
                                 (2, 1fddcd0db3e43a000153d0c4de56a7cc,
                                  [Command(Read c1, 2)])
                                 ]
                               });
                       cons = None })] |}]

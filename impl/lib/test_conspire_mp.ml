open! Core
module MP = Conspire_mp
module Imp = Actions_f.ImperativeActions (MP.Types)
module Impl = MP.Make (Imp)
module Rep = MP.Conspire.Rep
open Impl
open Types
open MP.Types

let action_pp = Ocons_core.Consensus_intf.action_pp ~pp_msg:pp_message

let make_clock term clocks =
  (*
  let clock =
    Map.of_alist_exn (module Int) (List.mapi clocks ~f:(fun idx v -> (idx, v)))
  in
  *)
  Conspire_command_tree.VectorClock.{term; clock= clocks}

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
             { state = { vval = 0:[0]; vterm = 0; term = 0; commit_index = 0:[0] };
               store = { ctree = [(0:[0]: Root)]; root = 0:[0] };
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
             { state = { vval = 0:[1]; vterm = 0; term = 0; commit_index = 0:[1] };
               store =
               { ctree =
                 [(0:[0]: Root);
                  (0:[1]:
                   { node = (1, 0:[0], [Command(Read c1, 1)]); parent = <opaque>;
                     vc = 0:[1] })];
                 root = 0:[0] };
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
             { state = { vval = 0:[2]; vterm = 0; term = 0; commit_index = 0:[2] };
               store =
               { ctree =
                 [(0:[0]: Root);
                  (0:[1]:
                   { node = (1, 0:[0], [Command(Read c1, 1)]); parent = <opaque>;
                     vc = 0:[1] });
                  (0:[2]:
                   { node = (2, 0:[1], [Command(Read c2, 3); Command(Read c3, 2)]);
                     parent = <opaque>; vc = 0:[2] })];
                 root = 0:[0] };
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
  let t1 = create (c4 1) in
  let t2 = create (c4 2) in
  let c1 = make_command (Read "c1") in
  let t0, actions = Impl.advance t0 (Commands (Iter.of_list [c1])) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] };
             store =
             { ctree =
               [(0:[0,0,0,0]: Root);
                (0:[1,0,0,0]:
                 { node = (1, 0:[0,0,0,0], [Command(Read c1, 1)]);
                   parent = <opaque>; vc = 0:[1,0,0,0] })];
               root = 0:[0,0,0,0] };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (2:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (3:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [Send(1,{ ctree =
                       (Some { new_head = 0:[1,0,0,0];
                               extension =
                               [(1, 0:[0,0,0,0], [Command(Read c1, 1)])] });
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[0,0,0,0] })
                       })
              Send(2,{ ctree =
                       (Some { new_head = 0:[1,0,0,0];
                               extension =
                               [(1, 0:[0,0,0,0], [Command(Read c1, 1)])] });
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[0,0,0,0] })
                       })
              Send(3,{ ctree =
                       (Some { new_head = 0:[1,0,0,0];
                               extension =
                               [(1, 0:[0,0,0,0], [Command(Read c1, 1)])] });
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[0,0,0,0] })
                       })] |}] ;
  let update =
    MP.Conspire.Rep.
      { ctree=
          Some
            { new_head= make_clock 0 [1; 0; 0; 0]
            ; extension= [(1, make_clock 0 [0; 0; 0; 0], [c1])] }
      ; cons= None }
  in
  let t1, actions = Impl.advance t1 (Recv (Ok update, 0)) in
  print t1 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] };
             store =
             { ctree =
               [(0:[0,0,0,0]: Root);
                (0:[1,0,0,0]:
                 { node = (1, 0:[0,0,0,0], [Command(Read c1, 1)]);
                   parent = <opaque>; vc = 0:[1,0,0,0] })];
               root = 0:[0,0,0,0] };
             remotes = <opaque> };
           other_nodes_state =
           [(0:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (2:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (3:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(0: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [] |}] ;
  let t0_vote =
    Rep.
      { ctree= None
      ; cons=
          Some
            { term= 0
            ; vterm= 0
            ; vval= make_clock 0 [1; 0; 0; 0]
            ; commit_index= make_clock 0 [0; 0; 0; 0] } }
  in
  let t1, actions = Impl.advance t1 (Recv (Ok t0_vote, 0)) in
  print t1 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] };
             store =
             { ctree =
               [(0:[0,0,0,0]: Root);
                (0:[1,0,0,0]:
                 { node = (1, 0:[0,0,0,0], [Command(Read c1, 1)]);
                   parent = <opaque>; vc = 0:[1,0,0,0] })];
               root = 0:[0,0,0,0] };
             remotes = <opaque> };
           other_nodes_state =
           [(0:
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (2:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (3:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(0: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [Send(0,{ ctree = None;
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[0,0,0,0] })
                       })] |}] ;
  let t0, actions = Impl.advance t0 (Recv (Ok t0_vote, 1)) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] };
             store =
             { ctree =
               [(0:[0,0,0,0]: Root);
                (0:[1,0,0,0]:
                 { node = (1, 0:[0,0,0,0], [Command(Read c1, 1)]);
                   parent = <opaque>; vc = 0:[1,0,0,0] })];
               root = 0:[0,0,0,0] };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (2:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (3:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] })];
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
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[1,0,0,0] };
             store =
             { ctree =
               [(0:[0,0,0,0]: Root);
                (0:[1,0,0,0]:
                 { node = (1, 0:[0,0,0,0], [Command(Read c1, 1)]);
                   parent = <opaque>; vc = 0:[1,0,0,0] })];
               root = 0:[0,0,0,0] };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (2:
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (3:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] })];
           config = <opaque>; commit_log = [[Command(Read c1, 1)]] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [CommitCommands(Command(Read c1, 1))
              Send(1,{ ctree = None;
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[1,0,0,0] })
                       })
              Send(2,{ ctree = None;
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[1,0,0,0] })
                       })
              Send(3,{ ctree = None;
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[1,0,0,0] })
                       })] |}] ;
  ignore (t0, t1, t2)

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
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] };
             store =
             { ctree =
               [(0:[0,0,0,0]: Root);
                (0:[1,0,0,0]:
                 { node = (1, 0:[0,0,0,0], [Command(Read c0, 1)]);
                   parent = <opaque>; vc = 0:[1,0,0,0] })];
               root = 0:[0,0,0,0] };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (2:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (3:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [Send(1,{ ctree =
                       (Some { new_head = 0:[1,0,0,0];
                               extension =
                               [(1, 0:[0,0,0,0], [Command(Read c0, 1)])] });
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[0,0,0,0] })
                       })
              Send(2,{ ctree =
                       (Some { new_head = 0:[1,0,0,0];
                               extension =
                               [(1, 0:[0,0,0,0], [Command(Read c0, 1)])] });
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[0,0,0,0] })
                       })
              Send(3,{ ctree =
                       (Some { new_head = 0:[1,0,0,0];
                               extension =
                               [(1, 0:[0,0,0,0], [Command(Read c0, 1)])] });
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[0,0,0,0] })
                       })] |}] ;
  (*
  let t1, actions = Impl.advance t1 (Commands (Iter.of_list [c1])) in
  print t1 actions ;
  [%expect
    {||}] ;
                             *)
  let root_clock = make_clock 0 [0; 0; 0; 0] in
  let c0_node = (1, make_clock 0 [0; 0; 0; 0], [c0]) in
  let c0_clock = make_clock 0 [1; 0; 0; 0] in
  let c1_node = (1, make_clock 0 [0; 0; 0; 0], [c1]) in
  let c1_clock = make_clock 0 [0; 1; 0; 0] in
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
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] };
             store =
             { ctree =
               [(0:[0,0,0,0]: Root);
                (0:[0,1,0,0]:
                 { node = (1, 0:[0,0,0,0], [Command(Read c1, 2)]);
                   parent = <opaque>; vc = 0:[0,1,0,0] });
                (0:[1,0,0,0]:
                 { node = (1, 0:[0,0,0,0], [Command(Read c0, 1)]);
                   parent = <opaque>; vc = 0:[1,0,0,0] })];
               root = 0:[0,0,0,0] };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (2:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (3:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] })];
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
             { vval = 0:[1,0,0,0]; vterm = 0; term = 1;
               commit_index = 0:[0,0,0,0] };
             store =
             { ctree =
               [(0:[0,0,0,0]: Root);
                (0:[0,1,0,0]:
                 { node = (1, 0:[0,0,0,0], [Command(Read c1, 2)]);
                   parent = <opaque>; vc = 0:[0,1,0,0] });
                (0:[1,0,0,0]:
                 { node = (1, 0:[0,0,0,0], [Command(Read c0, 1)]);
                   parent = <opaque>; vc = 0:[1,0,0,0] })];
               root = 0:[0,0,0,0] };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 0:[0,1,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (2:
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (3:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [Send(1,{ ctree = None;
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 0; term = 1;
                               commit_index = 0:[0,0,0,0] })
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
             { vval = 0:[1,0,0,0]; vterm = 1; term = 1;
               commit_index = 0:[0,0,0,0] };
             store =
             { ctree =
               [(0:[0,0,0,0]: Root);
                (0:[0,1,0,0]:
                 { node = (1, 0:[0,0,0,0], [Command(Read c1, 2)]);
                   parent = <opaque>; vc = 0:[0,1,0,0] });
                (0:[1,0,0,0]:
                 { node = (1, 0:[0,0,0,0], [Command(Read c0, 1)]);
                   parent = <opaque>; vc = 0:[1,0,0,0] })];
               root = 0:[0,0,0,0] };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 0:[0,1,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (2:
             { vval = 0:[1,0,0,0]; vterm = 0; term = 1;
               commit_index = 0:[0,0,0,0] });
            (3:
             { vval = 0:[0,1,0,0]; vterm = 0; term = 1;
               commit_index = 0:[0,0,0,0] })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [Send(1,{ ctree = None;
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 1; term = 1;
                               commit_index = 0:[0,0,0,0] })
                       })
              Send(2,{ ctree = None;
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 1; term = 1;
                               commit_index = 0:[0,0,0,0] })
                       })
              Send(3,{ ctree = None;
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 1; term = 1;
                               commit_index = 0:[0,0,0,0] })
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
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] };
             store =
             { ctree =
               [(0:[0,0,0,0]: Root);
                (0:[1,0,0,0]:
                 { node = (1, 0:[0,0,0,0], [Command(Read c0, 1)]);
                   parent = <opaque>; vc = 0:[1,0,0,0] })];
               root = 0:[0,0,0,0] };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (2:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (3:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [Send(1,{ ctree =
                       (Some { new_head = 0:[1,0,0,0];
                               extension =
                               [(1, 0:[0,0,0,0], [Command(Read c0, 1)])] });
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[0,0,0,0] })
                       })
              Send(2,{ ctree =
                       (Some { new_head = 0:[1,0,0,0];
                               extension =
                               [(1, 0:[0,0,0,0], [Command(Read c0, 1)])] });
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[0,0,0,0] })
                       })
              Send(3,{ ctree =
                       (Some { new_head = 0:[1,0,0,0];
                               extension =
                               [(1, 0:[0,0,0,0], [Command(Read c0, 1)])] });
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[0,0,0,0] })
                       })] |}] ;
  let root_clock = make_clock 0 [0; 0; 0; 0] in
  let c0_clock = make_clock 0 [1; 0; 0; 0] in
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
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[1,0,0,0] };
             store =
             { ctree =
               [(0:[0,0,0,0]: Root);
                (0:[1,0,0,0]:
                 { node = (1, 0:[0,0,0,0], [Command(Read c0, 1)]);
                   parent = <opaque>; vc = 0:[1,0,0,0] })];
               root = 0:[0,0,0,0] };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (2:
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (3:
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] })];
           config = <opaque>; commit_log = [[Command(Read c0, 1)]] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [CommitCommands(Command(Read c0, 1))
              Send(1,{ ctree = None;
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[1,0,0,0] })
                       })
              Send(2,{ ctree = None;
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[1,0,0,0] })
                       })
              Send(3,{ ctree = None;
                       cons =
                       (Some { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[1,0,0,0] })
                       })] |}] ;
  let t0, actions = Impl.advance t0 (Commands (c1 |> Iter.singleton)) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 0:[2,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[1,0,0,0] };
             store =
             { ctree =
               [(0:[0,0,0,0]: Root);
                (0:[1,0,0,0]:
                 { node = (1, 0:[0,0,0,0], [Command(Read c0, 1)]);
                   parent = <opaque>; vc = 0:[1,0,0,0] });
                (0:[2,0,0,0]:
                 { node = (2, 0:[1,0,0,0], [Command(Read c1, 2)]);
                   parent = <opaque>; vc = 0:[2,0,0,0] })];
               root = 0:[0,0,0,0] };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (2:
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (3:
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] })];
           config = <opaque>; commit_log = [[Command(Read c0, 1)]] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [Send(1,{ ctree =
                       (Some { new_head = 0:[2,0,0,0];
                               extension =
                               [(2, 0:[1,0,0,0], [Command(Read c1, 2)])] });
                       cons =
                       (Some { vval = 0:[2,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[1,0,0,0] })
                       })
              Send(2,{ ctree =
                       (Some { new_head = 0:[2,0,0,0];
                               extension =
                               [(2, 0:[1,0,0,0], [Command(Read c1, 2)])] });
                       cons =
                       (Some { vval = 0:[2,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[1,0,0,0] })
                       })
              Send(3,{ ctree =
                       (Some { new_head = 0:[2,0,0,0];
                               extension =
                               [(2, 0:[1,0,0,0], [Command(Read c1, 2)])] });
                       cons =
                       (Some { vval = 0:[2,0,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[1,0,0,0] })
                       })] |}] ;
  let c1_clock = make_clock 0 [2; 0; 0; 0] in
  let update =
    Ok
      MP.Conspire.Rep.
        { ctree=
            Some
              { new_head= make_clock 0 [0; 1; 0; 0]
              ; extension= [(1, make_clock 0 [1; 0; 0; 0], [c1])] }
        ; cons=
            Some
              { term= 0
              ; vterm= 0
              ; vval= c1_clock
              ; commit_index= make_clock 0 [0; 0; 0; 0] } }
  in
  let t1, actions = Impl.advance t1 (Recv (update, 0)) in
  print t1 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] };
             store = { ctree = [(0:[0,0,0,0]: Root)]; root = 0:[0,0,0,0] };
             remotes = <opaque> };
           other_nodes_state =
           [(0:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (2:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (3:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] })];
           config = <opaque>; commit_log = [] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(0: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [Send(0,{ commit = 0:[0,0,0,0] })] |}] ;
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
             { vval = 0:[2,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[1,0,0,0] };
             store =
             { ctree =
               [(0:[0,0,0,0]: Root);
                (0:[1,0,0,0]:
                 { node = (1, 0:[0,0,0,0], [Command(Read c0, 1)]);
                   parent = <opaque>; vc = 0:[1,0,0,0] });
                (0:[2,0,0,0]:
                 { node = (2, 0:[1,0,0,0], [Command(Read c1, 2)]);
                   parent = <opaque>; vc = 0:[2,0,0,0] })];
               root = 0:[0,0,0,0] };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (2:
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] });
            (3:
             { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
               commit_index = 0:[0,0,0,0] })];
           config = <opaque>; commit_log = [[Command(Read c0, 1)]] };
         failure_detector =
         { Conspire_mp.FailureDetector.state = [(1: 2)(2: 2)(3: 2)]; timeout = 2
           };
         stall_checker = <opaque> }
    actions: [Send(1,{ ctree =
                       (Some { new_head = 0:[2,0,0,0];
                               extension =
                               [(1, 0:[0,0,0,0], [Command(Read c0, 1)]);
                                 (2, 0:[1,0,0,0], [Command(Read c1, 2)])]
                               });
                       cons = None })] |}]

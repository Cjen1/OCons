open! Core
open! Types
open! Utils
open! Actions_f
module Imp = ImperativeActions (Conspire.Types)
module Impl = Conspire.Make (Imp)
open! Conspire.Types
open! Impl
open! Conspire.GlobalTypes

let action_pp = Ocons_core.Consensus_intf.action_pp ~pp_msg:PP.message_pp

let make_clock term clocks =
  let clock =
    Map.of_alist_exn (module Int) (List.mapi clocks ~f:(fun idx v -> (idx, v)))
  in
  Conspire_command_tree.VectorClock.{term; clock}

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
      t: { rep =
           { state = { vval = 0:[0]; vterm = 0; term = 0; commit_index = 0:[0] };
             store = { ctree = [(0:[0], Root)] }; change_flag = false; remotes = []
             };
           other_nodes = []; failure_detector = {}; config = <opaque>;
           command_queue = <opaque>; stall_checker = <opaque>; commit_log = [] }
      actions: [] |}] ;
  let c1 = make_command (Read "c1") in
  let t, actions = Impl.advance t (Commands (c1 |> Iter.singleton)) in
  print t actions ;
  [%expect
    {|
      t: { rep =
           { state = { vval = 0:[1]; vterm = 0; term = 0; commit_index = 0:[1] };
             store =
             { ctree = [(0:[0], Root): (0:[1], (1, 0:[0], [Command(Read c1, 1)]))]
               };
             change_flag = false; remotes = [] };
           other_nodes = []; failure_detector = {}; config = <opaque>;
           command_queue = <opaque>; stall_checker = <opaque>;
           commit_log = [[Command(Read c1, 1)]] }
      actions: [CommitCommands(Command(Read c1, 1))
                Broadcast((ConsUpdate
                             { vval = 0:[1]; vterm = 0; term = 0;
                               commit_index = 0:[1] }))] |}] ;
  let c2, c3 = (make_command (Read "c2"), make_command (Read "c3")) in
  let t, actions = Impl.advance t (Commands (Iter.of_list [c2; c3])) in
  print t actions ;
  [%expect
    {|
      t: { rep =
           { state = { vval = 0:[2]; vterm = 0; term = 0; commit_index = 0:[2] };
             store =
             { ctree =
               [(0:[0], Root): (0:[1], (1, 0:[0], [Command(Read c1, 1)])):
                (0:[2], (2, 0:[1], [Command(Read c2, 3), Command(Read c3, 2)]))]
               };
             change_flag = false; remotes = [] };
           other_nodes = []; failure_detector = {}; config = <opaque>;
           command_queue = <opaque>; stall_checker = <opaque>;
           commit_log =
           [[Command(Read c1, 1)][Command(Read c2, 3), Command(Read c3, 2)]] }
      actions: [CommitCommands(Command(Read c2, 3), Command(Read c3, 2))
                Broadcast((ConsUpdate
                             { vval = 0:[2]; vterm = 0; term = 0;
                               commit_index = 0:[2] }))] |}]

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
      t: { rep =
           { state =
             { vval = 0:[0,1,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
               };
             store =
             { ctree =
               [(0:[0,0,0,0], Root):
                (0:[0,1,0,0], (1, 0:[0,0,0,0], [Command(Read c1, 1)]))]
               };
             change_flag = false;
             remotes =
             [(0, { expected = <opaque> }): (2, { expected = <opaque> }):
              (3, { expected = <opaque> })]
             };
           other_nodes =
           [(0,
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
               }):
            (2,
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
               }):
            (3,
             { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
               })];
           failure_detector = {3: 2, 2: 2, 0: 2}; config = <opaque>;
           command_queue = <opaque>; stall_checker = <opaque>; commit_log = [] }
      actions: [Send(0,(CTreeUpdate
                          { new_head = 0:[0,1,0,0];
                            extension = [(1, 0:[0,0,0,0], [Command(Read c1, 1)])] }))
                Send(2,(CTreeUpdate
                          { new_head = 0:[0,1,0,0];
                            extension = [(1, 0:[0,0,0,0], [Command(Read c1, 1)])] }))
                Send(3,(CTreeUpdate
                          { new_head = 0:[0,1,0,0];
                            extension = [(1, 0:[0,0,0,0], [Command(Read c1, 1)])] }))
                Broadcast((ConsUpdate
                             { vval = 0:[0,1,0,0]; vterm = 0; term = 0;
                               commit_index = 0:[0,0,0,0] }))] |}] ;
  let update =
    CTreeUpdate
      { new_head= make_clock 0 [0; 1; 0; 0]
      ; extension= [(1, make_clock 0 [0; 0; 0; 0], [c1])] }
  in
  let t2, actions = Impl.advance t2 (Recv (update, 1)) in
  print t2 actions ;
  [%expect
    {|
    t: { rep =
         { state =
           { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             };
           store =
           { ctree =
             [(0:[0,0,0,0], Root):
              (0:[0,1,0,0], (1, 0:[0,0,0,0], [Command(Read c1, 1)]))]
             };
           change_flag = false;
           remotes =
           [(0, { expected = <opaque> }): (1, { expected = <opaque> }):
            (3, { expected = <opaque> })]
           };
         other_nodes =
         [(0,
           { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             }):
          (1,
           { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             }):
          (3,
           { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             })];
         failure_detector = {1: 2, 3: 2, 0: 2}; config = <opaque>;
         command_queue = <opaque>; stall_checker = <opaque>; commit_log = [] }
    actions: [] |}] ;
  let t1_vote =
    ConsUpdate
      { term= 0
      ; vterm= 0
      ; vval= make_clock 0 [0; 1; 0; 0]
      ; commit_index= make_clock 0 [0; 0; 0; 0] }
  in
  let t2, actions = Impl.advance t2 (Recv (t1_vote, 1)) in
  print t2 actions ;
  [%expect
    {|
    t: { rep =
         { state =
           { vval = 0:[0,1,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             };
           store =
           { ctree =
             [(0:[0,0,0,0], Root):
              (0:[0,1,0,0], (1, 0:[0,0,0,0], [Command(Read c1, 1)]))]
             };
           change_flag = false;
           remotes =
           [(0, { expected = <opaque> }): (1, { expected = <opaque> }):
            (3, { expected = <opaque> })]
           };
         other_nodes =
         [(0,
           { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             }):
          (1,
           { vval = 0:[0,1,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             }):
          (3,
           { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             })];
         failure_detector = {1: 2, 3: 2, 0: 2}; config = <opaque>;
         command_queue = <opaque>; stall_checker = <opaque>; commit_log = [] }
    actions: [Send(0,(CTreeUpdate
                        { new_head = 0:[0,1,0,0];
                          extension = [(1, 0:[0,0,0,0], [Command(Read c1, 1)])] }))
              Send(3,(CTreeUpdate
                        { new_head = 0:[0,1,0,0];
                          extension = [(1, 0:[0,0,0,0], [Command(Read c1, 1)])] }))
              Broadcast((ConsUpdate
                           { vval = 0:[0,1,0,0]; vterm = 0; term = 0;
                             commit_index = 0:[0,0,0,0] }))] |}] ;
  let t1, actions = Impl.advance t1 (Recv (t1_vote, 2)) in
  print t1 actions ;
  [%expect
    {|
    t: { rep =
         { state =
           { vval = 0:[0,1,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             };
           store =
           { ctree =
             [(0:[0,0,0,0], Root):
              (0:[0,1,0,0], (1, 0:[0,0,0,0], [Command(Read c1, 1)]))]
             };
           change_flag = false;
           remotes =
           [(0, { expected = <opaque> }): (2, { expected = <opaque> }):
            (3, { expected = <opaque> })]
           };
         other_nodes =
         [(0,
           { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             }):
          (2,
           { vval = 0:[0,1,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             }):
          (3,
           { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             })];
         failure_detector = {3: 2, 2: 2, 0: 2}; config = <opaque>;
         command_queue = <opaque>; stall_checker = <opaque>; commit_log = [] }
    actions: [] |}] ;
  let t1, actions = Impl.advance t1 (Recv (t1_vote, 3)) in
  print t1 actions ;
  [%expect
    {|
    t: { rep =
         { state =
           { vval = 0:[0,1,0,0]; vterm = 0; term = 0; commit_index = 0:[0,1,0,0]
             };
           store =
           { ctree =
             [(0:[0,0,0,0], Root):
              (0:[0,1,0,0], (1, 0:[0,0,0,0], [Command(Read c1, 1)]))]
             };
           change_flag = false;
           remotes =
           [(0, { expected = <opaque> }): (2, { expected = <opaque> }):
            (3, { expected = <opaque> })]
           };
         other_nodes =
         [(0,
           { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             }):
          (2,
           { vval = 0:[0,1,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             }):
          (3,
           { vval = 0:[0,1,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             })];
         failure_detector = {3: 2, 2: 2, 0: 2}; config = <opaque>;
         command_queue = <opaque>; stall_checker = <opaque>;
         commit_log = [[Command(Read c1, 1)]] }
    actions: [CommitCommands(Command(Read c1, 1))] |}] ;
  ignore (t1, t2, t3)

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
    t: { rep =
         { state =
           { vval = 0:[1,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             };
           store =
           { ctree =
             [(0:[0,0,0,0], Root):
              (0:[1,0,0,0], (1, 0:[0,0,0,0], [Command(Read c0, 1)]))]
             };
           change_flag = false;
           remotes =
           [(1, { expected = <opaque> }): (2, { expected = <opaque> }):
            (3, { expected = <opaque> })]
           };
         other_nodes =
         [(1,
           { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             }):
          (2,
           { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             }):
          (3,
           { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             })];
         failure_detector = {1: 2, 3: 2, 2: 2}; config = <opaque>;
         command_queue = <opaque>; stall_checker = <opaque>; commit_log = [] }
    actions: [Send(1,(CTreeUpdate
                        { new_head = 0:[1,0,0,0];
                          extension = [(1, 0:[0,0,0,0], [Command(Read c0, 1)])] }))
              Send(2,(CTreeUpdate
                        { new_head = 0:[1,0,0,0];
                          extension = [(1, 0:[0,0,0,0], [Command(Read c0, 1)])] }))
              Send(3,(CTreeUpdate
                        { new_head = 0:[1,0,0,0];
                          extension = [(1, 0:[0,0,0,0], [Command(Read c0, 1)])] }))
              Broadcast((ConsUpdate
                           { vval = 0:[1,0,0,0]; vterm = 0; term = 0;
                             commit_index = 0:[0,0,0,0] }))] |}] ;
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
      (Recv (CTreeUpdate {new_head= c1_clock; extension= [c1_node]}, 1))
  in
  print t0 actions ;
  [%expect
    {|
    t: { rep =
         { state =
           { vval = 0:[1,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             };
           store =
           { ctree =
             [(0:[0,0,0,0], Root):
              (0:[0,1,0,0], (1, 0:[0,0,0,0], [Command(Read c1, 2)])):
              (0:[1,0,0,0], (1, 0:[0,0,0,0], [Command(Read c0, 1)]))]
             };
           change_flag = false;
           remotes =
           [(1, { expected = <opaque> }): (2, { expected = <opaque> }):
            (3, { expected = <opaque> })]
           };
         other_nodes =
         [(1,
           { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             }):
          (2,
           { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             }):
          (3,
           { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             })];
         failure_detector = {1: 2, 3: 2, 2: 2}; config = <opaque>;
         command_queue = <opaque>; stall_checker = <opaque>; commit_log = [] }
    actions: [] |}] ;
  (* ---- Votes for values ---- *)
  let t0, _ =
    Impl.advance t0
      (Recv
         ( ConsUpdate
             {vval= c0_clock; vterm= 0; term= 0; commit_index= root_clock}
         , 2 ) )
  in
  let t0, actions =
    Impl.advance t0
      (Recv
         ( ConsUpdate
             {vval= c1_clock; vterm= 0; term= 0; commit_index= root_clock}
         , 1 ) )
  in
  (* NOTE does not replicate c1 branch of ctree since it is not in vval *)
  print t0 actions ;
  [%expect
    {|
    t: { rep =
         { state =
           { vval = 0:[1,0,0,0]; vterm = 0; term = 1; commit_index = 0:[0,0,0,0]
             };
           store =
           { ctree =
             [(0:[0,0,0,0], Root):
              (0:[0,1,0,0], (1, 0:[0,0,0,0], [Command(Read c1, 2)])):
              (0:[1,0,0,0], (1, 0:[0,0,0,0], [Command(Read c0, 1)]))]
             };
           change_flag = false;
           remotes =
           [(1, { expected = <opaque> }): (2, { expected = <opaque> }):
            (3, { expected = <opaque> })]
           };
         other_nodes =
         [(1,
           { vval = 0:[0,1,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             }):
          (2,
           { vval = 0:[1,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             }):
          (3,
           { vval = 0:[0,0,0,0]; vterm = 0; term = 0; commit_index = 0:[0,0,0,0]
             })];
         failure_detector = {1: 2, 3: 2, 2: 2}; config = <opaque>;
         command_queue = <opaque>; stall_checker = <opaque>; commit_log = [] }
    actions: [Broadcast((ConsUpdate
                           { vval = 0:[1,0,0,0]; vterm = 0; term = 1;
                             commit_index = 0:[0,0,0,0] }))] |}] ;
  (* conflict term responses *)
  let t0, _ =
    Impl.advance t0
      (Recv
         ( ConsUpdate
             {vval= c0_clock; vterm= 0; term= 1; commit_index= root_clock}
         , 2 ) )
  in
  let t0, _ =
    Impl.advance t0
      (Recv
         ( ConsUpdate
             {vval= c1_clock; vterm= 0; term= 1; commit_index= root_clock}
         , 3 ) )
  in
  let t0, actions =
    Impl.advance t0
      (Recv
         ( ConsUpdate
             {vval= c1_clock; vterm= 0; term= 1; commit_index= root_clock}
         , 1 ) )
  in
  (* ---- Now sufficient conflicts to recover *)
  print t0 actions ;
  [%expect
    {|
    t: { rep =
         { state =
           { vval = 0:[1,0,0,0]; vterm = 1; term = 1; commit_index = 0:[0,0,0,0]
             };
           store =
           { ctree =
             [(0:[0,0,0,0], Root):
              (0:[0,1,0,0], (1, 0:[0,0,0,0], [Command(Read c1, 2)])):
              (0:[1,0,0,0], (1, 0:[0,0,0,0], [Command(Read c0, 1)]))]
             };
           change_flag = false;
           remotes =
           [(1, { expected = <opaque> }): (2, { expected = <opaque> }):
            (3, { expected = <opaque> })]
           };
         other_nodes =
         [(1,
           { vval = 0:[0,1,0,0]; vterm = 0; term = 1; commit_index = 0:[0,0,0,0]
             }):
          (2,
           { vval = 0:[1,0,0,0]; vterm = 0; term = 1; commit_index = 0:[0,0,0,0]
             }):
          (3,
           { vval = 0:[0,1,0,0]; vterm = 0; term = 1; commit_index = 0:[0,0,0,0]
             })];
         failure_detector = {1: 2, 3: 2, 2: 2}; config = <opaque>;
         command_queue = <opaque>; stall_checker = <opaque>; commit_log = [] }
    actions: [Broadcast((ConsUpdate
                           { vval = 0:[1,0,0,0]; vterm = 1; term = 1;
                             commit_index = 0:[0,0,0,0] }))] |}] ;
  ignore (t0, _t1, c0_node, c1_node)

open! Types
open! Utils
open! Actions_f
module Imp = ImperativeActions (Conspire.Types)
module Impl = Conspire.Make (Imp)
open! Conspire.Types
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
  print t [] ;
  [%expect
    {|
    t: config: node_id: 0
               quorum_size: 1
               fd_timeout: 2
               invrs: Ok
               replica_ids: [0]
       failure_detector: state: []
       local_state: commit_index: -1
                    term: 0
                    vterm: 0
                    vval: []
       sent_cache:
       state_cache: []
       command_queue: []
    actions: [] |}] ;
  let c1 = make_command (Read "c1") in
  let t, actions = Impl.advance t (Commands (c1 |> Iter.singleton)) in
  print t actions ;
  [%expect
    {|
    t: config: node_id: 0
               quorum_size: 1
               fd_timeout: 2
               invrs: Ok
               replica_ids: [0]
       failure_detector: state: []
       local_state: commit_index: 0
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache:
       state_cache: []
       command_queue: []
    actions: [CommitCommands(Command(Read c1, 1))] |}] ;
  let c2, c3 = (make_command (Read "c2"), make_command (Read "c3")) in
  let t, actions = Impl.advance t (Commands (Iter.of_list [c2; c3])) in
  print t actions ;
  [%expect
    {|
    t: config: node_id: 0
               quorum_size: 1
               fd_timeout: 2
               invrs: Ok
               replica_ids: [0]
       failure_detector: state: []
       local_state:
        commit_index: 2
        term: 0
        vterm: 0
        vval:
         [[Command(Read c1, 1)], [Command(Read c2, 3)], [Command(Read c3, 2)]]
       sent_cache:
       state_cache: []
       command_queue: []
    actions: [CommitCommands(Command(Read c2, 3), Command(Read c3, 2))] |}]

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
    t: config:
        node_id: 1
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 2); (2: 2); (3: 2)]
       local_state: commit_index: -1
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(2: 0)(3: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(0,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)
              Send(2,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)
              Send(3,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)] |}] ;
  let recv_c1 =
    Recv
      ( { term= 0
        ; vval_seg= {segment_start= 0; segment_entries= [[c1]]}
        ; vterm= 0
        ; commit_index= -1 }
      , 1 )
  in
  let t2, actions = Impl.advance t2 recv_c1 in
  print t2 actions ;
  [%expect
    {|
  t: config:
      node_id: 2
      quorum_size: 3
      fd_timeout: 2
      invrs: Ok
      replica_ids: [0, 1, 2, 3]
     failure_detector: state: [(0: 2); (1: 2); (3: 2)]
     local_state: commit_index: -1
                  term: 0
                  vterm: 0
                  vval: [[Command(Read c1, 1)]]
     sent_cache: (0: 0)(1: 0)(3: 0)
     state_cache:
      [(0: commit_index: -1
           term: 0
           vterm: 0
           vval: [])
       (1: commit_index: -1
           term: 0
           vterm: 0
           vval: [[Command(Read c1, 1)]])
       (3: commit_index: -1
           term: 0
           vterm: 0
           vval: [])]
     command_queue: []
  actions: [Send(0,term: 0
                   commit_index: -1
                   vval: start: 0
                         entries: [[Command(Read c1, 1)]]
                   vterm: 0)
            Send(1,term: 0
                   commit_index: -1
                   vval: start: 0
                         entries: [[Command(Read c1, 1)]]
                   vterm: 0)
            Send(3,term: 0
                   commit_index: -1
                   vval: start: 0
                         entries: [[Command(Read c1, 1)]]
                   vterm: 0)] |}] ;
  let t3, actions = Impl.advance t3 recv_c1 in
  print t3 actions ;
  [%expect
    {|
    t: config:
        node_id: 3
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 2); (1: 2); (2: 2)]
       local_state: commit_index: -1
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(1: 0)(2: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (1: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c1, 1)]])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(0,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)
              Send(1,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)
              Send(2,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)] |}] ;
  let recv i =
    Recv
      ( { term= 0
        ; commit_index= -1
        ; vval_seg= {segment_start= 0; segment_entries= [[c1]]}
        ; vterm= 0 }
      , i )
  in
  let t1, actions = Impl.advance t1 (recv 2) in
  print t1 actions ;
  [%expect
    {|
    t: config:
        node_id: 1
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 2); (2: 2); (3: 2)]
       local_state: commit_index: -1
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(2: 0)(3: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c1, 1)]])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [] |}] ;
  let t1, actions = Impl.advance t1 (recv 3) in
  print t1 actions ;
  [%expect
    {|
    t: config:
        node_id: 1
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 2); (2: 2); (3: 2)]
       local_state: commit_index: 0
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(2: 0)(3: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c1, 1)]])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c1, 1)]])]
       command_queue: []
    actions: [CommitCommands(Command(Read c1, 1))
              Send(0,term: 0
                     commit_index: 0
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(2,term: 0
                     commit_index: 0
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(3,term: 0
                     commit_index: 0
                     vval: start: 1
                           entries: []
                     vterm: 0)] |}]

let%expect_test "e2e conflict re-propose" =
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
    t: config:
        node_id: 1
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 2); (2: 2); (3: 2)]
       local_state: commit_index: -1
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(2: 0)(3: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(0,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)
              Send(2,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)
              Send(3,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)] |}] ;
  let t1, _ = Impl.advance t1 Tick in
  let t1, actions = Impl.advance t1 Tick in
  print t1 actions ;
  [%expect
    {|
    t: config:
        node_id: 1
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 0); (2: 0); (3: 0)]
       local_state: commit_index: -1
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(2: 0)(3: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(0,term: 0
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(2,term: 0
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(3,term: 0
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)] |}] ;
  let c2 = make_command (Read "c2") in
  let t2, actions = Impl.advance t2 (Commands (Iter.of_list [c2])) in
  print t2 actions ;
  [%expect
    {|
    t: config:
        node_id: 2
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 2); (1: 2); (3: 2)]
       local_state: commit_index: -1
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c2, 2)]]
       sent_cache: (0: 0)(1: 0)(3: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (1: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(0,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c2, 2)]]
                     vterm: 0)
              Send(1,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c2, 2)]]
                     vterm: 0)
              Send(3,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c2, 2)]]
                     vterm: 0)] |}] ;
  let recv c i =
    Recv
      ( { term= 0
        ; commit_index= -1
        ; vval_seg= {segment_start= 0; segment_entries= [[c]]}
        ; vterm= 0 }
      , i )
  in
  let t3, actions = Impl.advance t3 (recv c1 1) in
  print t3 actions ;
  [%expect
    {|
    t: config:
        node_id: 3
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 2); (1: 2); (2: 2)]
       local_state: commit_index: -1
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(1: 0)(2: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (1: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c1, 1)]])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(0,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)
              Send(1,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)
              Send(2,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)] |}] ;
  (* Conflict from t1 *)
  let t2, actions = Impl.advance t2 (recv c1 1) in
  print t2 actions ;
  [%expect
    {|
    t: config:
        node_id: 2
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 2); (1: 2); (3: 2)]
       local_state: commit_index: -1
                    term: 1
                    vterm: 0
                    vval: [[Command(Read c2, 2)]]
       sent_cache: (0: 0)(1: 0)(3: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (1: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c1, 1)]])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(0,term: 1
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(1,term: 1
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(3,term: 1
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)] |}] ;
  (* Conflict from t2 *)
  let t1, actions = Impl.advance t1 (recv c2 2) in
  print t1 actions ;
  [%expect
    {|
    t: config:
        node_id: 1
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 0); (2: 2); (3: 0)]
       local_state: commit_index: -1
                    term: 1
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(2: 0)(3: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c2, 2)]])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(0,term: 1
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(2,term: 1
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(3,term: 1
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)] |}] ;
  let t3, actions = Impl.advance t3 (recv c2 2) in
  print t3 actions ;
  [%expect
    {|
    t: config:
        node_id: 3
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 2); (1: 2); (2: 2)]
       local_state: commit_index: -1
                    term: 1
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(1: 0)(2: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (1: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c1, 1)]])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c2, 2)]])]
       command_queue: []
    actions: [Send(0,term: 1
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(1,term: 1
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(2,term: 1
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)] |}] ;
  (* Conflict result *)
  let t1, actions =
    Impl.advance t1
      (Recv
         ( { term= 1
           ; vterm= 0
           ; vval_seg= {segment_start= 0; segment_entries= [[c1]]}
           ; commit_index= -1 }
         , 3 ) )
  in
  print t1 actions ;
  [%expect
    {|
    t: config:
        node_id: 1
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 0); (2: 2); (3: 2)]
       local_state: commit_index: -1
                    term: 1
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(2: 0)(3: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c2, 2)]])
         (3: commit_index: -1
             term: 1
             vterm: 0
             vval: [[Command(Read c1, 1)]])]
       command_queue: []
    actions: [] |}] ;
  let t1, actions =
    Impl.advance t1
      (Recv
         ( { term= 1
           ; vterm= 0
           ; vval_seg= {segment_start= 0; segment_entries= [[c2]]}
           ; commit_index= -1 }
         , 2 ) )
  in
  print t1 actions ;
  [%expect
    {|
    t: config:
        node_id: 1
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 0); (2: 2); (3: 2)]
       local_state: commit_index: -1
                    term: 1
                    vterm: 1
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(2: 0)(3: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (2: commit_index: -1
             term: 1
             vterm: 0
             vval: [[Command(Read c2, 2)]])
         (3: commit_index: -1
             term: 1
             vterm: 0
             vval: [[Command(Read c1, 1)]])]
       command_queue: []
    actions: [] |}] ;
  ignore (t1, t2, t3)

let%expect_test "commit other" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t1 = create (c4 1) in
  let c1 = make_command (Read "c1") in
  let t1, _ = Impl.advance t1 (Commands (Iter.of_list [c1])) in
  let t1, _ = Impl.advance t1 Tick in
  let t1, actions = Impl.advance t1 Tick in
  print t1 actions ;
  [%expect
    {|
    t: config:
        node_id: 1
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 0); (2: 0); (3: 0)]
       local_state: commit_index: -1
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(2: 0)(3: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(0,term: 0
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(2,term: 0
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(3,term: 0
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)] |}] ;
  let c2 = make_command (Read "c2") in
  let t1, actions =
    Impl.advance t1
      (Recv
         ( { commit_index= -1
           ; term= 0
           ; vterm= 0
           ; vval_seg= {segment_start= 0; segment_entries= [[c2]]} }
         , 0 ) )
  in
  print t1 actions ;
  [%expect
    {|
    t: config:
        node_id: 1
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 2); (2: 0); (3: 0)]
       local_state: commit_index: -1
                    term: 1
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(2: 0)(3: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c2, 2)]])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(0,term: 1
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(2,term: 1
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(3,term: 1
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)] |}] ;
  let t1, actions =
    Impl.advance t1
      (Recv
         ( { commit_index= -1
           ; term= 0
           ; vterm= 0
           ; vval_seg= {segment_start= 0; segment_entries= [[c2]]} }
         , 2 ) )
  in
  print t1 actions ;
  [%expect
    {|
    t: config:
        node_id: 1
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 2); (2: 2); (3: 0)]
       local_state: commit_index: -1
                    term: 1
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(2: 0)(3: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c2, 2)]])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c2, 2)]])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [] |}] ;
  let t1, actions =
    Impl.advance t1
      (Recv
         ( { commit_index= -1
           ; term= 0
           ; vterm= 0
           ; vval_seg= {segment_start= 0; segment_entries= [[c2]]} }
         , 3 ) )
  in
  print t1 actions ;
  [%expect
    {|
    t: config:
        node_id: 1
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 2); (2: 2); (3: 2)]
       local_state: commit_index: 0
                    term: 1
                    vterm: 0
                    vval: [[Command(Read c2, 2)]]
       sent_cache: (0: 0)(2: 0)(3: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c2, 2)]])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c2, 2)]])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c2, 2)]])]
       command_queue: []
    actions: [CommitCommands(Command(Read c2, 2))
              Send(0,term: 1
                     commit_index: 0
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(2,term: 1
                     commit_index: 0
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(3,term: 1
                     commit_index: 0
                     vval: start: 1
                           entries: []
                     vterm: 0)] |}] ;
  ignore t1

let%expect_test "commit force remote" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t0 = create (c4 0) in
  let c1 = make_command (Read "c1") in
  let t0, actions = Impl.advance t0 (Commands (Iter.of_list [c1])) in
  print t0 actions ;
  [%expect
    {|
    t: config:
        node_id: 0
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(1: 2); (2: 2); (3: 2)]
       local_state: commit_index: -1
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (1: 0)(2: 0)(3: 0)
       state_cache:
        [(1: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(1,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)
              Send(2,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)
              Send(3,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)] |}] ;
  let c2 = make_command (Read "c2") in
  let t0, actions =
    Impl.advance t0
      (Recv
         ( { commit_index= 0
           ; term= 0
           ; vterm= 0
           ; vval_seg= {segment_start= 0; segment_entries= [[c2]]} }
         , 1 ) )
  in
  print t0 actions ;
  [%expect
    {|
    t: config:
        node_id: 0
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(1: 2); (2: 2); (3: 2)]
       local_state: commit_index: 0
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c2, 2)]]
       sent_cache: (1: 0)(2: 0)(3: 0)
       state_cache:
        [(1: commit_index: 0
             term: 0
             vterm: 0
             vval: [[Command(Read c2, 2)]])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [CommitCommands(Command(Read c2, 2))
              Send(1,term: 0
                     commit_index: 0
                     vval: start: 0
                           entries: [[Command(Read c2, 2)]]
                     vterm: 0)
              Send(2,term: 0
                     commit_index: 0
                     vval: start: 0
                           entries: [[Command(Read c2, 2)]]
                     vterm: 0)
              Send(3,term: 0
                     commit_index: 0
                     vval: start: 0
                           entries: [[Command(Read c2, 2)]]
                     vterm: 0)] |}] ;
  ignore t0

let%expect_test "conflict merge recovery" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t0 = create (c4 0) in
  let c1 = make_command (Read "c1") in
  let c2 = make_command (Read "c2") in
  let c3 = make_command (Read "c3") in
  let t0, actions = Impl.advance t0 (Commands (Iter.of_list [c1])) in
  print t0 actions ;
  [%expect
    {|
    t: config:
        node_id: 0
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(1: 2); (2: 2); (3: 2)]
       local_state: commit_index: -1
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (1: 0)(2: 0)(3: 0)
       state_cache:
        [(1: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(1,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)
              Send(2,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)
              Send(3,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)] |}] ;
  let t0, actions =
    Impl.advance t0
      (Recv
         ( { commit_index= -1
           ; term= 1
           ; vterm= 0
           ; vval_seg= {segment_start= 0; segment_entries= [[c2]]} }
         , 1 ) )
  in
  print t0 actions ;
  [%expect
    {|
    t: config:
        node_id: 0
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(1: 2); (2: 2); (3: 2)]
       local_state: commit_index: -1
                    term: 1
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (1: 0)(2: 0)(3: 0)
       state_cache:
        [(1: commit_index: -1
             term: 1
             vterm: 0
             vval: [[Command(Read c2, 2)]])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(1,term: 1
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(2,term: 1
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(3,term: 1
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)] |}] ;
  let t0, actions =
    Impl.advance t0
      (Recv
         ( { commit_index= -1
           ; term= 1
           ; vterm= 0
           ; vval_seg= {segment_start= 0; segment_entries= [[c3]]} }
         , 2 ) )
  in
  print t0 actions ;
  [%expect
    {|
    t: config:
        node_id: 0
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(1: 2); (2: 2); (3: 2)]
       local_state: commit_index: -1
                    term: 1
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (1: 0)(2: 0)(3: 0)
       state_cache:
        [(1: commit_index: -1
             term: 1
             vterm: 0
             vval: [[Command(Read c2, 2)]])
         (2: commit_index: -1
             term: 1
             vterm: 0
             vval: [[Command(Read c3, 3)]])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [] |}] ;
  let t0, actions =
    Impl.advance t0
      (Recv
         ( { commit_index= -1
           ; term= 1
           ; vterm= 0
           ; vval_seg= {segment_start= 0; segment_entries= [[c2; c3]]} }
         , 3 ) )
  in
  print t0 actions ;
  [%expect
    {|
    t: config:
        node_id: 0
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(1: 2); (2: 2); (3: 2)]
       local_state:
        commit_index: -1
        term: 1
        vterm: 1
        vval: [[Command(Read c1, 1), Command(Read c2, 2), Command(Read c3, 3)]]
       sent_cache: (1: 0)(2: 0)(3: 0)
       state_cache:
        [(1: commit_index: -1
             term: 1
             vterm: 0
             vval: [[Command(Read c2, 2)]])
         (2: commit_index: -1
             term: 1
             vterm: 0
             vval: [[Command(Read c3, 3)]])
         (3:
          commit_index: -1
          term: 1
          vterm: 0
          vval: [[Command(Read c2, 2), Command(Read c3, 3)]])]
       command_queue: []
    actions: [Send(1,term: 1
                     commit_index: -1
                     vval:
                      start: 0
                      entries:
                       [[Command(Read c1, 1), Command(Read c2, 2),
                         Command(Read c3, 3)]]
                     vterm: 1)
              Send(2,term: 1
                     commit_index: -1
                     vval:
                      start: 0
                      entries:
                       [[Command(Read c1, 1), Command(Read c2, 2),
                         Command(Read c3, 3)]]
                     vterm: 1)
              Send(3,term: 1
                     commit_index: -1
                     vval:
                      start: 0
                      entries:
                       [[Command(Read c1, 1), Command(Read c2, 2),
                         Command(Read c3, 3)]]
                     vterm: 1)] |}]

let%expect_test "conflict o4&merge" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t0 = create (c4 0) in
  let c1 = make_command (Read "c1") in
  let c2 = make_command (Read "c2") in
  let c3 = make_command (Read "c3") in
  let c4 = make_command (Read "c4") in
  let c5 = make_command (Read "c5") in
  let c6 = make_command (Read "c6") in
  let t0, _ = Impl.advance t0 Tick in
  let t0, _ = Impl.advance t0 Tick in
  let t0, actions = Impl.advance t0 (Commands (Iter.of_list [c1; c2; c5])) in
  print t0 actions ;
  [%expect
    {|
    t: config:
        node_id: 0
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(1: 0); (2: 0); (3: 0)]
       local_state:
        commit_index: -1
        term: 0
        vterm: 0
        vval:
         [[Command(Read c1, 1)], [Command(Read c2, 2)], [Command(Read c5, 5)]]
       sent_cache: (1: 2)(2: 2)(3: 2)
       state_cache:
        [(1: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(1,term: 0
                     commit_index: -1
                     vval:
                      start: 0
                      entries:
                       [[Command(Read c1, 1)]; [Command(Read c2, 2)];
                        [Command(Read c5, 5)]]
                     vterm: 0)
              Send(2,term: 0
                     commit_index: -1
                     vval:
                      start: 0
                      entries:
                       [[Command(Read c1, 1)]; [Command(Read c2, 2)];
                        [Command(Read c5, 5)]]
                     vterm: 0)
              Send(3,term: 0
                     commit_index: -1
                     vval:
                      start: 0
                      entries:
                       [[Command(Read c1, 1)]; [Command(Read c2, 2)];
                        [Command(Read c5, 5)]]
                     vterm: 0)] |}] ;
  let t0, actions =
    Impl.advance t0
      (Recv
         ( { commit_index= -1
           ; term= 1
           ; vterm= 0
           ; vval_seg= {segment_start= 0; segment_entries= [[c1]; [c3]; [c5]]}
           }
         , 1 ) )
  in
  print t0 actions ;
  [%expect
    {|
    t: config:
        node_id: 0
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(1: 2); (2: 0); (3: 0)]
       local_state:
        commit_index: -1
        term: 1
        vterm: 0
        vval:
         [[Command(Read c1, 1)], [Command(Read c2, 2)], [Command(Read c5, 5)]]
       sent_cache: (1: 2)(2: 2)(3: 2)
       state_cache:
        [(1:
          commit_index: -1
          term: 1
          vterm: 0
          vval:
           [[Command(Read c1, 1)], [Command(Read c3, 3)], [Command(Read c5, 5)]])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(1,term: 1
                     commit_index: -1
                     vval: start: 3
                           entries: []
                     vterm: 0)
              Send(2,term: 1
                     commit_index: -1
                     vval: start: 3
                           entries: []
                     vterm: 0)
              Send(3,term: 1
                     commit_index: -1
                     vval: start: 3
                           entries: []
                     vterm: 0)] |}] ;
  let t0, actions = Impl.advance t0 (Commands (Iter.of_list [c6])) in
  print t0 actions ;
  [%expect
    {|
    t: config:
        node_id: 0
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(1: 2); (2: 0); (3: 0)]
       local_state:
        commit_index: -1
        term: 1
        vterm: 0
        vval:
         [[Command(Read c1, 1)], [Command(Read c2, 2)], [Command(Read c5, 5)]]
       sent_cache: (1: 2)(2: 2)(3: 2)
       state_cache:
        [(1:
          commit_index: -1
          term: 1
          vterm: 0
          vval:
           [[Command(Read c1, 1)], [Command(Read c3, 3)], [Command(Read c5, 5)]])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: [Command(Read c6, 6)]
    actions: [] |}] ;
  ignore c6 ;
  let t0, actions =
    Impl.advance t0
      (Recv
         ( { commit_index= -1
           ; term= 1
           ; vterm= 0
           ; vval_seg= {segment_start= 0; segment_entries= [[c2]; [c4]; [c4]]}
           }
         , 2 ) )
  in
  print t0 actions ;
  [%expect
    {|
    t: config:
        node_id: 0
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(1: 2); (2: 2); (3: 0)]
       local_state:
        commit_index: -1
        term: 1
        vterm: 1
        vval:
         [[Command(Read c1, 1)],
          [Command(Read c2, 2), Command(Read c3, 3), Command(Read c4, 4)],
          [Command(Read c4, 4), Command(Read c5, 5)], [Command(Read c6, 6)]]
       sent_cache: (1: 3)(2: 3)(3: 3)
       state_cache:
        [(1:
          commit_index: -1
          term: 1
          vterm: 0
          vval:
           [[Command(Read c1, 1)], [Command(Read c3, 3)], [Command(Read c5, 5)]])
         (2:
          commit_index: -1
          term: 1
          vterm: 0
          vval:
           [[Command(Read c2, 2)], [Command(Read c4, 4)], [Command(Read c4, 4)]])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(1,term: 1
                     commit_index: -1
                     vval:
                      start: 1
                      entries:
                       [[Command(Read c2, 2), Command(Read c3, 3),
                         Command(Read c4, 4)];
                        [Command(Read c4, 4), Command(Read c5, 5)];
                        [Command(Read c6, 6)]]
                     vterm: 1)
              Send(2,term: 1
                     commit_index: -1
                     vval:
                      start: 1
                      entries:
                       [[Command(Read c2, 2), Command(Read c3, 3),
                         Command(Read c4, 4)];
                        [Command(Read c4, 4), Command(Read c5, 5)];
                        [Command(Read c6, 6)]]
                     vterm: 1)
              Send(3,term: 1
                     commit_index: -1
                     vval:
                      start: 1
                      entries:
                       [[Command(Read c2, 2), Command(Read c3, 3),
                         Command(Read c4, 4)];
                        [Command(Read c4, 4), Command(Read c5, 5)];
                        [Command(Read c6, 6)]]
                     vterm: 1)] |}] ;
  ignore t0

let%expect_test "conflict o4&merge" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t0 = create (c4 0) in
  let c1 = make_command (Read "c1") in
  let c2 = make_command (Read "c2") in
  let c3 = make_command (Read "c3") in
  let c4 = make_command (Read "c4") in
  let t0, _ = Impl.advance t0 Tick in
  let t0, _ = Impl.advance t0 Tick in
  let t0, actions = Impl.advance t0 (Commands (Iter.of_list [c1; c4])) in
  print t0 actions ;
  [%expect
    {|
    t: config:
        node_id: 0
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(1: 0); (2: 0); (3: 0)]
       local_state:
        commit_index: -1
        term: 0
        vterm: 0
        vval: [[Command(Read c1, 1)], [Command(Read c4, 4)]]
       sent_cache: (1: 1)(2: 1)(3: 1)
       state_cache:
        [(1: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(1,term: 0
                     commit_index: -1
                     vval:
                      start: 0
                      entries: [[Command(Read c1, 1)]; [Command(Read c4, 4)]]
                     vterm: 0)
              Send(2,term: 0
                     commit_index: -1
                     vval:
                      start: 0
                      entries: [[Command(Read c1, 1)]; [Command(Read c4, 4)]]
                     vterm: 0)
              Send(3,term: 0
                     commit_index: -1
                     vval:
                      start: 0
                      entries: [[Command(Read c1, 1)]; [Command(Read c4, 4)]]
                     vterm: 0)] |}] ;
  let t0, actions =
    Impl.advance t0
      (Recv
         ( { commit_index= -1
           ; term= 1
           ; vterm= 1
           ; vval_seg= {segment_start= 0; segment_entries= [[c1]; [c2]; [c3]]}
           }
         , 1 ) )
  in
  print t0 actions ;
  [%expect
    {|
    t: config:
        node_id: 0
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(1: 2); (2: 0); (3: 0)]
       local_state:
        commit_index: -1
        term: 1
        vterm: 1
        vval:
         [[Command(Read c1, 1)], [Command(Read c2, 2)], [Command(Read c3, 3)]]
       sent_cache: (1: 2)(2: 2)(3: 2)
       state_cache:
        [(1:
          commit_index: -1
          term: 1
          vterm: 1
          vval:
           [[Command(Read c1, 1)], [Command(Read c2, 2)], [Command(Read c3, 3)]])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(1,term: 1
                     commit_index: -1
                     vval:
                      start: 1
                      entries: [[Command(Read c2, 2)]; [Command(Read c3, 3)]]
                     vterm: 1)
              Send(2,term: 1
                     commit_index: -1
                     vval:
                      start: 1
                      entries: [[Command(Read c2, 2)]; [Command(Read c3, 3)]]
                     vterm: 1)
              Send(3,term: 1
                     commit_index: -1
                     vval:
                      start: 1
                      entries: [[Command(Read c2, 2)]; [Command(Read c3, 3)]]
                     vterm: 1)] |}]
(* TODO finish this test *)

let%expect_test "4th vote bug" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t0 = create (c4 0) in
  let c1 = make_command (Read "c1") in
  let t0, actions = Impl.advance t0 (Commands (Iter.of_list [c1])) in
  print t0 actions ;
  [%expect
    {|
    t: config:
        node_id: 0
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(1: 2); (2: 2); (3: 2)]
       local_state: commit_index: -1
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (1: 0)(2: 0)(3: 0)
       state_cache:
        [(1: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(1,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)
              Send(2,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)
              Send(3,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)] |}] ;
  let t0, actions =
    Impl.advance t0
      (Recv
         ( { commit_index= -1
           ; term= 0
           ; vterm= 0
           ; vval_seg= {segment_start= 0; segment_entries= [[c1]]} }
         , 1 ) )
  in
  print t0 actions ;
  [%expect
    {|
    t: config:
        node_id: 0
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(1: 2); (2: 2); (3: 2)]
       local_state: commit_index: -1
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (1: 0)(2: 0)(3: 0)
       state_cache:
        [(1: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c1, 1)]])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [] |}] ;
  let t0, actions =
    Impl.advance t0
      (Recv
         ( { commit_index= -1
           ; term= 0
           ; vterm= 0
           ; vval_seg= {segment_start= 0; segment_entries= [[c1]]} }
         , 2 ) )
  in
  print t0 actions ;
  [%expect
    {|
    t: config:
        node_id: 0
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(1: 2); (2: 2); (3: 2)]
       local_state: commit_index: 0
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (1: 0)(2: 0)(3: 0)
       state_cache:
        [(1: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c1, 1)]])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c1, 1)]])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [CommitCommands(Command(Read c1, 1))
              Send(1,term: 0
                     commit_index: 0
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(2,term: 0
                     commit_index: 0
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(3,term: 0
                     commit_index: 0
                     vval: start: 1
                           entries: []
                     vterm: 0)] |}] ;
  let t0, actions =
    Impl.advance t0
      (Recv
         ( { commit_index= -1
           ; term= 0
           ; vterm= 0
           ; vval_seg= {segment_start= 0; segment_entries= [[c1]]} }
         , 3 ) )
  in
  print t0 actions ;
  [%expect
    {|
    t: config:
        node_id: 0
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(1: 2); (2: 2); (3: 2)]
       local_state: commit_index: 0
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (1: 0)(2: 0)(3: 0)
       state_cache:
        [(1: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c1, 1)]])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c1, 1)]])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c1, 1)]])]
       command_queue: []
    actions: [] |}]

let%expect_test "Commit acceptor_increment race condition" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let t0 = create (c4 0) in
  let t1 = create (c4 1) in
  let c0, c1 = (make_command (Read "c0"), make_command (Read "c1")) in
  let t0, actions = Impl.advance t0 (Commands (Iter.singleton c0)) in
  print t0 actions ;
  [%expect
    {|
    t: config:
        node_id: 0
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(1: 2); (2: 2); (3: 2)]
       local_state: commit_index: -1
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c0, 2)]]
       sent_cache: (1: 0)(2: 0)(3: 0)
       state_cache:
        [(1: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(1,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c0, 2)]]
                     vterm: 0)
              Send(2,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c0, 2)]]
                     vterm: 0)
              Send(3,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c0, 2)]]
                     vterm: 0)] |}] ;
  let t1, actions = Impl.advance t1 (Commands (Iter.singleton c1)) in
  print t1 actions ;
  [%expect
    {|
    t: config:
        node_id: 1
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 2); (2: 2); (3: 2)]
       local_state: commit_index: -1
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(2: 0)(3: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(0,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)
              Send(2,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)
              Send(3,term: 0
                     commit_index: -1
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)] |}] ;
  (* Vote for c1 from 2 *)
  let t1, actions =
    Impl.advance t1
      (Recv
         ( { commit_index= -1
           ; term= 0
           ; vterm= 0
           ; vval_seg= {segment_start= 0; segment_entries= [[c1]]} }
         , 2 ) )
  in
  print t1 actions ;
  [%expect
    {|
    t: config:
        node_id: 1
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 2); (2: 2); (3: 2)]
       local_state: commit_index: -1
                    term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(2: 0)(3: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c1, 1)]])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [] |}] ;
  (* conflict from t0 *)
  let t1, actions =
    Impl.advance t1
      (Recv
         ( { commit_index= -1
           ; term= 0
           ; vterm= 0
           ; vval_seg= {segment_start= 0; segment_entries= [[c0]]} }
         , 0 ) )
  in
  print t1 actions ;
  [%expect
    {|
    t: config:
        node_id: 1
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 2); (2: 2); (3: 2)]
       local_state: commit_index: -1
                    term: 1
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(2: 0)(3: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c0, 2)]])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c1, 1)]])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [])]
       command_queue: []
    actions: [Send(0,term: 1
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(2,term: 1
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(3,term: 1
                     commit_index: -1
                     vval: start: 1
                           entries: []
                     vterm: 0)] |}] ;
  (* Vote for c1 from 3 *)
  let t1, actions =
    Impl.advance t1
      (Recv
         ( { commit_index= -1
           ; term= 0
           ; vterm= 0
           ; vval_seg= {segment_start= 0; segment_entries= [[c1]]} }
         , 3 ) )
  in
  (* Commit c1, but be on term 1 from conflict from t0 *)
  print t1 actions ;
  [%expect
    {|
    t: config:
        node_id: 1
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(0: 2); (2: 2); (3: 2)]
       local_state: commit_index: 0
                    term: 1
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(2: 0)(3: 0)
       state_cache:
        [(0: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c0, 2)]])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c1, 1)]])
         (3: commit_index: -1
             term: 0
             vterm: 0
             vval: [[Command(Read c1, 1)]])]
       command_queue: []
    actions: [CommitCommands(Command(Read c1, 1))
              Send(0,term: 1
                     commit_index: 0
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(2,term: 1
                     commit_index: 0
                     vval: start: 1
                           entries: []
                     vterm: 0)
              Send(3,term: 1
                     commit_index: 0
                     vval: start: 1
                           entries: []
                     vterm: 0)] |}] ;
  (* TODO check that this cannot occur *)
  let t0, actions =
    Impl.advance t0
      (Recv
         ( { commit_index= 0
           ; term= 1
           ; vterm= 0
           ; vval_seg= {segment_start= 0; segment_entries= [[c1]]} }
         , 3 ) )
  in
  print t0 actions ; [%expect {|
    t: config:
        node_id: 0
        quorum_size: 3
        fd_timeout: 2
        invrs: Ok
        replica_ids: [0, 1, 2, 3]
       failure_detector: state: [(1: 2); (2: 2); (3: 2)]
       local_state: commit_index: 0
                    term: 1
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (1: 0)(2: 0)(3: 0)
       state_cache:
        [(1: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (2: commit_index: -1
             term: 0
             vterm: 0
             vval: [])
         (3: commit_index: 0
             term: 1
             vterm: 0
             vval: [[Command(Read c1, 1)]])]
       command_queue: []
    actions: [CommitCommands(Command(Read c1, 1))
              Send(1,term: 1
                     commit_index: 0
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)
              Send(2,term: 1
                     commit_index: 0
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)
              Send(3,term: 1
                     commit_index: 0
                     vval: start: 0
                           entries: [[Command(Read c1, 1)]]
                     vterm: 0)] |}]

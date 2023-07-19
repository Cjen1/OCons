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
  print t [];
  [%expect{|
    t: config:
        node_id: 0
        quorum_size: 1
        fd_timeout: 2
        invrs: violated: [replica_count correct]
        replica_ids: [0]
       commit_index: -1
       failure_detector: state: []
       local_state: term: 0
                    vterm: 0
                    vval: []
       sent_cache:
       state_cache: []
    actions: [] |}];
  let c1 = make_command (Read "c1") in
  let t, actions = Impl.advance t (Commands (c1 |> Iter.singleton)) in
  print t actions ;
  [%expect{|
    diverge: None
    t: config:
        node_id: 0
        quorum_size: 1
        fd_timeout: 2
        invrs: violated: [replica_count correct]
        replica_ids: [0]
       commit_index: 0
       failure_detector: state: []
       local_state: term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache:
       state_cache: []
    actions: [CommitCommands(Command(Read c1, 1))] |}];
  let c2, c3 = (make_command (Read "c2"), make_command (Read "c3")) in
  let t, actions = Impl.advance t (Commands (Iter.of_list [c2; c3])) in
  print t actions ;
  [%expect{|
    diverge: None
    t: config:
        node_id: 0
        quorum_size: 1
        fd_timeout: 2
        invrs: violated: [replica_count correct]
        replica_ids: [0]
       commit_index: 2
       failure_detector: state: []
       local_state:
        term: 0
        vterm: 0
        vval:
         [[Command(Read c1, 1)], [Command(Read c2, 3)], [Command(Read c3, 2)]]
       sent_cache:
       state_cache: []
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
  [%expect{|
    diverge: None
    t: config:
        node_id: 1
        quorum_size: 3
        fd_timeout: 2
        invrs: violated: [replica_count correct]
        replica_ids: [0, 1, 2, 3]
       commit_index: -1
       failure_detector: state: [(0: 2); (2: 2); (3: 2)]
       local_state: term: 0
                    vterm: 0
                    vval: [[Command(Read c1, 1)]]
       sent_cache: (0: 0)(2: 0)(3: 0)
       state_cache:
        [(0: term: 0
             vterm: 0
             vval: [])
         (2: term: 0
             vterm: 0
             vval: [])
         (3: term: 0
             vterm: 0
             vval: [])]
    actions: [] |}](*;
  let recv_c1 = Recv (Sync (0, {term= 0; value= [c1]}), 1) in
  let t2, actions = Impl.advance t2 recv_c1 in
  print t2 actions ;
  [%expect{||}];
  let t3, actions = Impl.advance t3 recv_c1 in
  print t3 actions ;
  [%expect{||}];
  let recv i = Recv (SyncResp (0,{ term= 0; vterm= 0; vvalue= [c1]}), i) in
  let t1, actions = Impl.advance t1 (recv 2) in
  print t1 actions ;
  [%expect{||}];
  let t1, actions = Impl.advance t1 (recv 3) in
  print t1 actions ;
  [%expect
  [%expect{||}];
  *);
  ignore (t1,t2,t3);

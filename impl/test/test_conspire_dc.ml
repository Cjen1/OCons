open! Core
module Utils = Impl_core.Utils
module DC = Impl_core__Conspire_dc
module Imp = Impl_core__Actions_f.ImperativeActions (DC.Types)
module Impl = DC.Make (Imp)
module Rep = DC.Conspire.Rep
open Impl
open Impl_core__Types
open DC.Types

let action_pp = Ocons_core.Consensus_intf.action_pp ~pp_msg:pp_message

let interval = DC.Time.Span.of_sec 1.

let c1 clk =
  make_config ~node_id:0 ~replica_ids:[0] ~delay_interval:interval
    ~batching_interval:interval clk ~tick_limit:100

let c4 node_id clk =
  make_config ~node_id ~replica_ids:[0; 1; 2; 3] ~delay_interval:interval
    ~batching_interval:interval clk ~tick_limit:100

let print t acts =
  Fmt.pr "t: @[<v>%a@]@." PP.t_pp t ;
  Fmt.pr "actions: @[%a@]@." Fmt.(brackets @@ list action_pp) acts

let make_clock () =
  let clock = Eio_mock.Clock.make () in
  let advance f =
    let now = Eio.Time.now clock in
    Eio_mock.Clock.set_time clock (now +. f)
  in
  ((clock :> float Eio.Time.clock_ty Eio.Time.clock), advance)

let%expect_test "local_commit" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let c0, ac0 = make_clock () in
  let t = create (c1 c0) in
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
         command_buffer =
         { store = []; hwm = 0.00000; interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 0; limit = 100 };
         clock = <opaque> }
    actions: [] |}] ;
  let c0 = make_command (Read "c0") in
  let t, actions = Impl.advance t (Commands (c0 |> Iter.singleton)) in
  print t actions ;
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
         command_buffer =
         { store = [(1.00000: [Command(Read c0, 1)])]; hwm = 0.00000;
           interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 0; limit = 100 };
         clock = <opaque> }
    actions: [Broadcast((Conspire_dc.Types.Commands
                           ([Command(Read c0, 1)], 1.00000)))] |}] ;
  ac0 1. ;
  [%expect {| +mock time is now 1 |}] ;
  let t, actions = Impl.advance t Tick in
  print t actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 34e452ffd92c22ea72ef125a33a0a593; vterm = 0; term = 0;
               commit_index = 34e452ffd92c22ea72ef125a33a0a593 };
             store =
             { ctree =
               [(34e452ffd92c22ea72ef125a33a0a593:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e,
                    ([Command(Read c0, 1)], 1.00000));
                   parent = <opaque>; key = 34e452ffd92c22ea72ef125a33a0a593 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state = []; config = <opaque>;
           commit_log = [([Command(Read c0, 1)], 1.00000)] };
         command_buffer =
         { store = []; hwm = 1.00000; interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 1; limit = 100 };
         clock = <opaque> }
    actions: [CommitCommands(Command(Read c0, 1))] |}] ;
  let c1 = make_command (Read "c1") in
  let c2 = make_command (Read "c2") in
  let t, actions = Impl.advance t (Commands (Iter.of_list [c1; c2])) in
  print t actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 34e452ffd92c22ea72ef125a33a0a593; vterm = 0; term = 0;
               commit_index = 34e452ffd92c22ea72ef125a33a0a593 };
             store =
             { ctree =
               [(34e452ffd92c22ea72ef125a33a0a593:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e,
                    ([Command(Read c0, 1)], 1.00000));
                   parent = <opaque>; key = 34e452ffd92c22ea72ef125a33a0a593 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state = []; config = <opaque>;
           commit_log = [([Command(Read c0, 1)], 1.00000)] };
         command_buffer =
         { store = [(2.00000: [Command(Read c2, 3); Command(Read c1, 2)])];
           hwm = 1.00000; interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 1; limit = 100 };
         clock = <opaque> }
    actions: [Broadcast((Conspire_dc.Types.Commands
                           ([Command(Read c1, 2); Command(Read c2, 3)], 2.00000)))] |}] ;
  ac0 1. ;
  [%expect {| +mock time is now 2 |}] ;
  let t, actions = Impl.advance t Tick in
  print t actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = ccfc0ddc71cfcede53a100a51835b7ff; vterm = 0; term = 0;
               commit_index = ccfc0ddc71cfcede53a100a51835b7ff };
             store =
             { ctree =
               [(34e452ffd92c22ea72ef125a33a0a593:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e,
                    ([Command(Read c0, 1)], 1.00000));
                   parent = <opaque>; key = 34e452ffd92c22ea72ef125a33a0a593 });
                (ccfc0ddc71cfcede53a100a51835b7ff:
                 { node =
                   (2, 34e452ffd92c22ea72ef125a33a0a593,
                    ([Command(Read c1, 2); Command(Read c2, 3)], 2.00000));
                   parent = <opaque>; key = ccfc0ddc71cfcede53a100a51835b7ff });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state = []; config = <opaque>;
           commit_log =
           [([Command(Read c0, 1)], 1.00000)
            ([Command(Read c1, 2); Command(Read c2, 3)], 2.00000)]
           };
         command_buffer =
         { store = [(2.00000: [Command(Read c1, 2)])]; hwm = 2.00000;
           interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 2; limit = 100 };
         clock = <opaque> }
    actions: [CommitCommands(Command(Read c1, 2), Command(Read c2, 3))] |}]

let%expect_test "e2e commit" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let clk0, ac0 = make_clock () in
  let t0 = create (c4 0 clk0) in
  let clk1, ac1 = make_clock () in
  let t1 = create (c4 1 clk1) in
  let c1 = make_command (Read "c1") in
  let t0, actions = Impl.advance t0 (Commands (Iter.of_list [c1])) in
  print t0 actions ;
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
         command_buffer =
         { store = [(1.00000: [Command(Read c1, 1)])]; hwm = 0.00000;
           interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 0; limit = 100 };
         clock = <opaque> }
    actions: [Broadcast((Conspire_dc.Types.Commands
                           ([Command(Read c1, 1)], 1.00000)))] |}] ;
  let t1, actions =
    Impl.advance t1 (Recv (Commands ([c1], Utils.float_to_time 1.), 0))
  in
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
         command_buffer =
         { store = [(1.00000: [Command(Read c1, 1)])]; hwm = 0.00000;
           interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 0; limit = 100 };
         clock = <opaque> }
    actions: [] |}] ;
  ac0 1. ;
  ac1 1. ;
  [%expect {|
    +mock time is now 1
    +mock time is now 1 |}] ;
  let t0, actions = Impl.advance t0 Tick in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = c4677fbe3fa92d4246f849e282f15527; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(c4677fbe3fa92d4246f849e282f15527:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e,
                    ([Command(Read c1, 1)], 1.00000));
                   parent = <opaque>; key = c4677fbe3fa92d4246f849e282f15527 });
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
         command_buffer =
         { store = []; hwm = 1.00000; interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 1; limit = 100 };
         clock = <opaque> }
    actions: [Send(1,(Conspire_dc.Types.Conspire
                        { ctree =
                          (Some { new_head = c4677fbe3fa92d4246f849e282f15527;
                                  extension =
                                  [(1, d41d8cd98f00b204e9800998ecf8427e,
                                    ([Command(Read c1, 1)], 1.00000))]
                                  });
                          cons =
                          (Some { vval = c4677fbe3fa92d4246f849e282f15527;
                                  vterm = 0; term = 0;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))
              Send(2,(Conspire_dc.Types.Conspire
                        { ctree =
                          (Some { new_head = c4677fbe3fa92d4246f849e282f15527;
                                  extension =
                                  [(1, d41d8cd98f00b204e9800998ecf8427e,
                                    ([Command(Read c1, 1)], 1.00000))]
                                  });
                          cons =
                          (Some { vval = c4677fbe3fa92d4246f849e282f15527;
                                  vterm = 0; term = 0;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))
              Send(3,(Conspire_dc.Types.Conspire
                        { ctree =
                          (Some { new_head = c4677fbe3fa92d4246f849e282f15527;
                                  extension =
                                  [(1, d41d8cd98f00b204e9800998ecf8427e,
                                    ([Command(Read c1, 1)], 1.00000))]
                                  });
                          cons =
                          (Some { vval = c4677fbe3fa92d4246f849e282f15527;
                                  vterm = 0; term = 0;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))] |}] ;
  let t1, actions = Impl.advance t1 Tick in
  print t1 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = c4677fbe3fa92d4246f849e282f15527; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(c4677fbe3fa92d4246f849e282f15527:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e,
                    ([Command(Read c1, 1)], 1.00000));
                   parent = <opaque>; key = c4677fbe3fa92d4246f849e282f15527 });
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
         command_buffer =
         { store = []; hwm = 1.00000; interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 1; limit = 100 };
         clock = <opaque> }
    actions: [Send(0,(Conspire_dc.Types.Conspire
                        { ctree =
                          (Some { new_head = c4677fbe3fa92d4246f849e282f15527;
                                  extension =
                                  [(1, d41d8cd98f00b204e9800998ecf8427e,
                                    ([Command(Read c1, 1)], 1.00000))]
                                  });
                          cons =
                          (Some { vval = c4677fbe3fa92d4246f849e282f15527;
                                  vterm = 0; term = 0;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))
              Send(2,(Conspire_dc.Types.Conspire
                        { ctree =
                          (Some { new_head = c4677fbe3fa92d4246f849e282f15527;
                                  extension =
                                  [(1, d41d8cd98f00b204e9800998ecf8427e,
                                    ([Command(Read c1, 1)], 1.00000))]
                                  });
                          cons =
                          (Some { vval = c4677fbe3fa92d4246f849e282f15527;
                                  vterm = 0; term = 0;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))
              Send(3,(Conspire_dc.Types.Conspire
                        { ctree =
                          (Some { new_head = c4677fbe3fa92d4246f849e282f15527;
                                  extension =
                                  [(1, d41d8cd98f00b204e9800998ecf8427e,
                                    ([Command(Read c1, 1)], 1.00000))]
                                  });
                          cons =
                          (Some { vval = c4677fbe3fa92d4246f849e282f15527;
                                  vterm = 0; term = 0;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))] |}] ;
  let root_clk = t0.conspire.rep.store.root in
  let c1_clk = Md5.of_hex_exn "c4677fbe3fa92d4246f849e282f15527" in
  let replication_message =
    Conspire
      (Ok
         { ctree=
             Some
               { new_head= c1_clk
               ; extension= [(1, root_clk, ([c1], Utils.float_to_time 1.))] }
         ; cons= Some {vval= c1_clk; vterm= 0; term= 0; commit_index= root_clk}
         } )
  in
  let t0, actions = Impl.advance t0 (Recv (replication_message, 1)) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = c4677fbe3fa92d4246f849e282f15527; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(c4677fbe3fa92d4246f849e282f15527:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e,
                    ([Command(Read c1, 1)], 1.00000));
                   parent = <opaque>; key = c4677fbe3fa92d4246f849e282f15527 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = c4677fbe3fa92d4246f849e282f15527; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         command_buffer =
         { store = []; hwm = 1.00000; interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 1; limit = 100 };
         clock = <opaque> }
    actions: [] |}] ;
  let t0, actions = Impl.advance t0 (Recv (replication_message, 2)) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = c4677fbe3fa92d4246f849e282f15527; vterm = 0; term = 0;
               commit_index = c4677fbe3fa92d4246f849e282f15527 };
             store =
             { ctree =
               [(c4677fbe3fa92d4246f849e282f15527:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e,
                    ([Command(Read c1, 1)], 1.00000));
                   parent = <opaque>; key = c4677fbe3fa92d4246f849e282f15527 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = c4677fbe3fa92d4246f849e282f15527; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = c4677fbe3fa92d4246f849e282f15527; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [([Command(Read c1, 1)], 1.00000)] };
         command_buffer =
         { store = []; hwm = 1.00000; interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 1; limit = 100 };
         clock = <opaque> }
    actions: [CommitCommands(Command(Read c1, 1))
              Send(1,(Conspire_dc.Types.Conspire
                        { ctree = None;
                          cons =
                          (Some { vval = c4677fbe3fa92d4246f849e282f15527;
                                  vterm = 0; term = 0;
                                  commit_index = c4677fbe3fa92d4246f849e282f15527
                                  })
                          }))
              Send(2,(Conspire_dc.Types.Conspire
                        { ctree = None;
                          cons =
                          (Some { vval = c4677fbe3fa92d4246f849e282f15527;
                                  vterm = 0; term = 0;
                                  commit_index = c4677fbe3fa92d4246f849e282f15527
                                  })
                          }))
              Send(3,(Conspire_dc.Types.Conspire
                        { ctree = None;
                          cons =
                          (Some { vval = c4677fbe3fa92d4246f849e282f15527;
                                  vterm = 0; term = 0;
                                  commit_index = c4677fbe3fa92d4246f849e282f15527
                                  })
                          }))] |}]

let%expect_test "batching" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let clk0, ac0 = make_clock () in
  let t0 = create (c4 0 clk0) in
  let c0 = make_command (Read "c0") in
  let t0, actions = Impl.advance t0 (Commands (Iter.singleton c0)) in
  print t0 actions ;
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
         command_buffer =
         { store = [(1.00000: [Command(Read c0, 1)])]; hwm = 0.00000;
           interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 0; limit = 100 };
         clock = <opaque> }
    actions: [Broadcast((Conspire_dc.Types.Commands
                           ([Command(Read c0, 1)], 1.00000)))] |}] ;
  ac0 1.1 ;
  [%expect {| +mock time is now 1.1 |}] ;
  let c1 = make_command (Read "c1") in
  let t0, actions = Impl.advance t0 (Commands (Iter.of_list [c1])) in
  print t0 actions ;
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
         command_buffer =
         { store =
           [(1.00000: [Command(Read c0, 1)]); (2.10000: [Command(Read c1, 2)])];
           hwm = 0.00000; interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 0; limit = 100 };
         clock = <opaque> }
    actions: [Broadcast((Conspire_dc.Types.Commands
                           ([Command(Read c1, 2)], 2.10000)))] |}] ;
  ac0 0.5 ;
  let c2 = make_command (Read "c2") in
  let t0, actions = Impl.advance t0 (Commands (Iter.of_list [c2])) in
  print t0 actions ;
  [%expect
    {|
    +mock time is now 1.6
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
         command_buffer =
         { store =
           [(1.00000: [Command(Read c0, 1)]); (2.10000: [Command(Read c1, 2)]);
            (2.60000: [Command(Read c2, 3)])];
           hwm = 0.00000; interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 0; limit = 100 };
         clock = <opaque> }
    actions: [Broadcast((Conspire_dc.Types.Commands
                           ([Command(Read c2, 3)], 2.60000)))] |}] ;
  let t0, actions = Impl.advance t0 Tick in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 34e452ffd92c22ea72ef125a33a0a593; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(34e452ffd92c22ea72ef125a33a0a593:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e,
                    ([Command(Read c0, 1)], 1.00000));
                   parent = <opaque>; key = 34e452ffd92c22ea72ef125a33a0a593 });
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
         command_buffer =
         { store =
           [(2.10000: [Command(Read c1, 2)]); (2.60000: [Command(Read c2, 3)])];
           hwm = 1.00000; interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 1; limit = 100 };
         clock = <opaque> }
    actions: [Send(1,(Conspire_dc.Types.Conspire
                        { ctree =
                          (Some { new_head = 34e452ffd92c22ea72ef125a33a0a593;
                                  extension =
                                  [(1, d41d8cd98f00b204e9800998ecf8427e,
                                    ([Command(Read c0, 1)], 1.00000))]
                                  });
                          cons =
                          (Some { vval = 34e452ffd92c22ea72ef125a33a0a593;
                                  vterm = 0; term = 0;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))
              Send(2,(Conspire_dc.Types.Conspire
                        { ctree =
                          (Some { new_head = 34e452ffd92c22ea72ef125a33a0a593;
                                  extension =
                                  [(1, d41d8cd98f00b204e9800998ecf8427e,
                                    ([Command(Read c0, 1)], 1.00000))]
                                  });
                          cons =
                          (Some { vval = 34e452ffd92c22ea72ef125a33a0a593;
                                  vterm = 0; term = 0;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))
              Send(3,(Conspire_dc.Types.Conspire
                        { ctree =
                          (Some { new_head = 34e452ffd92c22ea72ef125a33a0a593;
                                  extension =
                                  [(1, d41d8cd98f00b204e9800998ecf8427e,
                                    ([Command(Read c0, 1)], 1.00000))]
                                  });
                          cons =
                          (Some { vval = 34e452ffd92c22ea72ef125a33a0a593;
                                  vterm = 0; term = 0;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))] |}] ;
  ac0 1. ;
  [%expect {| +mock time is now 2.6 |}] ;
  ac0 1. ;
  [%expect {| +mock time is now 3.6 |}] ;
  let t0, actions = Impl.advance t0 Tick in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 4208465ea72aa1217306cdb4da303438; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(34e452ffd92c22ea72ef125a33a0a593:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e,
                    ([Command(Read c0, 1)], 1.00000));
                   parent = <opaque>; key = 34e452ffd92c22ea72ef125a33a0a593 });
                (4208465ea72aa1217306cdb4da303438:
                 { node =
                   (2, 34e452ffd92c22ea72ef125a33a0a593,
                    ([Command(Read c1, 2); Command(Read c2, 3)], 3.00000));
                   parent = <opaque>; key = 4208465ea72aa1217306cdb4da303438 });
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
         command_buffer =
         { store = []; hwm = 3.00000; interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 2; limit = 100 };
         clock = <opaque> }
    actions: [Send(1,(Conspire_dc.Types.Conspire
                        { ctree =
                          (Some { new_head = 4208465ea72aa1217306cdb4da303438;
                                  extension =
                                  [(2, 34e452ffd92c22ea72ef125a33a0a593,
                                    ([Command(Read c1, 2); Command(Read c2, 3)],
                                     3.00000))
                                    ]
                                  });
                          cons =
                          (Some { vval = 4208465ea72aa1217306cdb4da303438;
                                  vterm = 0; term = 0;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))
              Send(2,(Conspire_dc.Types.Conspire
                        { ctree =
                          (Some { new_head = 4208465ea72aa1217306cdb4da303438;
                                  extension =
                                  [(2, 34e452ffd92c22ea72ef125a33a0a593,
                                    ([Command(Read c1, 2); Command(Read c2, 3)],
                                     3.00000))
                                    ]
                                  });
                          cons =
                          (Some { vval = 4208465ea72aa1217306cdb4da303438;
                                  vterm = 0; term = 0;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))
              Send(3,(Conspire_dc.Types.Conspire
                        { ctree =
                          (Some { new_head = 4208465ea72aa1217306cdb4da303438;
                                  extension =
                                  [(2, 34e452ffd92c22ea72ef125a33a0a593,
                                    ([Command(Read c1, 2); Command(Read c2, 3)],
                                     3.00000))
                                    ]
                                  });
                          cons =
                          (Some { vval = 4208465ea72aa1217306cdb4da303438;
                                  vterm = 0; term = 0;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))] |}]

let%expect_test "Conflict" =
  Imp.set_is_test true ;
  reset_make_command_state () ;
  let clk0, ac0 = make_clock () in
  let clk1, ac1 = make_clock () in
  let t0 = create (c4 0 clk0) in
  let t1 = create (c4 1 clk1) in
  (* add c0 to t0 *)
  let c0 = make_command (Read "c0") in
  let t0, _ = Impl.advance t0 (Commands (Iter.singleton c0)) in
  ac0 2. ;
  let t0, actions = Impl.advance t0 Tick in
  print t0 actions ;
  [%expect
    {|
    +mock time is now 2
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 34e452ffd92c22ea72ef125a33a0a593; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(34e452ffd92c22ea72ef125a33a0a593:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e,
                    ([Command(Read c0, 1)], 1.00000));
                   parent = <opaque>; key = 34e452ffd92c22ea72ef125a33a0a593 });
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
         command_buffer =
         { store = []; hwm = 2.00000; interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 1; limit = 100 };
         clock = <opaque> }
    actions: [Send(1,(Conspire_dc.Types.Conspire
                        { ctree =
                          (Some { new_head = 34e452ffd92c22ea72ef125a33a0a593;
                                  extension =
                                  [(1, d41d8cd98f00b204e9800998ecf8427e,
                                    ([Command(Read c0, 1)], 1.00000))]
                                  });
                          cons =
                          (Some { vval = 34e452ffd92c22ea72ef125a33a0a593;
                                  vterm = 0; term = 0;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))
              Send(2,(Conspire_dc.Types.Conspire
                        { ctree =
                          (Some { new_head = 34e452ffd92c22ea72ef125a33a0a593;
                                  extension =
                                  [(1, d41d8cd98f00b204e9800998ecf8427e,
                                    ([Command(Read c0, 1)], 1.00000))]
                                  });
                          cons =
                          (Some { vval = 34e452ffd92c22ea72ef125a33a0a593;
                                  vterm = 0; term = 0;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))
              Send(3,(Conspire_dc.Types.Conspire
                        { ctree =
                          (Some { new_head = 34e452ffd92c22ea72ef125a33a0a593;
                                  extension =
                                  [(1, d41d8cd98f00b204e9800998ecf8427e,
                                    ([Command(Read c0, 1)], 1.00000))]
                                  });
                          cons =
                          (Some { vval = 34e452ffd92c22ea72ef125a33a0a593;
                                  vterm = 0; term = 0;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))] |}] ;
  (* add c1 to t1 *)
  let c1 = make_command (Read "c1") in
  let t1, _ = Impl.advance t1 (Commands (Iter.singleton c1)) in
  ac1 2. ;
  let t1, actions = Impl.advance t1 Tick in
  print t1 actions ;
  [%expect
    {|
    +mock time is now 2
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 9ddc31c480e04b8965ff34f30c37d919; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(9ddc31c480e04b8965ff34f30c37d919:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e,
                    ([Command(Read c1, 2)], 1.00000));
                   parent = <opaque>; key = 9ddc31c480e04b8965ff34f30c37d919 });
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
         command_buffer =
         { store = []; hwm = 2.00000; interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 1; limit = 100 };
         clock = <opaque> }
    actions: [Send(0,(Conspire_dc.Types.Conspire
                        { ctree =
                          (Some { new_head = 9ddc31c480e04b8965ff34f30c37d919;
                                  extension =
                                  [(1, d41d8cd98f00b204e9800998ecf8427e,
                                    ([Command(Read c1, 2)], 1.00000))]
                                  });
                          cons =
                          (Some { vval = 9ddc31c480e04b8965ff34f30c37d919;
                                  vterm = 0; term = 0;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))
              Send(2,(Conspire_dc.Types.Conspire
                        { ctree =
                          (Some { new_head = 9ddc31c480e04b8965ff34f30c37d919;
                                  extension =
                                  [(1, d41d8cd98f00b204e9800998ecf8427e,
                                    ([Command(Read c1, 2)], 1.00000))]
                                  });
                          cons =
                          (Some { vval = 9ddc31c480e04b8965ff34f30c37d919;
                                  vterm = 0; term = 0;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))
              Send(3,(Conspire_dc.Types.Conspire
                        { ctree =
                          (Some { new_head = 9ddc31c480e04b8965ff34f30c37d919;
                                  extension =
                                  [(1, d41d8cd98f00b204e9800998ecf8427e,
                                    ([Command(Read c1, 2)], 1.00000))]
                                  });
                          cons =
                          (Some { vval = 9ddc31c480e04b8965ff34f30c37d919;
                                  vterm = 0; term = 0;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))] |}] ;
  let root_clk = t0.conspire.rep.store.root in
  let c0_clk = Md5.of_hex_exn "34e452ffd92c22ea72ef125a33a0a593" in
  let update_t0 term =
    Conspire
      (Ok
         { ctree=
             Some
               { new_head= c0_clk
               ; extension= [(1, root_clk, ([c0], Utils.float_to_time 1.))] }
         ; cons= Some {vval= c0_clk; vterm= 0; term; commit_index= root_clk} }
      )
  in
  let t0, actions = Impl.advance t0 (Recv (update_t0 0, 2)) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 34e452ffd92c22ea72ef125a33a0a593; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(34e452ffd92c22ea72ef125a33a0a593:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e,
                    ([Command(Read c0, 1)], 1.00000));
                   parent = <opaque>; key = 34e452ffd92c22ea72ef125a33a0a593 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = 34e452ffd92c22ea72ef125a33a0a593; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         command_buffer =
         { store = []; hwm = 2.00000; interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 1; limit = 100 };
         clock = <opaque> }
    actions: [] |}] ;
  let c1_clk = Md5.of_hex_exn "620122743bc84de6b418bd632ea0cdc2" in
  let update_t1 term =
    Conspire
      (Ok
         { ctree=
             Some
               { new_head= c1_clk
               ; extension= [(1, root_clk, ([c1], Utils.float_to_time 1.))] }
         ; cons= Some {vval= c1_clk; vterm= 0; term; commit_index= root_clk} }
      )
  in
  let t0, actions = Impl.advance t0 (Recv (update_t1 0, 1)) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 34e452ffd92c22ea72ef125a33a0a593; vterm = 0; term = 1;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(34e452ffd92c22ea72ef125a33a0a593:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e,
                    ([Command(Read c0, 1)], 1.00000));
                   parent = <opaque>; key = 34e452ffd92c22ea72ef125a33a0a593 });
                (620122743bc84de6b418bd632ea0cdc2:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e,
                    ([Command(Read c1, 2)], 1.00000));
                   parent = <opaque>; key = 620122743bc84de6b418bd632ea0cdc2 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 620122743bc84de6b418bd632ea0cdc2; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = 34e452ffd92c22ea72ef125a33a0a593; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         command_buffer =
         { store = []; hwm = 2.00000; interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 1; limit = 100 };
         clock = <opaque> }
    actions: [Send(1,(Conspire_dc.Types.Conspire
                        { ctree = None;
                          cons =
                          (Some { vval = 34e452ffd92c22ea72ef125a33a0a593;
                                  vterm = 0; term = 1;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))
              Send(2,(Conspire_dc.Types.Conspire
                        { ctree = None;
                          cons =
                          (Some { vval = 34e452ffd92c22ea72ef125a33a0a593;
                                  vterm = 0; term = 1;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))
              Send(3,(Conspire_dc.Types.Conspire
                        { ctree = None;
                          cons =
                          (Some { vval = 34e452ffd92c22ea72ef125a33a0a593;
                                  vterm = 0; term = 1;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))] |}] ;
  let t0, actions = Impl.advance t0 (Recv (update_t1 1, 1)) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 34e452ffd92c22ea72ef125a33a0a593; vterm = 0; term = 1;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(34e452ffd92c22ea72ef125a33a0a593:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e,
                    ([Command(Read c0, 1)], 1.00000));
                   parent = <opaque>; key = 34e452ffd92c22ea72ef125a33a0a593 });
                (620122743bc84de6b418bd632ea0cdc2:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e,
                    ([Command(Read c1, 2)], 1.00000));
                   parent = <opaque>; key = 620122743bc84de6b418bd632ea0cdc2 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 620122743bc84de6b418bd632ea0cdc2; vterm = 0; term = 1;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = 34e452ffd92c22ea72ef125a33a0a593; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         command_buffer =
         { store = []; hwm = 2.00000; interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 1; limit = 100 };
         clock = <opaque> }
    actions: [] |}] ;
  let t0, actions = Impl.advance t0 (Recv (update_t0 1, 2)) in
  print t0 actions ;
  [%expect
    {|
    t: { config = <opaque>;
         conspire =
         { rep =
           { state =
             { vval = 34e452ffd92c22ea72ef125a33a0a593; vterm = 1; term = 1;
               commit_index = d41d8cd98f00b204e9800998ecf8427e };
             store =
             { ctree =
               [(34e452ffd92c22ea72ef125a33a0a593:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e,
                    ([Command(Read c0, 1)], 1.00000));
                   parent = <opaque>; key = 34e452ffd92c22ea72ef125a33a0a593 });
                (620122743bc84de6b418bd632ea0cdc2:
                 { node =
                   (1, d41d8cd98f00b204e9800998ecf8427e,
                    ([Command(Read c1, 2)], 1.00000));
                   parent = <opaque>; key = 620122743bc84de6b418bd632ea0cdc2 });
                (d41d8cd98f00b204e9800998ecf8427e: Root)];
               root = d41d8cd98f00b204e9800998ecf8427e };
             remotes = <opaque> };
           other_nodes_state =
           [(1:
             { vval = 620122743bc84de6b418bd632ea0cdc2; vterm = 0; term = 1;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (2:
             { vval = 34e452ffd92c22ea72ef125a33a0a593; vterm = 0; term = 1;
               commit_index = d41d8cd98f00b204e9800998ecf8427e });
            (3:
             { vval = d41d8cd98f00b204e9800998ecf8427e; vterm = 0; term = 0;
               commit_index = d41d8cd98f00b204e9800998ecf8427e })];
           config = <opaque>; commit_log = [] };
         command_buffer =
         { store = []; hwm = 2.00000; interval = 1s; compare = <fun> };
         tick_count = { Conspire_dc.Counter.count = 1; limit = 100 };
         clock = <opaque> }
    actions: [Send(1,(Conspire_dc.Types.Conspire
                        { ctree = None;
                          cons =
                          (Some { vval = 34e452ffd92c22ea72ef125a33a0a593;
                                  vterm = 1; term = 1;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))
              Send(2,(Conspire_dc.Types.Conspire
                        { ctree = None;
                          cons =
                          (Some { vval = 34e452ffd92c22ea72ef125a33a0a593;
                                  vterm = 1; term = 1;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))
              Send(3,(Conspire_dc.Types.Conspire
                        { ctree = None;
                          cons =
                          (Some { vval = 34e452ffd92c22ea72ef125a33a0a593;
                                  vterm = 1; term = 1;
                                  commit_index = d41d8cd98f00b204e9800998ecf8427e
                                  })
                          }))] |}]

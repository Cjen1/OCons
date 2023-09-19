open! Core
open! Conspire_command_tree

module CTree = CommandTree (struct
  type t = int [@@deriving compare, show, bin_io]
end)

let nnodes = 3

let print_tree = Fmt.pr "%a" CTree.pp

let make_clock term clocks =
  let clock =
    Map.of_alist_exn (module Int) (List.mapi clocks ~f:(fun idx v -> (idx, v)))
  in
  VectorClock.{term; clock}

let%expect_test "create_and_add" =
  let ct = CTree.create [0;1;2] 0 in
  print_tree ct ;
  [%expect
    {|
    { ctree = [(0:[0,0,0], Root)] } |}] ;
  let ct, _ =
    CTree.addv ct ~node:0 ~parent:(make_clock 0 [0; 0; 0]) (Iter.of_list [1; 2])
  in
  print_tree ct ;
  [%expect
    {|
  { ctree =
    [(0:[0,0,0], Root): (0:[1,0,0], (1, 0:[0,0,0], 1)):
     (0:[2,0,0], (2, 0:[1,0,0], 2))]
    } |}] ;
  let ct , _=
    CTree.addv ct ~node:1 ~parent:(make_clock 0 [1; 0; 0]) (Iter.of_list [3])
  in
  let ct, _=
    CTree.addv ct ~node:1
      ~parent:(make_clock 0 [1; 1; 0])
      (Iter.of_list [4]) ~term:1
  in
  (*
  0,000 - 0,100 - 0,200
                - 0,110 - 1,120
  *)
  print_tree ct ;
  [%expect
    {|
  { ctree =
    [(0:[0,0,0], Root): (0:[1,0,0], (1, 0:[0,0,0], 1)):
     (0:[1,1,0], (2, 0:[1,0,0], 3)): (0:[2,0,0], (2, 0:[1,0,0], 2)):
     (1:[1,2,0], (3, 0:[1,1,0], 4))]
    } |}]

let%expect_test "prefix" =
  let ct = CTree.create [0;1;2] 0 in
  let ct, _ =
    CTree.addv ct ~node:0 ~parent:(make_clock 0 [0; 0; 0]) (Iter.of_list [1; 2])
  in
  let ct, _ =
    CTree.addv ct ~node:1 ~parent:(make_clock 0 [1; 0; 0]) (Iter.of_list [3])
  in
  let ct, _ =
    CTree.addv ct ~node:1
      ~parent:(make_clock 0 [1; 1; 0])
      (Iter.of_list [4]) ~term:1
  in
  (*
  0,000 - 0,100 - 0,200
                - 0,110 - 1,120
  *)
  Fmt.pr "0,000 < 0,100 %b@."
    (CTree.prefix ct (make_clock 0 [0; 0; 0]) (make_clock 0 [1; 0; 0])) ;
  Fmt.pr "0,100 ~ 0,010 %b@."
    (CTree.prefix ct (make_clock 0 [1; 0; 0]) (make_clock 0 [0; 1; 0])) ;
  Fmt.pr "0,100 < 1,120 %b@."
    (CTree.prefix ct (make_clock 0 [1; 0; 0]) (make_clock 1 [1; 2; 0])) ;
  Fmt.pr "0,200 < 1,120 %b@."
    (CTree.prefix ct (make_clock 0 [2; 0; 0]) (make_clock 1 [1; 2; 0])) ;
  [%expect
    {|
    0,000 < 0,100 true
    0,100 ~ 0,010 false
    0,100 < 1,120 true
    0,200 < 1,120 false |}]

let%expect_test "make_update" =
  let ct = CTree.create [0;1;2] 0 in
  let ct, _ =
    CTree.addv ct ~node:0 ~parent:(make_clock 0 [0; 0; 0]) (Iter.of_list [1; 2])
  in
  let partial_tree = ct in
  Fmt.pr "%a@." (Conspire_command_tree.set_pp VectorClock.pp) (Map.key_set ct.ctree) ;
  [%expect
    {|
    [0:[0,0,0], 0:[1,0,0], 0:[2,0,0]] |}] ;
  let ct, _ =
    CTree.addv ct ~node:1 ~parent:(make_clock 0 [1; 0; 0]) (Iter.of_list [3])
  in
  let ct, _ =
    CTree.addv ct ~node:1
      ~parent:(make_clock 0 [1; 1; 0])
      (Iter.of_list [4]) ~term:1
  in
  (*
  0,000 - 0,100 - 0,200
                - 0,110 - 1,120
  *)
  let update = CTree.make_update ct (make_clock 1 [1; 2; 0]) partial_tree in
  Fmt.pr "%a@." CTree.pp_update update ;
  [%expect
    {|
  { Conspire_command_tree.CommandTree.new_head = 1:[1,2,0];
    extension = [(2, 0:[1,0,0], 3); (3, 0:[1,1,0], 4)] } |}]

let%expect_test "sufficient_prefix" =
  let ct = CTree.create [0;1;2] 0 in
  let ct, _ =
    CTree.addv ct ~node:0 ~parent:(make_clock 0 [0; 0; 0]) (Iter.of_list [1; 2])
  in
  let ct, _ =
    CTree.addv ct ~node:1 ~parent:(make_clock 0 [1; 0; 0]) (Iter.of_list [3])
  in
  let ct, _ =
    CTree.addv ct ~node:1
      ~parent:(make_clock 0 [1; 1; 0])
      (Iter.of_list [4]) ~term:1
  in
  let ct, _ =
    CTree.addv ct ~node:2
      ~parent:(make_clock 0 [1; 1; 0])
      (Iter.of_list [5]) ~term:0
  in
  (*
   * 0,000 - 0,100 - 0,200
   *               - 0,110 - 1,120
   *                       - 0,111
  *)
  let clks = [
    make_clock 0 [2;0;0];
    make_clock 1 [1;2;0];
    make_clock 0 [1;1;1];
  ] in
  let x = CTree.greatest_sufficiently_common_prefix ct clks 2 in
  Fmt.pr "%a@." VectorClock.pp x;
  [%expect{|
    0:[1,1,0] |}];
  let x = CTree.greatest_sufficiently_common_prefix ct clks 3 in
  Fmt.pr "%a@." VectorClock.pp x;
  [%expect{|
    0:[1,0,0] |}];

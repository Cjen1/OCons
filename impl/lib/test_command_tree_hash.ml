open! Core
open! Conspire_command_tree_hashed

module CTree = CommandTree (struct
  type t = int [@@deriving compare, show, bin_io, hash]
end)

let nnodes = 3

let print_tree = Fmt.pr "%a" CTree.pp

let make_clock term clocks = Conspire_command_tree.VectorClock.test_make_clock term clocks

let%expect_test "create_and_add" =
  let ct = CTree.empty in
  print_tree ct ;
  [%expect {|
    { ctree = [(0: Root)]; root = 0 } |}] ;
  let ct, _ =
    CTree.addv ct ~node:0 ~parent:(0) (Iter.of_list [1; 2])
  in
  print_tree ct ;
  [%expect
    {|
  { ctree =
    [(0: Root);
     (630357328:
      { node = (2, 1002037630, 2); parent = <opaque>; key = 630357328 });
     (1002037630: { node = (1, 0, 1); parent = <opaque>; key = 1002037630 })];
    root = 0 } |}] ;
  let ct, _ =
    CTree.addv ct ~node:1 ~parent:(1002037630) (Iter.of_list [3])
  in
  let ct, _ =
    CTree.addv ct ~node:1
      ~parent:(760594639)
      (Iter.of_list [4])
  in
  (*
  RT - 1 - 2
         - 3 - 4
  *)
  print_tree ct ;
  [%expect
    {|
  { ctree =
    [(0: Root);
     (343168259:
      { node = (3, 760594639, 4); parent = <opaque>; key = 343168259 });
     (630357328:
      { node = (2, 1002037630, 2); parent = <opaque>; key = 630357328 });
     (760594639:
      { node = (2, 1002037630, 3); parent = <opaque>; key = 760594639 });
     (1002037630: { node = (1, 0, 1); parent = <opaque>; key = 1002037630 })];
    root = 0 } |}]

let%expect_test "prefix" =
  let ct = CTree.empty  in
  let hd1 = 1002037630 in
  let ct, hd2 =
    CTree.addv ct ~node:0 ~parent:(0) (Iter.of_list [1; 2])
  in
  let ct, hd3 =
    CTree.addv ct ~node:1 ~parent:(hd1) (Iter.of_list [3])
  in
  let ct, hd4 =
    CTree.addv ct ~node:1
      ~parent:(hd3)
      (Iter.of_list [4]) 
  in
  (*
  RT - 1 - 2
         - 3 - 4
  *)
  Fmt.pr "RT < 1 %b@."
    (CTree.prefix ct (0) (hd1)) ;
  Fmt.pr "1 < 4 %b@."
    (CTree.prefix ct (hd1) (hd4)) ;
  Fmt.pr "2 < 4 %b@."
    (CTree.prefix ct (hd2) (hd4)) ;
  [%expect
    {|
    RT < 1 true
    1 < 4 true
    2 < 4 false |}]

let%expect_test "make_update" =
  let ct = CTree.empty  in
  let ct, _ =
    CTree.addv ct ~node:0 ~parent:(0) (Iter.of_list [1; 2])
  in
  let partial_tree = ct in
  Fmt.pr "%a@."
    (Conspire_command_tree.set_pp Int.pp)
    (Map.key_set ct.ctree) ;
  [%expect {|
    [0, 630357328, 1002037630] |}] ;
  let ct, _ =
    CTree.addv ct ~node:1 ~parent:(1002037630) (Iter.of_list [3])
  in
  let ct, hd_4 =
    CTree.addv ct ~node:1
      ~parent:(760594639)
      (Iter.of_list [4])
  in
  (*
  RT - 1 - 2
         - 3 - 4
  *)
  let update = CTree.make_update ct (hd_4) partial_tree in
  Fmt.pr "%a@." CTree.pp_update update ;
  [%expect{| { new_head = 343168259; extension = [(2, 1002037630, 3); (3, 760594639, 4)] } |}]

let%expect_test "sufficient_prefix" =
  let ct = CTree.empty in
  let ct, hd2 =
    CTree.addv ct ~node:0 ~parent:(0) (Iter.of_list [1; 2])
  in
  let ct, hd3 =
    CTree.addv ct ~node:1 ~parent:(1002037630) (Iter.of_list [3])
  in
  let ct, hd4 =
    CTree.addv ct ~node:1
      ~parent:(hd3)
      (Iter.of_list [4]) 
  in
  let ct, hd5 =
    CTree.addv ct ~node:2
      ~parent:(hd3)
      (Iter.of_list [5]) 
  in
  (*
   * RT - 1 - 2
   *        - 3 - 4
   *            - 5
   *)
  let clks =
    [hd2;hd4;hd5]
  in
  Fmt.pr "%a@." Fmt.(list ~sep:comma int) (clks @ [hd3]);
  [%expect{|
    630357328, 343168259, 723713970,
    760594639 |}];
  let x = CTree.greatest_sufficiently_common_prefix ct clks 2 in
  Fmt.pr "%a@." (Fmt.option ~none:(Fmt.any "none") CTree.pp_parent_ref_node) (Option.value_map x ~default:None ~f:(CTree.get ct));
  (* expected is 3 *)
  [%expect {|
    { node = (2, 1002037630, 3); parent = <opaque>; key = 760594639 } |}] ;
  let x = CTree.greatest_sufficiently_common_prefix ct clks 3 in
  Fmt.pr "%a@." (Fmt.option ~none:(Fmt.any "none") CTree.pp_parent_ref_node) (Option.value_map x ~default:None ~f:(CTree.get ct));
  (* expected is 1 *)
  [%expect {|
    { node = (1, 0, 1); parent = <opaque>; key = 1002037630 } |}]

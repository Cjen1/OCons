open! Core
open! Impl_core__Conspire_command_tree

module CTree = CommandTree (struct
  type t = int [@@deriving compare, show, bin_io, hash]
end)

let nnodes = 3

let print_tree = Fmt.pr "%a" CTree.pp

let%expect_test "create_and_add" =
  let ct = CTree.empty in
  print_tree ct ;
  [%expect
    {|
    { ctree = [(d41d8cd98f00b204e9800998ecf8427e: Root)];
      root = d41d8cd98f00b204e9800998ecf8427e } |}] ;
  let ct, _ = CTree.addv ct ~node:0 ~parent:ct.root (Iter.of_list [1; 2]) in
  print_tree ct ;
  [%expect
    {|
  { ctree =
    [(5278a244879fc58054713fb2f920f455:
      { node = (1, d41d8cd98f00b204e9800998ecf8427e, 1); parent = <opaque>;
        key = 5278a244879fc58054713fb2f920f455 });
     (9b72f276548d04f7e1b6c1f1419a523b:
      { node = (2, 5278a244879fc58054713fb2f920f455, 2); parent = <opaque>;
        key = 9b72f276548d04f7e1b6c1f1419a523b });
     (d41d8cd98f00b204e9800998ecf8427e: Root)];
    root = d41d8cd98f00b204e9800998ecf8427e } |}] ;
  let hd1 = Md5.of_hex_exn "5278a244879fc58054713fb2f920f455" in
  let ct, hd3 = CTree.addv ct ~node:1 ~parent:hd1 (Iter.of_list [3]) in
  let ct, _ = CTree.addv ct ~node:1 ~parent:hd3 (Iter.of_list [4]) in
  (*
  RT - 1 - 2
         - 3 - 4
  *)
  print_tree ct ;
  [%expect
    {|
    { ctree =
      [(5278a244879fc58054713fb2f920f455:
        { node = (1, d41d8cd98f00b204e9800998ecf8427e, 1); parent = <opaque>;
          key = 5278a244879fc58054713fb2f920f455 });
       (926b0e94c1e80f65be65604a4025663a:
        { node = (2, 5278a244879fc58054713fb2f920f455, 3); parent = <opaque>;
          key = 926b0e94c1e80f65be65604a4025663a });
       (9b72f276548d04f7e1b6c1f1419a523b:
        { node = (2, 5278a244879fc58054713fb2f920f455, 2); parent = <opaque>;
          key = 9b72f276548d04f7e1b6c1f1419a523b });
       (bc8f2c8b900214c0885460e6615a0a22:
        { node = (3, 926b0e94c1e80f65be65604a4025663a, 4); parent = <opaque>;
          key = bc8f2c8b900214c0885460e6615a0a22 });
       (d41d8cd98f00b204e9800998ecf8427e: Root)];
      root = d41d8cd98f00b204e9800998ecf8427e } |}]

let%expect_test "prefix" =
  let ct = CTree.empty in
  let hd1 = Md5.of_hex_exn "5278a244879fc58054713fb2f920f455" in
  let ct, hd2 = CTree.addv ct ~node:0 ~parent:ct.root (Iter.of_list [1; 2]) in
  let ct, hd3 = CTree.addv ct ~node:1 ~parent:hd1 (Iter.of_list [3]) in
  let ct, hd4 = CTree.addv ct ~node:1 ~parent:hd3 (Iter.of_list [4]) in
  (*
  RT - 1 - 2
         - 3 - 4
  *)
  Fmt.pr "RT < 1 %b@." (CTree.prefix ct ct.root hd1) ;
  Fmt.pr "1 < 4 %b@." (CTree.prefix ct hd1 hd4) ;
  Fmt.pr "2 < 4 %b@." (CTree.prefix ct hd2 hd4) ;
  [%expect {|
    RT < 1 true
    1 < 4 true
    2 < 4 false |}]

let%expect_test "make_update" =
  let ct = CTree.empty in
  let ct, _ = CTree.addv ct ~node:0 ~parent:ct.root (Iter.of_list [1; 2]) in
  let hd1 = Md5.of_hex_exn "5278a244879fc58054713fb2f920f455" in
  let partial_tree = ct in
  Fmt.pr "%a@."
    Impl_core__Conspire_command_tree.(set_pp CTree.Key.pp)
    (Map.key_set ct.ctree) ;
  [%expect
    {|
    [5278a244879fc58054713fb2f920f455, 9b72f276548d04f7e1b6c1f1419a523b,
     d41d8cd98f00b204e9800998ecf8427e] |}] ;
  let ct, hd3 = CTree.addv ct ~node:1 ~parent:hd1 (Iter.of_list [3]) in
  let ct, hd_4 = CTree.addv ct ~node:1 ~parent:hd3 (Iter.of_list [4]) in
  (*
  RT - 1 - 2
         - 3 - 4
  *)
  let update = CTree.make_update ct hd_4 partial_tree in
  Fmt.pr "%a@." CTree.pp_update update ;
  [%expect
    {|
    { new_head = bc8f2c8b900214c0885460e6615a0a22;
      extension =
      [(2, 5278a244879fc58054713fb2f920f455, 3);
        (3, 926b0e94c1e80f65be65604a4025663a, 4)]
      } |}]

let%expect_test "sufficient_prefix" =
  let ct = CTree.empty in
  let ct, hd2 = CTree.addv ct ~node:0 ~parent:ct.root (Iter.of_list [1; 2]) in
  let hd1 = Md5.of_hex_exn "5278a244879fc58054713fb2f920f455" in
  let ct, hd3 = CTree.addv ct ~node:1 ~parent:hd1 (Iter.of_list [3]) in
  let ct, hd4 = CTree.addv ct ~node:1 ~parent:hd3 (Iter.of_list [4]) in
  let ct, hd5 = CTree.addv ct ~node:2 ~parent:hd3 (Iter.of_list [5]) in
  (*
   * RT - 1 - 2
   *        - 3 - 4
   *            - 5
   *)
  let clks = [hd2; hd4; hd5] in
  Fmt.pr "%a@." Fmt.(list ~sep:comma CTree.Key.pp) (clks @ [hd3]) ;
  [%expect
    {|
    9b72f276548d04f7e1b6c1f1419a523b, bc8f2c8b900214c0885460e6615a0a22,
    8911b6dc8fdf4d6beb30e6957a1eafd2,
    926b0e94c1e80f65be65604a4025663a |}] ;
  let x = CTree.greatest_sufficiently_common_prefix ct clks 2 in
  Fmt.pr "%a@."
    (Fmt.option ~none:(Fmt.any "none") CTree.pp_parent_ref_node)
    (Option.value_map x ~default:None ~f:(CTree.get ct)) ;
  (* expected is 3 *)
  [%expect
    {|
    { node = (2, 5278a244879fc58054713fb2f920f455, 3); parent = <opaque>;
      key = 926b0e94c1e80f65be65604a4025663a } |}] ;
  let x = CTree.greatest_sufficiently_common_prefix ct clks 3 in
  Fmt.pr "%a@."
    (Fmt.option ~none:(Fmt.any "none") CTree.pp_parent_ref_node)
    (Option.value_map x ~default:None ~f:(CTree.get ct)) ;
  (* expected is 1 *)
  [%expect
    {|
    { node = (1, d41d8cd98f00b204e9800998ecf8427e, 1); parent = <opaque>;
      key = 5278a244879fc58054713fb2f920f455 } |}]

let%expect_test "compare" =
  let ct = CTree.empty in
  let ct, hd2 = CTree.addv ct ~node:0 ~parent:ct.root (Iter.of_list [1; 2]) in
  let hd1 = Md5.of_hex_exn "5278a244879fc58054713fb2f920f455" in
  let ct, hd3 = CTree.addv ct ~node:1 ~parent:hd1 (Iter.of_list [3]) in
  let ct, _ = CTree.addv ct ~node:1 ~parent:hd3 (Iter.of_list [4]) in
  let ct, hd5 = CTree.addv ct ~node:2 ~parent:hd3 (Iter.of_list [5]) in
  (*
   * RT - 1 - 2
   *        - 3 - 4
   *            - 5
   *)
  Fmt.pr "1 ? 5 %a@."
    Fmt.(option ~none:(any "NONE") Impl_core__Utils.pp_comp)
    (CTree.compare_keys ct hd1 hd5) ;
  [%expect {| 1 ? 5 Utils.LT |}] ;
  Fmt.pr "5 ? 1 %a@."
    Fmt.(option ~none:(any "NONE") Impl_core__Utils.pp_comp)
    (CTree.compare_keys ct hd5 hd1) ;
  [%expect {| 5 ? 1 Utils.GT |}] ;
  Fmt.pr "1 ? 2 %a@."
    Fmt.(option ~none:(any "NONE") Impl_core__Utils.pp_comp)
    (CTree.compare_keys ct hd1 hd2) ;
  [%expect {| 1 ? 2 Utils.LT |}] ;
  Fmt.pr "2 ? 1 %a@."
    Fmt.(option ~none:(any "NONE") Impl_core__Utils.pp_comp)
    (CTree.compare_keys ct hd5 hd1) ;
  [%expect {| 2 ? 1 Utils.GT |}] ;
  Fmt.pr "2 ? 3 %a@."
    Fmt.(option ~none:(any "NONE") Impl_core__Utils.pp_comp)
    (CTree.compare_keys ct hd2 hd3) ;
  [%expect {| 2 ? 3 NONE |}] ;
  Fmt.pr "2 ? 5 %a@."
    Fmt.(option ~none:(any "NONE") Impl_core__Utils.pp_comp)
    (CTree.compare_keys ct hd2 hd5) ;
  [%expect {| 2 ? 5 NONE |}]

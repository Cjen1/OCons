open! Core
open! Core_bench

module Hashed = struct
  open Impl_core__Conspire_command_tree_hashed

  module CTree = CommandTree (struct
    type t = int [@@deriving compare, show, bin_io, hash]
  end)

  let ctree_sufficient_prefix =
    let test `init =
      let ct = CTree.empty in
      let ct, hd2 = CTree.addv ct ~node:0 ~parent:0 (Iter.of_list [1; 2]) in
      let ct, hd3 =
        CTree.addv ct ~node:1 ~parent:1002037630 (Iter.of_list [3])
      in
      let ct, hd4 = CTree.addv ct ~node:1 ~parent:hd3 (Iter.of_list [4]) in
      let ct, hd5 = CTree.addv ct ~node:2 ~parent:hd3 (Iter.of_list [5]) in
      let clks = [hd2; hd4; hd5] in
      fun () -> CTree.greatest_sufficiently_common_prefix ct clks 3 |> ignore
    in
    Bench.Test.create_with_initialization ~name:"CTree.ctree_sufficient_prefix" test
end

module VC = struct
  open Impl_core__Conspire_command_tree

  module CTree = CommandTree (struct
    type t = int [@@deriving compare, show, bin_io, hash]
  end)

  let make_clock term clocks = VectorClock.test_make_clock term clocks

  let ctree_sufficient_prefix =
    let test `init =
      let ct = CTree.create [0; 1; 2] 0 in
      let ct, _ =
        CTree.addv ct ~node:0
          ~parent:(make_clock 0 [0; 0; 0])
          (Iter.of_list [1; 2])
      in
      let ct, _ =
        CTree.addv ct ~node:1
          ~parent:(make_clock 0 [1; 0; 0])
          (Iter.of_list [3])
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
      let clks =
        [make_clock 0 [2; 0; 0]; make_clock 1 [1; 2; 0]; make_clock 0 [1; 1; 1]]
      in
      fun () -> CTree.greatest_sufficiently_common_prefix ct clks 3 |> ignore
    in
    Bench.Test.create_with_initialization ~name:"VC.ctree_sufficient_prefix" test
end

let () = Command_unix.run (Bench.make_command [Hashed.ctree_sufficient_prefix; VC.ctree_sufficient_prefix])

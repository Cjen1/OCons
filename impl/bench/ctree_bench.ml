open! Core
open! Core_bench
open Impl_core__Conspire_command_tree

module CTree = CommandTree (struct
  type t = int [@@deriving compare, show, bin_io, hash]
end)

let ctree_sufficient_prefix =
  let test `init =
    let ct = CTree.empty in
    let ct, hd2 = CTree.addv ct ~node:0 ~parent:ct.root (Iter.of_list [1; 2]) in
    let hd1 = Md5.of_hex_exn "5278a244879fc58054713fb2f920f455" in
    let ct, hd3 = CTree.addv ct ~node:1 ~parent:hd1 (Iter.of_list [3]) in
    let ct, hd4 = CTree.addv ct ~node:1 ~parent:hd3 (Iter.of_list [4]) in
    let ct, hd5 = CTree.addv ct ~node:2 ~parent:hd3 (Iter.of_list [5]) in
    let clks = [hd2; hd4; hd5] in
    fun () -> CTree.greatest_sufficiently_common_prefix ct clks 3 |> ignore
  in
  Bench.Test.create_with_initialization ~name:"CTree.ctree_sufficient_prefix"
    test

let () = Command_unix.run (Bench.make_command [ctree_sufficient_prefix])

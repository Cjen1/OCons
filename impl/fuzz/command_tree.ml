open! Core

module CTree = Impl_core__Conspire_command_tree.CommandTree (struct
  type t = int [@@deriving compare, show, bin_io, hash]
end)

let ctree =
  let open Crowbar in
  fix (fun gen ->
      choose
        [ const CTree.empty
        ; map [gen; int; list1 int] (fun ctree kid vs ->
          let nodes = Map.keys ctree.CTree.ctree in
          let kid = kid % List.length nodes in
          let root = List.nth_exn nodes kid in
          let ctree', _ =
            CTree.addv ~node:0 ctree ~parent:root (List.iter vs |> Iter.from_labelled_iter) in
          ctree')])

(* take ctree and random node and node in its history *)
(* make an update of that and add it to the tree of just the path to that *)
let test_ctree_update ctree u_start u_end =
  let nodes =
    ctree.CTree.ctree |> Map.keys
    |> List.sort ~compare:(fun ka kb ->
           Int.compare (CTree.get_idx ctree ka) (CTree.get_idx ctree kb) )
  in
  let u_end = u_end % List.length nodes in
  let n_end = List.nth_exn nodes u_end in
  (* path goes from root to n_end *)
  let path = CTree.path_between ctree CTree.root_key n_end in
  let remote_path, _ = List.split_n path (u_start % max 1 (List.length path)) in
  let remote, _ =
    CTree.addv CTree.empty ~node:0 ~parent:CTree.root_key
      ( List.iter remote_path |> Iter.from_labelled_iter
      |> Iter.map (fun (_, _, v) -> v) )
  in
  let update = CTree.make_update ctree n_end remote in
  let remote = CTree.apply_update_exn remote update in
  let local_path = CTree.path_between ctree CTree.root_key n_end in
  let remote_path = CTree.path_between remote CTree.root_key n_end in
  let pp ppf v =
    Fmt.pf ppf "%a"
      Fmt.(brackets @@ list ~sep:comma int)
      (List.map v ~f:(fun (_idx, _key, v) -> v))
  in
  Crowbar.check_eq ~pp ~cmp:[%compare: CTree.node list] local_path remote_path

let () =
  let open Crowbar in
  add_test ~name:"ctree update" [ctree; int; int] test_ctree_update

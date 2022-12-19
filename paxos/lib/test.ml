open! Types
open! Utils

module Imp = ImperativeActions
module Impl = Sm.Make(ImperativeActions)

let c1 = make_config ~node_id:0 ~node_list:[0] ~election_timeout:5
let c3 = make_config ~node_id:0 ~node_list:[0;1;2] ~election_timeout:5

let%expect_test "transit_follower" =
  let t = create c1 in
  let t', actions= Imp.run_side_effects (fun () -> Impl.transit_follower 10) t in
  Fmt.pr "%a\n" node_state_pp t'.node_state;
  Fmt.pr "actions: %a\n" Fmt.(brackets @@ list ~sep:(const string "\n") action_pp) actions;
  [%expect {||}]



let%expect_test "transit_candidate" =
  let t = create c1 in
  Fmt.pr "t0: %a\n" t_pp t;
  let t', actions= Imp.run_side_effects Impl.transit_candidate t in
  Fmt.pr "t': %a\n" t_pp t';
  Fmt.pr "actions: %a\n" Fmt.(brackets @@ list ~sep:(const string "\n") action_pp) actions;
  [%expect {||}]

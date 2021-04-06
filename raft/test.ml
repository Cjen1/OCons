open! Core
open! Ocons_core
module S = Immutable_store
module R = Raft.Make (S)
open! R.Test.StateR.Let_syntax

let cmd_of_int i =
  Types.Command.{op= Read (Int.to_string i); id= Types.Id.of_int_exn i}

let single_config =
  Raft.
    { phase1quorum= 1
    ; phase2quorum= 1
    ; other_nodes= []
    ; num_nodes= 1
    ; node_id= 1
    ; election_timeout= 1 }

let three_config =
  Raft.
    { phase1quorum= 2
    ; phase2quorum= 2
    ; other_nodes= [2; 3]
    ; num_nodes= 3
    ; node_id= 1
    ; election_timeout= 1 }

let pr_err s p =
  match R.Test.StateR.eval p s with
  | Error (`Msg s) ->
      print_endline @@ Fmt.str "Error: %s" s
  | Ok _ ->
      ()

let get_result s p =
  match R.Test.StateR.eval p s with
  | Error (`Msg s) ->
      raise @@ Invalid_argument s
  | Ok ((), {t; a= actions}) ->
      (t, actions)

let get_ok = function
  | Error (`Msg s) ->
      raise @@ Invalid_argument s
  | Ok v ->
      v

let print_state (t : R.t) actions =
  let t, store = R.pop_store t in
  [%message
    (R.Test.get_node_state t : R.Test.node_state)
      (store : S.t)
      (actions : R.actions)]
  |> Sexp.to_string_hum |> print_endline ;
  t

let make_empty t = R.Test.State.empty t

let%expect_test "transitions" =
  let store = S.init () in
  let t = R.create_node three_config store in
  let () = pr_err (make_empty t) @@ R.Test.transition_to_leader () in
  [%expect
    {| Error: Cannot transition to leader from states other than candidate |}] ;
  let t, actions =
    R.Test.transition_to_candidate () |> get_result (make_empty t)
  in
  let t = print_state t actions in
  [%expect
    {|
    (("R.Test.get_node_state t"
      (Candidate (quorum ((elts (1)) (n 1) (threshold 2) (eq <fun>)))
       (timeout 0)))
     (store
      ((data ((current_term 1) (log ((store ()) (command_set ()) (length 0)))))
       (ops ((Term 1)))))
     (actions
      ((acts
        ((Send
          (2
           (RequestVote
            ((src 1) (term 1) (candidate_id 1) (last_log_index 0)
             (last_log_term 0)))))
         (Send
          (3
           (RequestVote
            ((src 1) (term 1) (candidate_id 1) (last_log_index 0)
             (last_log_term 0)))))))
       (nonblock_sync false)))) |}] ;
  let t, actions =
    R.Test.transition_to_leader () |> get_result (make_empty t)
  in
  let t = print_state t actions in
  let _ = t in
  [%expect.unreachable]
[@@expect.uncaught_exn {|
  (* CR expect_test_collector: This test expectation appears to contain a backtrace.
     This is strongly discouraged as backtraces are fragile.
     Please change this test to not include a backtrace. *)

  (Invalid_argument
    "Cannot transition to leader from states other than candidate")
  Raised at Raft_lib__Test.get_result in file "raft/test.ml", line 38, characters 6-11
  Called from Raft_lib__Test.(fun) in file "raft/test.ml", line 91, characters 4-63
  Called from Expect_test_collector.Make.Instance.exec in file "collector/expect_test_collector.ml", line 244, characters 12-19 |}]

let%expect_test "tick" =
  let store = S.init () in
  let t = R.create_node three_config store in
  let t, actions = R.advance t `Tick |> get_ok in
  let t = print_state t actions in
  [%expect
    {|
    (("R.Test.get_node_state t"
      (Candidate (quorum ((elts (1)) (n 1) (threshold 2) (eq <fun>)))
       (timeout 0)))
     (store
      ((data ((current_term 1) (log ((store ()) (command_set ()) (length 0)))))
       (ops ((Term 1)))))
     (actions
      ((acts
        ((Send
          (2
           (RequestVote
            ((src 1) (term 1) (candidate_id 1) (last_log_index 0)
             (last_log_term 0)))))
         (Send
          (3
           (RequestVote
            ((src 1) (term 1) (candidate_id 1) (last_log_index 0)
             (last_log_term 0)))))))
       (nonblock_sync false)))) |}] ;
  let t, _ = R.Test.transition_to_follower () |> get_result (make_empty t) in
  let t, actions = R.advance t `Tick |> get_ok in
  let _t = print_state t actions in
  [%expect
    {|
    (("R.Test.get_node_state t" (Follower (timeout 1)))
     (store
      ((data ((current_term 1) (log ((store ()) (command_set ()) (length 0)))))
       (ops ())))
     (actions ((acts ()) (nonblock_sync false)))) |}]

let%expect_test "loop single" =
  let store = S.init () in
  let t = R.create_node single_config store in
  let t, actions =
    R.advance t (`Commands [cmd_of_int 1; cmd_of_int 2]) |> get_ok
  in
  let t = print_state t actions in
  [%expect
    {|
    (("R.Test.get_node_state t" (Follower (timeout 1)))
     (store
      ((data ((current_term 0) (log ((store ()) (command_set ()) (length 0)))))
       (ops ())))
     (actions
      ((acts ((Unapplied (((op (Read 1)) (id 1)) ((op (Read 2)) (id 2))))))
       (nonblock_sync false)))) |}] ;
  let t, actions = R.advance t `Tick |> get_ok in
  let t = print_state t actions in
  [%expect.unreachable] ;
  let t, actions =
    R.advance t (`Commands [cmd_of_int 1; cmd_of_int 2]) |> get_ok
  in
  let t = print_state t actions in
  [%expect.unreachable] ;
  let t, actions = R.advance t Int64.(`Syncd (of_int 2)) |> get_ok in
  let t = print_state t actions in
  let _ = t in
  [%expect.unreachable]
[@@expect.uncaught_exn {|
  (* CR expect_test_collector: This test expectation appears to contain a backtrace.
     This is strongly discouraged as backtraces are fragile.
     Please change this test to not include a backtrace. *)

  (Not_found_s 0)
  Raised at Base__Result.ok_exn in file "src/result.ml" (inlined), line 201, characters 17-26
  Called from Ocons_core__Immutable_store.ILog.get_index_exn in file "lib/immutable_store.ml", line 62, characters 26-56
  Called from Raft_lib__Raft.Make.check_commit_index.(fun) in file "raft/raft.ml", line 228, characters 10-48
  Called from Raft_lib__Raft.Make.State.bind in file "raft/raft.ml", line 138, characters 11-16
  Called from Raft_lib__Raft.Make.State.eval in file "raft/raft.ml" (inlined), line 133, characters 52-55
  Called from Raft_lib__Raft.Make.State.bind in file "raft/raft.ml", line 137, characters 18-27
  Called from Raft_lib__Raft.Make.State.eval in file "raft/raft.ml" (inlined), line 133, characters 52-55
  Called from Raft_lib__Raft.Make.StateR.eval in file "raft/raft.ml", line 199, characters 12-30
  Called from Raft_lib__Raft.Make.advance in file "raft/raft.ml", line 616, characters 6-38
  Called from Raft_lib__Test.(fun) in file "raft/test.ml", line 167, characters 19-36
  Called from Expect_test_collector.Make.Instance.exec in file "collector/expect_test_collector.ml", line 244, characters 12-19 |}]

let%expect_test "loop triple" =
  let s1 = S.init () in
  let t1 = R.create_node three_config s1 in
  let s2 = S.init () in
  let t2 =
    R.create_node {three_config with other_nodes= [1; 3]; node_id= 2} s2
  in
  let t2, actions = R.advance t2 `Tick |> get_ok in
  let t2 = print_state t2 actions in
  [%expect
    {|
    (("R.Test.get_node_state t"
      (Candidate (quorum ((elts (2)) (n 1) (threshold 2) (eq <fun>)))
       (timeout 0)))
     (store
      ((data ((current_term 1) (log ((store ()) (command_set ()) (length 0)))))
       (ops ((Term 1)))))
     (actions
      ((acts
        ((Send
          (1
           (RequestVote
            ((src 2) (term 1) (candidate_id 2) (last_log_index 0)
             (last_log_term 0)))))
         (Send
          (3
           (RequestVote
            ((src 2) (term 1) (candidate_id 2) (last_log_index 0)
             (last_log_term 0)))))))
       (nonblock_sync false)))) |}] ;
  let rv =
    List.find_map_exn actions.acts ~f:(function
      | `Send (dst, rv) when dst = 1 ->
          Some rv
      | _ ->
          None )
  in
  let t1, actions = R.advance t1 (`Recv rv) |> get_ok in
  let t1 = print_state t1 actions in
  [%expect
    {|
    (("R.Test.get_node_state t" (Follower (timeout 1)))
     (store
      ((data ((current_term 1) (log ((store ()) (command_set ()) (length 0)))))
       (ops ((Term 1)))))
     (actions
      ((acts
        ((Send (2 (RequestVoteResponse ((src 1) (term 1) (vote_granted false)))))))
       (nonblock_sync false)))) |}] ;
  let rvr =
    List.find_map_exn actions.acts ~f:(function
      | `Send (id, rvr) when id = 2 ->
          Some rvr
      | _ ->
          None )
  in
  let t2, actions = R.advance t2 (`Recv rvr) |> get_ok in
  let t2 = print_state t2 actions in
  [%expect
    {|
    (("R.Test.get_node_state t"
      (Candidate (quorum ((elts (2)) (n 1) (threshold 2) (eq <fun>)))
       (timeout 0)))
     (store
      ((data ((current_term 1) (log ((store ()) (command_set ()) (length 0)))))
       (ops ())))
     (actions ((acts ()) (nonblock_sync false)))) |}] ;
  let t2, actions =
    R.advance t2 (`Commands [cmd_of_int 1; cmd_of_int 2]) |> get_ok
  in
  let t2 = print_state t2 actions in
  [%expect
    {|
    (("R.Test.get_node_state t"
      (Candidate (quorum ((elts (2)) (n 1) (threshold 2) (eq <fun>)))
       (timeout 0)))
     (store
      ((data ((current_term 1) (log ((store ()) (command_set ()) (length 0)))))
       (ops ())))
     (actions
      ((acts ((Unapplied (((op (Read 1)) (id 1)) ((op (Read 2)) (id 2))))))
       (nonblock_sync false)))) |}] ;
  let ae =
    List.find_map_exn actions.acts ~f:(function
      | `Send (id, ae) when id = 1 ->
          Some ae
      | _ ->
          None )
  in
  let t2, actions = R.advance t2 (`Syncd (Int64.of_int 2)) |> get_ok in
  let t2 = print_state t2 actions in
  [%expect.unreachable] ;
  let t1, actions = R.advance t1 (`Recv ae) |> get_ok in
  let t1 = print_state t1 actions in
  let _ = t1 in
  [%expect.unreachable] ;
  let aer =
    List.find_map_exn actions.acts ~f:(function
      | `Send (id, aer) when id = 2 ->
          Some aer
      | _ ->
          None )
  in
  (* In case of full update *)
  let () =
    let t2, actions = R.advance t2 (`Recv aer) |> get_ok in
    let _t2 = print_state t2 actions in
    [%expect.unreachable]
  in
  ()
[@@expect.uncaught_exn {|
  (* CR expect_test_collector: This test expectation appears to contain a backtrace.
     This is strongly discouraged as backtraces are fragile.
     Please change this test to not include a backtrace. *)

  (Not_found_s "List.find_map_exn: not found")
  Raised at Base__List.find_map_exn.find_map_exn in file "src/list.ml" (inlined), line 263, characters 14-29
  Called from Raft_lib__Test.(fun) in file "raft/test.ml", line 280, characters 4-133
  Called from Expect_test_collector.Make.Instance.exec in file "collector/expect_test_collector.ml", line 244, characters 12-19 |}]

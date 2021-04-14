open! Core
open! Ocons_core
module S = Immutable_store
module P = Paxos.Make (S)
open! P.Test.StateR.Let_syntax

let cmd_of_int i =
  Types.Command.{op= Read (Int.to_string i); id= Types.Id.of_int_exn i}

let single_config =
  Paxos.
    { phase1quorum= 1
    ; phase2quorum= 1
    ; other_nodes= []
    ; num_nodes= 1
    ; node_id= 1
    ; election_timeout= 1 }

let three_config =
  Paxos.
    { phase1quorum= 2
    ; phase2quorum= 2
    ; other_nodes= [2; 3]
    ; num_nodes= 3
    ; node_id= 1
    ; election_timeout= 1 }

let pr_err s p =
  match P.Test.StateR.eval p s with
  | Error (`Msg s) ->
      print_endline @@ Fmt.str "Error: %s" s
  | Ok _ ->
      ()

let get_result s p =
  match P.Test.StateR.eval p s with
  | Error (`Msg s) ->
      raise @@ Invalid_argument s
  | Ok ((), {t; a= actions}) ->
      (t, actions)

let get_ok = function
  | Error (`Msg s) ->
      raise @@ Invalid_argument s
  | Ok v ->
      v

let print_state (t : P.t) actions =
  let t, store = P.pop_store t in
  [%message
    (P.Test.get_node_state t : P.Test.node_state)
      (store : S.t)
      (actions : P.actions)]
  |> Sexp.to_string_hum |> print_endline ;
  t

let make_empty t = P.Test.State.empty t

let%expect_test "transitions" =
  let store = S.init () in
  let t = P.create_node three_config store in
  let () = pr_err (make_empty t) @@ P.Test.transition_to_leader () in
  [%expect
    {| Error: Cannot transition to leader from states other than candidate |}] ;
  let t, actions =
    P.Test.transition_to_candidate () |> get_result (make_empty t)
  in
  let t = print_state t actions in
  [%expect
    {|
    (("P.Test.get_node_state t"
      (Candidate (quorum ((elts (1)) (n 1) (threshold 2) (eq <fun>)))
       (entries ()) (start_index 1) (timeout 0)))
     (store
      ((data ((current_term 1) (log ((store ()) (command_set ()) (length 0)))))
       (ops ((Term 1)))))
     (actions
      ((acts
        ((Send (2 (RequestVote ((src 1) (term 1) (leader_commit 0)))))
         (Send (3 (RequestVote ((src 1) (term 1) (leader_commit 0)))))))
       (nonblock_sync false)))) |}] ;
  let t, actions =
    P.Test.transition_to_leader () |> get_result (make_empty t)
  in
  let t = print_state t actions in
  let _ = t in
  [%expect
    {|
    (("P.Test.get_node_state t"
      (Leader (match_index ((1 0) (2 0) (3 0))) (next_index ((1 1) (2 1) (3 1)))
       (heartbeat 0)))
     (store
      ((data ((current_term 1) (log ((store ()) (command_set ()) (length 0)))))
       (ops ())))
     (actions
      ((acts
        ((Send
          (3
           (AppendEntries
            ((src 1) (term 1) (prev_log_index 0) (prev_log_term 0) (entries ())
             (entries_length 0) (leader_commit 0)))))
         (Send
          (2
           (AppendEntries
            ((src 1) (term 1) (prev_log_index 0) (prev_log_term 0) (entries ())
             (entries_length 0) (leader_commit 0)))))))
       (nonblock_sync false)))) |}]

let%expect_test "tick" =
  let store = S.init () in
  let t = P.create_node three_config store in
  let t, actions = P.advance t `Tick |> get_ok in
  let t = print_state t actions in
  [%expect
    {|
    (("P.Test.get_node_state t"
      (Candidate (quorum ((elts (1)) (n 1) (threshold 2) (eq <fun>)))
       (entries ()) (start_index 1) (timeout 0)))
     (store
      ((data ((current_term 1) (log ((store ()) (command_set ()) (length 0)))))
       (ops ((Term 1)))))
     (actions
      ((acts
        ((Send (2 (RequestVote ((src 1) (term 1) (leader_commit 0)))))
         (Send (3 (RequestVote ((src 1) (term 1) (leader_commit 0)))))))
       (nonblock_sync false)))) |}] ;
  let t, _ = P.Test.transition_to_follower () |> get_result (make_empty t) in
  let t, actions = P.advance t `Tick |> get_ok in
  let _t = print_state t actions in
  [%expect
    {|
    (("P.Test.get_node_state t" (Follower (timeout 1)))
     (store
      ((data ((current_term 1) (log ((store ()) (command_set ()) (length 0)))))
       (ops ())))
     (actions ((acts ()) (nonblock_sync false)))) |}]

let%expect_test "loop single" =
  let store = S.init () in
  let t = P.create_node single_config store in
  let t, actions =
    P.advance t (`Commands [cmd_of_int 1; cmd_of_int 2]) |> get_ok
  in
  let t = print_state t actions in
  [%expect
    {|
    (("P.Test.get_node_state t" (Follower (timeout 1)))
     (store
      ((data ((current_term 0) (log ((store ()) (command_set ()) (length 0)))))
       (ops ())))
     (actions
      ((acts ((Unapplied (((op (Read 1)) (id 1)) ((op (Read 2)) (id 2))))))
       (nonblock_sync false)))) |}] ;
  let t, actions = P.advance t `Tick |> get_ok in
  let t = print_state t actions in
  [%expect
    {|
    (("P.Test.get_node_state t"
      (Leader (match_index ((1 0))) (next_index ((1 1))) (heartbeat 0)))
     (store
      ((data ((current_term 1) (log ((store ()) (command_set ()) (length 0)))))
       (ops ((Term 1)))))
     (actions ((acts ()) (nonblock_sync true)))) |}] ;
  let t, actions =
    P.advance t (`Commands [cmd_of_int 1; cmd_of_int 2]) |> get_ok
  in
  let t = print_state t actions in
  [%expect
    {|
    (("P.Test.get_node_state t"
      (Leader (match_index ((1 0))) (next_index ((1 1))) (heartbeat 0)))
     (store
      ((data
        ((current_term 1)
         (log
          ((store
            (((command ((op (Read 2)) (id 2))) (term 1))
             ((command ((op (Read 1)) (id 1))) (term 1))))
           (command_set (1 2)) (length 2)))))
       (ops
        ((Log (Add ((command ((op (Read 2)) (id 2))) (term 1))))
         (Log (Add ((command ((op (Read 1)) (id 1))) (term 1))))))))
     (actions ((acts ()) (nonblock_sync true)))) |}] ;
  let t, actions = P.advance t Int64.(`Syncd (of_int 2)) |> get_ok in
  let t = print_state t actions in
  let _ = t in
  [%expect
    {|
    (("P.Test.get_node_state t"
      (Leader (match_index ((1 2))) (next_index ((1 1))) (heartbeat 0)))
     (store
      ((data
        ((current_term 1)
         (log
          ((store
            (((command ((op (Read 2)) (id 2))) (term 1))
             ((command ((op (Read 1)) (id 1))) (term 1))))
           (command_set (1 2)) (length 2)))))
       (ops ())))
     (actions ((acts ((CommitIndexUpdate 2))) (nonblock_sync true)))) |}]

let%expect_test "loop triple" =
  let s1 = S.init () in
  let t1 = P.create_node three_config s1 in
  let s2 = S.init () in
  let t2 =
    P.create_node {three_config with other_nodes= [1; 3]; node_id= 2} s2
  in
  let t2, actions = P.advance t2 `Tick |> get_ok in
  let t2 = print_state t2 actions in
  [%expect
    {|
    (("P.Test.get_node_state t"
      (Candidate (quorum ((elts (2)) (n 1) (threshold 2) (eq <fun>)))
       (entries ()) (start_index 1) (timeout 0)))
     (store
      ((data ((current_term 2) (log ((store ()) (command_set ()) (length 0)))))
       (ops ((Term 2)))))
     (actions
      ((acts
        ((Send (1 (RequestVote ((src 2) (term 2) (leader_commit 0)))))
         (Send (3 (RequestVote ((src 2) (term 2) (leader_commit 0)))))))
       (nonblock_sync false)))) |}] ;
  let rv =
    List.find_map_exn actions.acts ~f:(function
      | `Send (dst, rv) when dst = 1 ->
          Some rv
      | _ ->
          None )
  in
  let t1, actions = P.advance t1 (`Recv rv) |> get_ok in
  let t1 = print_state t1 actions in
  [%expect
    {|
    (("P.Test.get_node_state t" (Follower (timeout 0)))
     (store
      ((data ((current_term 2) (log ((store ()) (command_set ()) (length 0)))))
       (ops ((Term 2)))))
     (actions
      ((acts
        ((Send
          (2
           (RequestVoteResponse
            ((src 1) (term 2) (vote_granted true) (entries ()) (start_index 1)))))))
       (nonblock_sync false)))) |}] ;
  let rvr =
    List.find_map_exn actions.acts ~f:(function
      | `Send (id, rvr) when id = 2 ->
          Some rvr
      | _ ->
          None )
  in
  let t2, actions = P.advance t2 (`Recv rvr) |> get_ok in
  let t2 = print_state t2 actions in
  [%expect
    {|
    (("P.Test.get_node_state t"
      (Leader (match_index ((1 0) (2 0) (3 0))) (next_index ((1 1) (2 1) (3 1)))
       (heartbeat 0)))
     (store
      ((data ((current_term 2) (log ((store ()) (command_set ()) (length 0)))))
       (ops ())))
     (actions
      ((acts
        ((Send
          (3
           (AppendEntries
            ((src 2) (term 2) (prev_log_index 0) (prev_log_term 0) (entries ())
             (entries_length 0) (leader_commit 0)))))
         (Send
          (1
           (AppendEntries
            ((src 2) (term 2) (prev_log_index 0) (prev_log_term 0) (entries ())
             (entries_length 0) (leader_commit 0)))))))
       (nonblock_sync true)))) |}] ;
  let t2, actions =
    P.advance t2 (`Commands [cmd_of_int 1; cmd_of_int 2]) |> get_ok
  in
  let t2 = print_state t2 actions in
  [%expect
    {|
    (("P.Test.get_node_state t"
      (Leader (match_index ((1 0) (2 0) (3 0))) (next_index ((1 3) (2 1) (3 3)))
       (heartbeat 0)))
     (store
      ((data
        ((current_term 2)
         (log
          ((store
            (((command ((op (Read 2)) (id 2))) (term 2))
             ((command ((op (Read 1)) (id 1))) (term 2))))
           (command_set (1 2)) (length 2)))))
       (ops
        ((Log (Add ((command ((op (Read 2)) (id 2))) (term 2))))
         (Log (Add ((command ((op (Read 1)) (id 1))) (term 2))))))))
     (actions
      ((acts
        ((Send
          (3
           (AppendEntries
            ((src 2) (term 2) (prev_log_index 0) (prev_log_term 0)
             (entries
              (((command ((op (Read 2)) (id 2))) (term 2))
               ((command ((op (Read 1)) (id 1))) (term 2))))
             (entries_length 2) (leader_commit 0)))))
         (Send
          (1
           (AppendEntries
            ((src 2) (term 2) (prev_log_index 0) (prev_log_term 0)
             (entries
              (((command ((op (Read 2)) (id 2))) (term 2))
               ((command ((op (Read 1)) (id 1))) (term 2))))
             (entries_length 2) (leader_commit 0)))))))
       (nonblock_sync true)))) |}] ;
  let ae =
    List.find_map_exn actions.acts ~f:(function
      | `Send (id, ae) when id = 1 ->
          Some ae
      | _ ->
          None )
  in
  let t2, actions = P.advance t2 (`Syncd (Int64.of_int 2)) |> get_ok in
  let t2 = print_state t2 actions in
  [%expect
    {|
    (("P.Test.get_node_state t"
      (Leader (match_index ((1 0) (2 2) (3 0))) (next_index ((1 3) (2 1) (3 3)))
       (heartbeat 0)))
     (store
      ((data
        ((current_term 2)
         (log
          ((store
            (((command ((op (Read 2)) (id 2))) (term 2))
             ((command ((op (Read 1)) (id 1))) (term 2))))
           (command_set (1 2)) (length 2)))))
       (ops ())))
     (actions ((acts ()) (nonblock_sync true)))) |}] ;
  let t1, actions = P.advance t1 (`Recv ae) |> get_ok in
  let t1 = print_state t1 actions in
  let _ = t1 in
  [%expect
    {|
    (("P.Test.get_node_state t" (Follower (timeout 0)))
     (store
      ((data
        ((current_term 2)
         (log
          ((store
            (((command ((op (Read 2)) (id 2))) (term 2))
             ((command ((op (Read 1)) (id 1))) (term 2))))
           (command_set (1 2)) (length 2)))))
       (ops
        ((Log (Add ((command ((op (Read 2)) (id 2))) (term 2))))
         (Log (Add ((command ((op (Read 1)) (id 1))) (term 2))))))))
     (actions
      ((acts
        ((Send (2 (AppendEntriesResponse ((src 1) (term 2) (success (Ok 2))))))))
       (nonblock_sync false)))) |}] ;
  let aer =
    List.find_map_exn actions.acts ~f:(function
      | `Send (id, aer) when id = 2 ->
          Some aer
      | _ ->
          None )
  in
  (* In case of full update *)
  let () =
    let t2, actions = P.advance t2 (`Recv aer) |> get_ok in
    let _t2 = print_state t2 actions in
    [%expect
      {|
      (("P.Test.get_node_state t"
        (Leader (match_index ((1 2) (2 2) (3 0))) (next_index ((1 3) (2 1) (3 3)))
         (heartbeat 0)))
       (store
        ((data
          ((current_term 2)
           (log
            ((store
              (((command ((op (Read 2)) (id 2))) (term 2))
               ((command ((op (Read 1)) (id 1))) (term 2))))
             (command_set (1 2)) (length 2)))))
         (ops ())))
       (actions ((acts ((CommitIndexUpdate 2))) (nonblock_sync true)))) |}]
  in
  ()

open! Core
open! Async
open! Ocamlpaxos
module P = Paxos_core

let cmd_of_int i =
  Types.Command.{op= Read (Int.to_string i); id= Types.Id.of_int_exn i}

let single_config =
  P.
    { phase1majority= 1
    ; phase2majority= 1
    ; other_nodes= []
    ; num_nodes= 1
    ; node_id= 1
    ; election_timeout= 1 }

let three_config =
  P.
    { phase1majority= 2
    ; phase2majority= 2
    ; other_nodes= [2; 3]
    ; num_nodes= 3
    ; node_id= 1
    ; election_timeout= 1 }

let file_init n =
  let log_path = Fmt.str "%d.log" n in
  let%bind _, Types.Wal.P.{log; term} = Types.Wal.of_path log_path in
  return (log, term)

let pr_err = function
  | Error (`Msg s) ->
      print_endline @@ Fmt.str "Error: %s" s
  | Ok _ ->
      ()

let get_ok = function
  | Error (`Msg s) ->
      raise @@ Invalid_argument s
  | Ok v ->
      v

let print_state (t : P.t) actions =
  print_endline @@ Fmt.str "%a" P.pp_node_state (P.Test.get_node_state t) ;
  [%message (actions : P.action_sequence)]
  |> Sexp.to_string_hum |> print_endline

let%expect_test "transitions" =
  let%bind log, term = file_init 1 in
  print_endline @@ Fmt.str "%d" term ;
  let%bind () = [%expect {| 0 |}] in
  let t = P.create_node three_config log term in
  let () = P.Test.transition_to_leader t |> pr_err in
  let%bind () =
    [%expect
      {| Error: Cannot transition to leader from states other than candidate |}]
  in
  let t, actions = P.Test.transition_to_candidate t |> get_ok in
  print_endline @@ Fmt.str "%d" (P.get_term t) ;
  let%bind () = [%expect {| 1 |}] in
  print_state t actions ;
  let%bind () =
    [%expect
      {|
    Candidate
    (actions
     (((PersistantChange (Term 1))
       (SendRequestVote (2 ((src 1) (term 1) (leader_commit 0))))
       (SendRequestVote (3 ((src 1) (term 1) (leader_commit 0)))))
      false ())) |}]
  in
  let t, actions = P.Test.transition_to_leader t |> get_ok in
  print_state t actions ;
  [%expect
    {|
    Leader
    (actions
     (((SendAppendEntries
        (2
         ((src 1) (term 1) (prev_log_index 0) (prev_log_term 0) (entries ())
          (entries_length 0) (leader_commit 0))))
       (SendAppendEntries
        (3
         ((src 1) (term 1) (prev_log_index 0) (prev_log_term 0) (entries ())
          (entries_length 0) (leader_commit 0)))))
      false ())) |}]

let%expect_test "tick" =
  let%bind log, term = file_init 2 in
  let t = P.create_node three_config log term in
  let t, actions = P.advance t `Tick |> get_ok in
  print_state t actions ;
  let%bind () =
    [%expect
      {|
    Candidate
    (actions
     (((PersistantChange (Term 1))
       (SendRequestVote (2 ((src 1) (term 1) (leader_commit 0))))
       (SendRequestVote (3 ((src 1) (term 1) (leader_commit 0)))))
      false ())) |}]
  in
  let t, _ = P.Test.transition_to_follower t |> get_ok in
  let t, actions = P.advance t `Tick |> get_ok in
  print_state t actions ;
  [%expect {|
    Follower(1)
    (actions (() false ())) |}]

let%expect_test "loop single" =
  let%bind log, term = file_init 3 in
  let t = P.create_node single_config log term in
  let t, actions =
    P.advance t (`Commands [cmd_of_int 1; cmd_of_int 2]) |> get_ok
  in
  print_state t actions ;
  let%bind () =
    [%expect
      {|
    Follower(1)
    (actions
     (((Unapplied (((op (Read 1)) (id 1)) ((op (Read 2)) (id 2))))) false ())) |}]
  in
  let t, actions = P.advance t `Tick |> get_ok in
  print_state t actions ;
  let%bind () =
    [%expect
      {|
    Leader
    (actions (((PersistantChange (Term 1))) false ())) |}]
  in
  let t, actions =
    P.advance t (`Commands [cmd_of_int 1; cmd_of_int 2]) |> get_ok
  in
  (P.get_log t).store |> [%sexp_of: Types.log_entry list] |> Sexp.to_string_hum
  |> print_endline ;
  print_state t actions ;
  [%expect
    {|
    (((command ((op (Read 2)) (id 2))) (term 1))
     ((command ((op (Read 1)) (id 1))) (term 1)))
    Leader
    (actions
     (((PersistantChange (Log (Add ((command ((op (Read 2)) (id 2))) (term 1)))))
       (PersistantChange (Log (Add ((command ((op (Read 1)) (id 1))) (term 1))))))
      true ((CommitIndexUpdate 2)))) |}]

let%expect_test "loop triple" =
  let%bind log, term = file_init 4 in
  let t1 = P.create_node three_config log term in
  let%bind log, term = file_init 5 in
  let t2 =
    P.create_node {three_config with other_nodes= [1; 3]; node_id= 2} log term
  in
  let t2, ((pre, _, _) as actions) = P.advance t2 `Tick |> get_ok in
  print_state t2 actions ;
  let%bind () =
    [%expect
      {|
    Candidate
    (actions
     (((PersistantChange (Term 2))
       (SendRequestVote (1 ((src 2) (term 2) (leader_commit 0))))
       (SendRequestVote (3 ((src 2) (term 2) (leader_commit 0)))))
      false ())) |}]
  in
  let rv =
    List.find_map_exn pre ~f:(function
      | `SendRequestVote (dst, rv) when dst = 1 ->
          Some rv
      | _ ->
          None)
  in
  let t1, ((_, _, post) as actions) =
    P.advance t1 (`RRequestVote rv) |> get_ok
  in
  print_state t1 actions ;
  let%bind () =
    [%expect
      {|
    Follower(0)
    (actions
     (((PersistantChange (Term 2))) true
      ((SendRequestVoteResponse
        (2 ((src 1) (term 2) (vote_granted true) (entries ()) (start_index 1))))))) |}]
  in
  let rvr =
    List.find_map_exn post ~f:(function
      | `SendRequestVoteResponse (id, rvr) when id = 2 ->
          Some rvr
      | _ ->
          None)
  in
  let t2, actions = P.advance t2 (`RRequestVoteResponse rvr) |> get_ok in
  print_state t2 actions ;
  let%bind () =
    [%expect
      {|
    Leader
    (actions
     (((SendAppendEntries
        (1
         ((src 2) (term 2) (prev_log_index 0) (prev_log_term 0) (entries ())
          (entries_length 0) (leader_commit 0))))
       (SendAppendEntries
        (3
         ((src 2) (term 2) (prev_log_index 0) (prev_log_term 0) (entries ())
          (entries_length 0) (leader_commit 0)))))
      false ())) |}]
  in
  let t2, ((pre, _, _) as actions) =
    P.advance t2 (`Commands [cmd_of_int 1; cmd_of_int 2]) |> get_ok
  in
  print_state t2 actions ;
  let%bind () =
    [%expect
      {|
    Leader
    (actions
     (((PersistantChange (Log (Add ((command ((op (Read 2)) (id 2))) (term 2)))))
       (PersistantChange (Log (Add ((command ((op (Read 1)) (id 1))) (term 2)))))
       (SendAppendEntries
        (1
         ((src 2) (term 2) (prev_log_index 0) (prev_log_term 0)
          (entries
           (((command ((op (Read 2)) (id 2))) (term 2))
            ((command ((op (Read 1)) (id 1))) (term 2))))
          (entries_length 2) (leader_commit 0))))
       (SendAppendEntries
        (3
         ((src 2) (term 2) (prev_log_index 0) (prev_log_term 0)
          (entries
           (((command ((op (Read 2)) (id 2))) (term 2))
            ((command ((op (Read 1)) (id 1))) (term 2))))
          (entries_length 2) (leader_commit 0)))))
      false ())) |}]
  in
  let ae =
    List.find_map_exn pre ~f:(function
      | `SendAppendEntries (id, ae) when id = 1 ->
          Some ae
      | _ ->
          None)
  in
  let t1, ((_, _, post) as actions) =
    P.advance t1 (`RAppendEntries ae) |> get_ok
  in
  print_state t1 actions ;
  let%bind () =
    [%expect
      {|
    Follower(0)
    (actions
     (((PersistantChange (Log (Add ((command ((op (Read 1)) (id 1))) (term 2)))))
       (PersistantChange (Log (Add ((command ((op (Read 2)) (id 2))) (term 2))))))
      true ((SendAppendEntriesResponse (2 ((src 1) (term 2) (success (Ok 2)))))))) |}]
  in
  let aer =
    List.find_map_exn post ~f:(function
      | `SendAppendEntriesResponse (id, aer) when id = 2 ->
          Some aer
      | _ ->
          None)
  in
  (* In case of full update *)
  let%bind () =
    let t2, actions = P.advance t2 (`RAppendEntiresResponse aer) |> get_ok in
    print_state t2 actions ;
    [%expect {|
    Leader
    (actions (() true ((CommitIndexUpdate 2)))) |}]
  in
  (* In case of partial update *)
  let%bind () = 
    let aer =
      match aer with
      | {success= Ok v; _} ->
        {aer with success= Ok Int64.(v - one)}
      | _ ->
        assert false
    in
    let t2, actions = P.advance t2 (`RAppendEntiresResponse aer) |> get_ok in
    print_state t2 actions ;
    [%expect {|
      Leader
      (actions
       (((SendAppendEntries
          (1
           ((src 2) (term 2) (prev_log_index 1) (prev_log_term 2)
            (entries (((command ((op (Read 2)) (id 2))) (term 2))))
            (entries_length 1) (leader_commit 0)))))
        true ((CommitIndexUpdate 1))))|}]
  in 
  (* In case of a failed update *)
  let%bind () = 
    let aer =
        {aer with success= Error Int64.(of_int 2)}
    in
    let t2, actions = P.advance t2 (`RAppendEntiresResponse aer) |> get_ok in
    print_state t2 actions ;
    [%expect {|
      Leader
      (actions
       (((SendAppendEntries
          (1
           ((src 2) (term 2) (prev_log_index 1) (prev_log_term 2)
            (entries (((command ((op (Read 2)) (id 2))) (term 2))))
            (entries_length 1) (leader_commit 0)))))
        false ()))|}]
  in 
  Deferred.unit

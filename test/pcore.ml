open! Core
open! Async
open! Ocamlpaxos
module P = Paxos_core

let cmd_of_int i = Types.{op= Read (Int.to_string i); id= Types.Id.of_int_exn i}

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
  let term_path = Fmt.str "%d.term" n in
  let%bind _, log = Types.Log.Wal.of_path log_path in
  let%bind _, term = Types.Term.Wal.of_path term_path in
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
  print_endline @@ Fmt.str "%a" P.pp_node_state t.node_state ;
  print_endline @@ Fmt.str "%a" (Fmt.list ~sep:Fmt.comma P.pp_action) actions

let%expect_test "transitions" =
  let%bind log, term = file_init 1 in
  print_endline @@ Fmt.str "%d" term ;
  let%bind () = [%expect {| 0 |}] in
  let t = P.create_node three_config log term in
  let () = P.transition_to_leader t |> pr_err in
  let%bind () =
    [%expect
      {| Error: Cannot transition to leader from states other than candidate |}]
  in
  let t, actions = P.transition_to_candidate t |> get_ok in
  print_endline @@ Fmt.str "%d" t.current_term ;
  let%bind () = [%expect {| 1 |}] in
  print_state t actions ;
  let%bind () =
    [%expect
      {|
    Candidate
    SendRequestVote to 3, SendRequestVote to 2,
    PersistantChange to Term |}]
  in
  let t, actions = P.transition_to_leader t |> get_ok in
  print_state t actions ;
  [%expect
    {|
    Leader
    SendAppendEntries to 3,
    SendAppendEntries to 2 |}]

let%expect_test "tick" =
  let%bind log, term = file_init 2 in
  let t = P.create_node three_config log term in
  let t, actions = P.advance t `Tick |> get_ok in
  print_state t actions ;
  let%bind () =
    [%expect
      {|
    Candidate
    SendRequestVote to 3, SendRequestVote to 2,
    PersistantChange to Term |}]
  in
  let t = {t with node_state= Follower {heartbeat= 0}} in
  let t, actions = P.advance t `Tick |> get_ok in
  print_endline @@ Fmt.str "%a" P.pp_node_state t.node_state ;
  print_endline @@ Fmt.str "%a" (Fmt.list ~sep:Fmt.comma P.pp_action) actions ;
  [%expect {| Follower(1) |}]

let%expect_test "loop single" =
  let%bind log, term = file_init 3 in
  let t = P.create_node single_config log term in
  let t, actions = P.advance t `Tick |> get_ok in
  print_endline @@ Fmt.str "%a" P.pp_node_state t.node_state ;
  print_endline @@ Fmt.str "%a" (Fmt.list ~sep:Fmt.comma P.pp_action) actions ;
  let%bind () = [%expect {|
    Leader
    PersistantChange to Term |}] in
  let t, actions =
    P.advance t (`Commands [cmd_of_int 1; cmd_of_int 2]) |> get_ok
  in
  t.log.store |> [%sexp_of: Types.log_entry list] |> Sexp.to_string_hum
  |> print_endline ;
  print_endline @@ Fmt.str "%a" (Fmt.list ~sep:Fmt.comma P.pp_action) actions ;
  print_endline @@ Fmt.str "%a" Fmt.int64 t.commit_index ;
  [%expect
    {|
    (((command ((op (Read 2)) (id 2))) (term 1))
     ((command ((op (Read 1)) (id 1))) (term 1)))
    Sync, CommitIndexUpdate to 2, PersistantChange to Log,
    PersistantChange to Log
    2 |}]

let%expect_test "loop triple" =
  let%bind log, term = file_init 4 in
  let t1 = P.create_node three_config log term in
  let%bind log, term = file_init 5 in
  let t2 =
    P.create_node {three_config with other_nodes= [1; 3]; node_id= 2} log term
  in
  let t2, actions = P.advance t2 `Tick |> get_ok in
  print_state t2 actions ;
  let%bind () =
    [%expect
      {|
    Candidate
    SendRequestVote to 3, SendRequestVote to 1,
    PersistantChange to Term |}]
  in
  let rv =
    List.find_map_exn actions ~f:(function
      | `SendRequestVote (id, rv) when id = 1 ->
          Some rv
      | _ ->
          None)
  in
  let t1, actions = P.advance t1 (`RRequestVote (2, rv)) |> get_ok in
  print_state t1 actions ;
  let%bind () =
    [%expect
      {|
    Follower(0)
    Sync, SendRequestVoteResponse to 2,
    PersistantChange to Term |}]
  in
  let rvr =
    List.find_map_exn actions ~f:(function
      | `SendRequestVoteResponse (id, rvr) when id = 2 ->
          Some rvr
      | _ ->
          None)
  in
  let t2, actions = P.advance t2 (`RRequestVoteResponse (1, rvr)) |> get_ok in
  print_state t2 actions ;
  let%bind () =
    [%expect
      {|
    Leader
    SendAppendEntries to 3,
    SendAppendEntries to 1 |}]
  in
  let t2, actions =
    P.advance t2 (`Commands [cmd_of_int 1; cmd_of_int 2]) |> get_ok
  in
  print_state t2 actions ;
  let%bind () =
    [%expect
      {|
    Leader
    SendAppendEntries to 3, SendAppendEntries to 1, PersistantChange to Log,
    PersistantChange to Log |}]
  in
  let ae =
    List.find_map_exn actions ~f:(function
      | `SendAppendEntries (id, ae) when id = 1 ->
          Some ae
      | _ ->
          None)
  in
  let t1, actions = P.advance t1 (`RAppendEntries (2, ae)) |> get_ok in
  print_state t1 actions ;
  let%bind () =
    [%expect
      {|
    Follower(0)
    Sync, SendAppendEntriesResponse to 2, PersistantChange to Log,
    PersistantChange to Log |}]
  in
  let aer =
    List.find_map_exn actions ~f:(function
      | `SendAppendEntriesResponse (id, aer) when id = 2 ->
          Some aer
      | _ ->
          None)
  in
  let t2, actions = P.advance t2 (`RAppendEntiresResponse (1, aer)) |> get_ok in
  print_state t2 actions ; [%expect {|
    Leader
    Sync,
    CommitIndexUpdate to 2 |}]

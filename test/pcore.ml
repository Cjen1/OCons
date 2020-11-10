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
  let%bind _, Types.Wal.P.{log; term} = Types.Wal.of_path_async log_path in
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

let print_state (t : P.t)
    P.{wal= _; commit_idx; unapplied; pre; do_sync= sync; post} =
  print_endline @@ Fmt.str "%a" P.pp_node_state (P.Test.get_node_state t) ;
  let s =
    [%message
      "state:"
        ~pre:(Fmt.str "%a" (Fmt.list ~sep:Fmt.comma P.pp_action) pre : string)
        (commit_idx : int64 option)
        ~unapplied:(unapplied : Types.Id.t list)
        (sync : bool)
        ~post:(Fmt.str "%a" (Fmt.list ~sep:Fmt.comma P.pp_action) post : string)]
  in
  print_endline @@ Sexp.to_string_hum s

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
    (state: (pre  "SendRequestVote to 3,\
                 \nSendRequestVote to 2")
     (commit_idx ()) (unapplied ()) (sync false) (post "")) |}]
  in
  let t, actions = P.Test.transition_to_leader t |> get_ok in
  print_state t actions ;
  [%expect
    {|
    Leader
    (state: (pre  "SendAppendEntries to 3,\
                 \nSendAppendEntries to 2")
     (commit_idx ()) (unapplied ()) (sync false) (post "")) |}]

let%expect_test "tick" =
  let%bind log, term = file_init 2 in
  let t = P.create_node three_config log term in
  let t, actions = P.advance t `Tick |> get_ok in
  print_state t actions ;
  let%bind () =
    [%expect
      {|
    Candidate
    (state: (pre  "SendRequestVote to 3,\
                 \nSendRequestVote to 2")
     (commit_idx ()) (unapplied ()) (sync false) (post "")) |}]
  in
  let t, _ = P.Test.transition_to_follower t |> get_ok in
  let t, actions = P.advance t `Tick |> get_ok in
  print_state t actions ;
  [%expect
    {|
    Follower(1)
    (state: (pre "") (commit_idx ()) (unapplied ()) (sync false) (post "")) |}]

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
    (state: (pre "") (commit_idx ()) (unapplied (1 2)) (sync false) (post "")) |}]
  in
  let t, actions = P.advance t `Tick |> get_ok in
  print_state t actions ;
  let%bind () =
    [%expect
      {|
    Leader
    (state: (pre "") (commit_idx ()) (unapplied ()) (sync false) (post "")) |}]
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
    (state: (pre "") (commit_idx (2)) (unapplied ()) (sync true) (post "")) |}]

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
    (state: (pre  "SendRequestVote to 3,\
                 \nSendRequestVote to 1")
     (commit_idx ()) (unapplied ()) (sync false) (post "")) |}]
  in
  let rv =
    List.find_map_exn actions.pre ~f:(function
      | `SendRequestVote (dst, rv) when dst = 1 ->
          Some rv
      | _ ->
          None)
  in
  let t1, actions = P.advance t1 (`RRequestVote rv) |> get_ok in
  print_state t1 actions ;
  let%bind () =
    [%expect
      {|
    Follower(0)
    (state: (pre "") (commit_idx ()) (unapplied ()) (sync true)
     (post "SendRequestVoteResponse to 2")) |}]
  in
  let rvr =
    List.find_map_exn actions.post ~f:(function
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
    (state: (pre  "SendAppendEntries to 3,\
                 \nSendAppendEntries to 1")
     (commit_idx ()) (unapplied ()) (sync false) (post "")) |}]
  in
  let t2, actions =
    P.advance t2 (`Commands [cmd_of_int 1; cmd_of_int 2]) |> get_ok
  in
  print_state t2 actions ;
  let%bind () =
    [%expect
      {|
    Leader
    (state: (pre  "SendAppendEntries to 1,\
                 \nSendAppendEntries to 3")
     (commit_idx ()) (unapplied ()) (sync false) (post "")) |}]
  in
  let ae =
    List.find_map_exn actions.pre ~f:(function
      | `SendAppendEntries (id, ae) when id = 1 ->
          Some ae
      | _ ->
          None)
  in
  let t1, actions = P.advance t1 (`RAppendEntries ae) |> get_ok in
  print_state t1 actions ;
  let%bind () =
    [%expect
      {|
    Follower(0)
    (state: (pre "") (commit_idx ()) (unapplied ()) (sync true)
     (post "SendAppendEntriesResponse to 2")) |}]
  in
  let aer =
    List.find_map_exn actions.post ~f:(function
      | `SendAppendEntriesResponse (id, aer) when id = 2 ->
          Some aer
      | _ ->
          None)
  in
  let t2, actions = P.advance t2 (`RAppendEntiresResponse aer) |> get_ok in
  print_state t2 actions ;
  [%expect
    {|
    Leader
    (state: (pre "") (commit_idx (2)) (unapplied ()) (sync true) (post "")) |}]

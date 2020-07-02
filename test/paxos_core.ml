open Base 
open Lwt.Infix
module OP = Ocamlpaxos
module P = OP.Paxos_core

let node_state =
  Alcotest.testable P.pp_node_state (fun a b ->
      let open P in
      match (a, b) with
      | Leader _, Leader _ ->
          true
      | Candidate _, Candidate _ ->
          true
      | Follower _, Follower _ ->
          true
      | _ ->
          false)

let action = Alcotest.testable P.pp_action Stdlib.(=)

let single_config =
  P.
    { phase1majority= 1
    ; phase2majority= 1
    ; other_nodes= []
    ; num_nodes= 1
    ; node_id= Int64.one 
    ; election_timeout=1}

let three_config =
  P.
    { phase1majority= 2
    ; phase2majority= 2
    ; other_nodes= [2; 3] |> List.map ~f:Int64.of_int
    ; num_nodes= 3
    ; node_id= Int64.one 
    ; election_timeout = 1}

let file_init switch n =
  let log_file = Fmt.str "%d.log" n in
  let term_file = Fmt.str "%d.term" n in
  OP.Log.of_file log_file
  >>= fun log ->
  OP.Term.of_file term_file
  >>= fun term ->
  Lwt_switch.add_hook (Some switch) (fun () ->
      Lwt.join [OP.Log.close log; OP.Term.close term]
      >>= fun () ->
      Lwt.join [Lwt_unix.unlink log_file; Lwt_unix.unlink term_file]) ;
  Lwt.return (log, term)

let test_transitions switch () =
  file_init switch 1
  >>= fun (log, term) ->
  Alcotest.(check int64) "Check initial term" term.t Int64.zero ;
  let t = P.create_node three_config log term in
  Alcotest.check_raises "Leader transition prevented"
    (Invalid_argument
       "Cannot transition to leader from states other than candidate")
    (fun () -> P.transition_to_leader t |> ignore) ;
  let t, actions = P.transition_to_candidate t in
  Alcotest.(check int64) "Check correct term update" t.current_term.t Int64.one ;
  let () =
    match t.node_state with
    | Candidate _ ->
        ()
    | _ ->
        Alcotest.fail "Incorrect node state"
  in
  let rem =
    List.fold_left
      ~f:(fun send_to item ->
        match item with
        | `SendRequestVote (src, _) when List.mem send_to src ~equal:(Int64.equal) ->
            List.filter ~f:(fun v -> Int64.(v <> src)) send_to
        | _ ->
            Alcotest.fail "Incorrect send requestVote")
      ~init:t.config.other_nodes actions
  in
  Alcotest.(check int) "Number of sends" 0 (List.length rem) ;
  let t, actions = P.transition_to_leader t in
  let rem =
    List.fold_left
      ~f:(fun send_to item ->
        match item with
        | `SendAppendEntries (src, _) when List.mem send_to src ~equal:(Int64.equal) ->
            List.filter ~f:Int64.(fun v -> v <> src) send_to
        | _ ->
            Alcotest.fail "Incorrect send appendEntries")
      ~init:t.config.other_nodes actions
  in
  Alcotest.(check int) "Number of sends" 0 (List.length rem) ;
  let () =
    match t.node_state with
    | Leader _ ->
        ()
    | _ ->
        Alcotest.fail "Not leader after transition"
  in
  Lwt.return_unit

let test_tick switch () =
  file_init switch 2
  >>= fun (log, term) ->
  let t = P.create_node three_config log term in
  let t, actions = P.advance t `Tick in
  let () =
    match t.node_state with
    | Candidate _ ->
        ()
    | _ ->
        Alcotest.fail "Did not transition to candidate on tick"
  in
  let rem =
    List.fold_left
      ~f:(fun send_to item ->
        match item with
        | `SendRequestVote (src, _) when List.mem send_to src ~equal:(Int64.equal) ->
            List.filter ~f:Int64.(fun v -> v <> src) send_to
        | _ ->
            Alcotest.fail "Incorrect send requestVote")
      ~init:t.config.other_nodes actions
  in
  Alcotest.(check int) "Number of sends" 0 (List.length rem) ;
  let t = {t with node_state= Follower {heartbeat= 0}} in
  let t, actions = P.advance t `Tick in
  Alcotest.(check int) "Check empty actions" 0 (List.length actions) ;
  let () =
    match t.node_state with
    | Follower {heartbeat= 1} ->
        ()
    | _ ->
        Alcotest.fail "Incorrect tick state"
  in
  Lwt.return_unit

let test_loop_single switch () =
  file_init switch 3
  >>= fun (log, term) ->
  let t = P.create_node single_config log term in
  let t, actions = P.advance t `Tick in
  let () =
    match t.node_state with
    | Leader _ ->
        ()
    | _ ->
        Alcotest.fail "Not leader after singleton election"
  in
  let () =
    match actions with [] -> () | _ -> Alcotest.fail "Not empty actions"
  in
  let log =
    t.log
    |> OP.Log.add Int64.{term= t.current_term.t; command_id= of_int 1}
    |> OP.Log.add Int64.{term= t.current_term.t; command_id= of_int 2}
  in
  let t = {t with log} in
  let t, actions = P.advance t `LogAddition in
  (* Since singleton then actions should include a commit index update to 2 *)
  Alcotest.(check @@ list action)
    "Actions = [commit index to 2]"
    [`CommitIndexUpdate (Int64.of_int 2)]
    actions ;
  Alcotest.(check bool)
    "t has correct commit index" true
    Int64.(t.commit_index = of_int 2) ;
  Lwt.return_unit

let test_loop_triple switch () =
  file_init switch 11
  >>= fun (log, term) ->
  let t1 = P.create_node three_config log term in
  file_init switch 12
  >>= fun (log, term) ->
  let t2 =
    P.create_node
      { three_config with
        other_nodes= [1; 3] |> List.map ~f:Int64.of_int
      ; node_id= Int64.of_int 2 }
      log term
  in
  let n1 = t1.config.node_id in
  let n2 = t2.config.node_id in
  (*
  file_init switch 13 >>= fun (log, term) ->
  let t3 = P.create_node {three_config with node_id = Int64.of_int 3} log term in
     *)
  let t2, actions = P.advance t2 `Tick in
  let rv1 =
    List.find_map
      ~f:(function
        | `SendRequestVote (id, rv) when Int64.(id = n1) ->
            Some rv
        | `SendRequestVote (id, _rv) ->
          Logs.debug (fun m -> m "%a" Fmt.int64 id);
          None
        | _ ->
            None)
      actions
    |> function
    | Some rv -> rv | None -> Alcotest.fail "Did not have request vote"
  in
  let t1, actions = P.advance t1 (`RRequestVote (n2, rv1)) in
  let rvr =
    List.find_map
      ~f:(function
        | `SendRequestVoteResponse (id, rvr) when Int64.(id = n2) ->
            Some rvr
        | _ ->
            None)
      actions
    |> function
    | Some rvr ->
        rvr
    | None ->
        Alcotest.fail "Did not receive request vote response"
  in
  let () = 
    match t2.node_state with
    | Candidate s ->
      Logs.debug (fun m -> m "Candidate(start Index = %a), rvr(start index = %a" Fmt.int64 s.start_index Fmt.int64 rvr.start_index);
    | _ -> Alcotest.fail "Should be candidate still"
  in
  let t2, _ = P.advance t2 (`RRequestVoteResponse (n1, rvr)) in
  Alcotest.(check string)
    "Leader after election" "Leader"
    (Fmt.str "%a" P.pp_node_state t2.node_state) ;
  let log =
    t2.log
    |> OP.Log.add Int64.{term= t2.current_term.t; command_id= of_int 1}
    |> OP.Log.add Int64.{term= t2.current_term.t; command_id= of_int 2}
  in
  let t2 = {t2 with log} in
  let t2, actions = P.advance t2 `LogAddition in
  Logs.debug (fun m -> m "Log addition: %a" (Fmt.list ~sep:(Fmt.comma) P.pp_action) actions);
  let ae =
    List.find_map
      ~f:(function
          | `SendAppendEntries (id, ae) when Int64.(id = n1) ->
            Some ae
          | _ ->
            None)
      actions
    |> function
    | Some ae ->
      ae
    | None ->
      Alcotest.fail "Did not receive append entries"
  in
  let t1, actions = P.advance t1 (`RAppendEntries (n2, ae)) in
  Logs.debug (fun m -> m "Append entries: %a" (Fmt.list ~sep:(Fmt.comma) P.pp_action) actions);
  let aer =
    List.find_map
      ~f:(function
          | `SendAppendEntriesResponse (id, aer) when Int64.(id = n2) ->
            Some aer
          | _ ->
            None)
      actions
    |> function
    | Some aer ->
      aer
    | None ->
      Alcotest.fail "Did not receive append entries"
  in
  let t2, actions = P.advance t2 (`RAppendEntiresResponse (n1, aer)) in
  Logs.debug (fun m -> m "Append entries response: %a" (Fmt.list ~sep:(Fmt.comma) P.pp_action) actions);
  let () = 
    List.find actions ~f:(function 
        | `CommitIndexUpdate (index) when Int64.(index = of_int 2) -> true
        | _ -> false
      ) |> function
    | Some _ -> ()
    | None -> Alcotest.fail "Did not get commit index update"
  in 
  let t1, actions = P.advance t1 `Tick in
  Alcotest.(check int) "Tick actions = []" 0 (List.length actions);
  let t1, actions = P.advance t1 `Tick in

  Lwt.return_unit

let reporter =
  let open Core in
  let report src level ~over k msgf =
    let k _ = over () ; k () in
    let src = Logs.Src.name src in
    msgf
    @@ fun ?header ?tags:_ fmt ->
    Fmt.kpf k Fmt.stdout
      ("[%a] %a %a @[" ^^ fmt ^^ "@]@.")
      Time.pp (Time.now ())
      Fmt.(styled `Magenta string)
      (Printf.sprintf "%14s" src)
      Logs_fmt.pp_header (level, header)
  in
  {Logs.report}

let () =
  Logs.(set_level (Some Debug)) ;
  Logs.set_reporter reporter ;
  let open Alcotest_lwt in
  Fmt.pr "Starting paxos test" ;
  Lwt_main.run
  @@ run "Paxos_core test"
       [ ( "Basic functionality"
         , [ test_case "transitions" `Quick test_transitions
           ; test_case "tick" `Quick test_tick
           ; test_case "loop singleton" `Quick test_loop_single
           ; test_case "loop triple" `Quick test_loop_triple ] ) ]

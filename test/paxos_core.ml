open Base
open Lwt.Infix
module OP = Ocamlpaxos
module P = OP.Paxos_core

let cmd_of_int i = OP.Types.StateMachine.{id= Int64.of_int i; op= Read ""}

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

let action = Alcotest.testable P.pp_action Stdlib.( = )

let single_config =
  P.
    { phase1majority= 1
    ; phase2majority= 1
    ; other_nodes= []
    ; num_nodes= 1
    ; node_id= Int64.one
    ; election_timeout= 1 }

let three_config =
  P.
    { phase1majority= 2
    ; phase2majority= 2
    ; other_nodes= [2; 3] |> List.map ~f:Int64.of_int
    ; num_nodes= 3
    ; node_id= Int64.one
    ; election_timeout= 1 }

let delete test_dir =
  let open Result in
  match (
    Fpath.of_string test_dir >>= fun path ->
    Bos.OS.Dir.delete ~must_exist:false ~recurse:true path
  ) with
  | _ -> ()

let file_init switch n =
  let log_file = Fmt.str "%d.log" n in
  let term_file = Fmt.str "%d.term" n in
  OP.Log.of_dir log_file
  >>= fun (l_wal, log) ->
  OP.Term.of_dir term_file
  >>= fun (t_wal, term) ->
  Lwt_switch.add_hook (Some switch) (fun () ->
      Lwt.join [OP.Log.close l_wal; OP.Term.close t_wal]
      >>= fun () ->
      delete log_file; delete term_file;
      Lwt.return_unit
      ) ;
  Lwt.return (l_wal, log, t_wal, term)

module C = P.Comp

let dst_equal xs ys ~equal =
  List.fold xs ~init:true ~f:(fun acc v -> acc && List.mem ys v ~equal)


let test_transitions switch () =
  file_init switch 1
  >>= fun (_l_wal, log, _t_wal, term) ->
  Alcotest.(check int64) "Check initial term" term Int64.zero ;
  let t = P.create_node three_config log term in
  Alcotest.check_raises "Leader transition prevented"
    (Invalid_argument
       "Cannot transition to leader from states other than candidate")
    (fun () -> P.transition_to_leader t |> ignore) ;
  let t, actions = C.run @@ P.transition_to_candidate t in
  Alcotest.(check int64) "Check correct term update" t.current_term Int64.one ;
  let () =
    match t.node_state with
    | Candidate _ ->
        ()
    | _ ->
        Alcotest.fail "Incorrect node state"
  in
  (* Check that we have correct send destinations *)
  let send_destinations = List.filter_map actions ~f:(function 
      | `SendRequestVote (src, _) -> Some src
      | _ -> None
    ) in
  if not @@ dst_equal send_destinations t.config.other_nodes ~equal:Int64.equal
  then Alcotest.fail (Fmt.str "Did not send to correct destinations got %a" Fmt.(list ~sep:comma P.pp_action) actions);
  Alcotest.(check int) "Number of sends" 2 (List.length send_destinations) ;
  let t, actions = C.run @@ P.transition_to_leader t in
  let send_destinations = List.filter_map actions ~f:(function 
      | `SendAppendEntries (src, _) -> Some src
      | _ -> None
    ) in
  if not @@ dst_equal send_destinations t.config.other_nodes ~equal:Int64.equal
  then Alcotest.fail (Fmt.str "Did not send to correct destinations got %a" Fmt.(list ~sep:comma P.pp_action) actions);
  Alcotest.(check int) "Number of sends" 2 (List.length send_destinations) ;
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
  >>= fun (_, log, _, term) ->
  let t = P.create_node three_config log term in
  let t, actions = P.advance t `Tick in
  let () =
    match t.node_state with
    | Candidate _ ->
        ()
    | _ ->
        Alcotest.fail "Did not transition to candidate on tick"
  in
  let send_destinations = List.filter_map actions ~f:(function 
      | `SendRequestVote (src, _) -> Some src
      | _ -> None
    ) in
  if not @@ dst_equal send_destinations t.config.other_nodes ~equal:Int64.equal
  then Alcotest.fail (Fmt.str "Did not send to correct destinations got %a" Fmt.(list ~sep:comma P.pp_action) actions);
  Alcotest.(check int) "Number of sends" 2 (List.length send_destinations) ;
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

let filter_persistant ls = List.filter ls ~f:(function
    | `PersistantChange _ -> false
    | _ -> true
  )

let test_loop_single switch () =
  file_init switch 3
  >>= fun (_, log, _, term) ->
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
    match actions with [`PersistantChange (`Term _)] -> () | _ -> Alcotest.fail (Fmt.str "Not empty actions %a" (Fmt.list P.pp_action) actions)
  in
  let t, actions = P.advance t (`LogAddition [cmd_of_int 1; cmd_of_int 2]) in
  (* Since singleton then actions should include a commit index update to 2 *)
  Alcotest.(check @@ list action)
    "Actions = [commit index to 2]"
    [`CommitIndexUpdate (Int64.of_int 2)]
    (filter_persistant actions) ;
  Alcotest.(check bool)
    "t has correct commit index" true
    Int64.(t.commit_index = of_int 2) ;
  Lwt.return_unit

let test_loop_triple switch () =
  file_init switch 11
  >>= fun (_, log, _, term) ->
  let t1 = P.create_node three_config log term in
  file_init switch 12
  >>= fun (_, log, _, term) ->
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
            Logs.debug (fun m -> m "%a" Fmt.int64 id) ;
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
        Logs.debug (fun m ->
            m "Candidate(start Index = %a), rvr(start index = %a" Fmt.int64
              s.start_index Fmt.int64 rvr.start_index)
    | _ ->
        Alcotest.fail "Should be candidate still"
  in
  let t2, _ = P.advance t2 (`RRequestVoteResponse (n1, rvr)) in
  Alcotest.(check string)
    "Leader after election" "Leader"
    (Fmt.str "%a" P.pp_node_state t2.node_state) ;
  let t2, actions = P.advance t2 (`LogAddition [cmd_of_int 1; cmd_of_int 2]) in
  Logs.debug (fun m ->
      m "Log addition: %a" (Fmt.list ~sep:Fmt.comma P.pp_action) actions) ;
  let ae =
    List.find_map
      ~f:(function
        | `SendAppendEntries (id, ae) when Int64.(id = n1) ->
            Some ae
        | _ ->
            None)
      actions
    |> function
    | Some ae -> ae | None -> Alcotest.fail "Did not receive append entries"
  in
  let t1, actions = P.advance t1 (`RAppendEntries (n2, ae)) in
  Logs.debug (fun m ->
      m "Append entries: %a" (Fmt.list ~sep:Fmt.comma P.pp_action) actions) ;
  let aer =
    List.find_map
      ~f:(function
        | `SendAppendEntriesResponse (id, aer) when Int64.(id = n2) ->
            Some aer
        | _ ->
            None)
      actions
    |> function
    | Some aer -> aer | None -> Alcotest.fail "Did not receive append entries"
  in
  let t2, actions = P.advance t2 (`RAppendEntiresResponse (n1, aer)) in
  Logs.debug (fun m ->
      m "Append entries response: %a"
        (Fmt.list ~sep:Fmt.comma P.pp_action)
        actions) ;
  let () =
    List.find actions ~f:(function
      | `CommitIndexUpdate index when Int64.(index = of_int 2) ->
          true
      | _ ->
          false)
    |> function
    | Some _ -> () | None -> Alcotest.fail "Did not get commit index update"
  in
  ignore (t1, t2) ;
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

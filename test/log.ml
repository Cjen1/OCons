open! Core
open! Ocons_core
open Types
module IS = Immutable_store
module PS = Mutable_store.Make (IS)

let uid_of_id id = Uuid.create_random (Random.State.make [|id|])

let make_entry id term key =
  let command =
    Types.Command.{op= Read (Int.to_string key); id= uid_of_id id}
  in
  Types.{command; term}

module IStorageTest = struct
  let init_log istate =
    let ops =
      [(1, 1); (1, 2); (3, 3); (3, 4)]
      |> List.mapi ~f:(fun id (term, key) -> make_entry (id + 1) term key)
    in
    List.fold_left ops ~init:istate ~f:(fun istate entry ->
        IS.add_entry istate ~entry )

  let%expect_test "init_log" =
    let istate = IS.init () in
    istate |> IS.to_string |> print_endline ;
    [%expect
      {|
      (t
       ((data ((current_term 0) (log ((store ()) (command_set ()) (length 0)))))
        (ops ()))) |}] ;
    let istate = init_log istate in
    istate |> IS.to_string |> print_endline ;
    [%expect
      {|
      (t
       ((data
         ((current_term 0)
          (log
           ((store
             (((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
               (term 3))
              ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
               (term 3))
              ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
               (term 1))
              ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
               (term 1))))
            (command_set
             (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
              6c4ac624-e62a-45ee-c2f5-f327ad1a21f6
              da9280e5-0845-4466-e4bb-94e2f401c14a
              eed8f731-aab8-4baf-f515-521eff34be65))
            (length 4)))))
        (ops
         ((Log
           (Add
            ((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
             (term 3))))
          (Log
           (Add
            ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
             (term 3))))
          (Log
           (Add
            ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
             (term 1))))
          (Log
           (Add
            ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
             (term 1)))))))) |}] ;
    istate |> [%sexp_of: IS.t] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      ((data
        ((current_term 0)
         (log
          ((store
            (((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
              (term 3))
             ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
              (term 3))
             ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
              (term 1))
             ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
              (term 1))))
           (command_set
            (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
             6c4ac624-e62a-45ee-c2f5-f327ad1a21f6
             da9280e5-0845-4466-e4bb-94e2f401c14a
             eed8f731-aab8-4baf-f515-521eff34be65))
           (length 4)))))
       (ops
        ((Log
          (Add
           ((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
            (term 3))))
         (Log
          (Add
           ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
            (term 3))))
         (Log
          (Add
           ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
            (term 1))))
         (Log
          (Add
           ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
            (term 1))))))) |}]

  let%expect_test "get_ops" =
    let istate = IS.init () |> init_log in
    IS.get_ops istate |> [%sexp_of: IS.op list] |> Sexp.to_string_hum
    |> print_endline ;
    [%expect
      {|
      ((Log
        (Add
         ((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
          (term 3))))
       (Log
        (Add
         ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
          (term 3))))
       (Log
        (Add
         ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
          (term 1))))
       (Log
        (Add
         ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
          (term 1))))) |}]

  let%expect_test "reset_ops" =
    let istate = IS.init () |> init_log in
    let istate = IS.reset_ops istate in
    istate |> [%sexp_of: IS.t] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      ((data
        ((current_term 0)
         (log
          ((store
            (((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
              (term 3))
             ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
              (term 3))
             ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
              (term 1))
             ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
              (term 1))))
           (command_set
            (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
             6c4ac624-e62a-45ee-c2f5-f327ad1a21f6
             da9280e5-0845-4466-e4bb-94e2f401c14a
             eed8f731-aab8-4baf-f515-521eff34be65))
           (length 4)))))
       (ops ())) |}]

  let%expect_test "update_term" =
    let istate = IS.init () |> init_log in
    istate |> [%sexp_of: IS.t] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      ((data
        ((current_term 0)
         (log
          ((store
            (((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
              (term 3))
             ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
              (term 3))
             ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
              (term 1))
             ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
              (term 1))))
           (command_set
            (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
             6c4ac624-e62a-45ee-c2f5-f327ad1a21f6
             da9280e5-0845-4466-e4bb-94e2f401c14a
             eed8f731-aab8-4baf-f515-521eff34be65))
           (length 4)))))
       (ops
        ((Log
          (Add
           ((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
            (term 3))))
         (Log
          (Add
           ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
            (term 3))))
         (Log
          (Add
           ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
            (term 1))))
         (Log
          (Add
           ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
            (term 1))))))) |}] ;
    let istate = IS.update_term istate ~term:5 in
    istate |> [%sexp_of: IS.t] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      ((data
        ((current_term 5)
         (log
          ((store
            (((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
              (term 3))
             ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
              (term 3))
             ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
              (term 1))
             ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
              (term 1))))
           (command_set
            (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
             6c4ac624-e62a-45ee-c2f5-f327ad1a21f6
             da9280e5-0845-4466-e4bb-94e2f401c14a
             eed8f731-aab8-4baf-f515-521eff34be65))
           (length 4)))))
       (ops
        ((Term 5)
         (Log
          (Add
           ((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
            (term 3))))
         (Log
          (Add
           ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
            (term 3))))
         (Log
          (Add
           ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
            (term 1))))
         (Log
          (Add
           ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
            (term 1))))))) |}]

  let%expect_test "add_entry" =
    let istate = IS.init () in
    istate |> [%sexp_of: IS.t] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      ((data ((current_term 0) (log ((store ()) (command_set ()) (length 0)))))
       (ops ())) |}] ;
    let istate = IS.add_entry istate ~entry:(make_entry 5 4 3) in
    istate |> [%sexp_of: IS.t] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      ((data
        ((current_term 0)
         (log
          ((store
            (((command ((op (Read 3)) (id dbb2a9af-5f11-427b-238c-63c074946538)))
              (term 4))))
           (command_set (dbb2a9af-5f11-427b-238c-63c074946538)) (length 1)))))
       (ops
        ((Log
          (Add
           ((command ((op (Read 3)) (id dbb2a9af-5f11-427b-238c-63c074946538)))
            (term 4))))))) |}]

  let%expect_test "remove_geq" =
    let istate = IS.init () |> init_log in
    [%expect {||}] ;
    let istate = IS.remove_geq istate ~index:Int64.(of_int 3) in
    istate |> [%sexp_of: IS.t] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      ((data
        ((current_term 0)
         (log
          ((store
            (((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
              (term 1))
             ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
              (term 1))))
           (command_set
            (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
             da9280e5-0845-4466-e4bb-94e2f401c14a))
           (length 2)))))
       (ops
        ((Log (RemoveGEQ 3))
         (Log
          (Add
           ((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
            (term 3))))
         (Log
          (Add
           ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
            (term 3))))
         (Log
          (Add
           ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
            (term 1))))
         (Log
          (Add
           ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
            (term 1))))))) |}]

  let%expect_test "get_index" =
    let istate = IS.init () |> init_log in
    IS.get_index istate (Int64.of_int 0)
    |> [%sexp_of: (Types.log_entry, exn) Result.t] |> Sexp.to_string_hum
    |> print_endline ;
    [%expect {| (Error (Not_found_s 0)) |}] ;
    IS.get_index_exn istate (Int64.of_int 1)
    |> [%sexp_of: Types.log_entry] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
       (term 1)) |}] ;
    IS.get_index_exn istate (Int64.of_int 2)
    |> [%sexp_of: Types.log_entry] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
       (term 1)) |}]

  let%expect_test "get_term" =
    let istate = IS.init () |> init_log in
    IS.get_term_exn istate (Int64.of_int 0)
    |> [%sexp_of: Types.term] |> Sexp.to_string_hum |> print_endline ;
    [%expect {| 0 |}] ;
    IS.get_term_exn istate (Int64.of_int 1)
    |> [%sexp_of: Types.term] |> Sexp.to_string_hum |> print_endline ;
    [%expect {| 1 |}] ;
    IS.get_term_exn istate (Int64.of_int 2)
    |> [%sexp_of: Types.term] |> Sexp.to_string_hum |> print_endline ;
    [%expect {| 1 |}] ;
    IS.get_term_exn istate (Int64.of_int 3)
    |> [%sexp_of: Types.term] |> Sexp.to_string_hum |> print_endline ;
    [%expect {| 3 |}] ;
    IS.get_term_exn istate (Int64.of_int 4)
    |> [%sexp_of: Types.term] |> Sexp.to_string_hum |> print_endline ;
    [%expect {| 3 |}]

  let%expect_test "get_max_index" =
    let istate = IS.init () in
    istate |> IS.get_max_index |> Int64.to_string |> print_endline ;
    [%expect {| 0 |}] ;
    let istate = init_log istate in
    istate |> IS.get_max_index |> Int64.to_string |> print_endline ;
    [%expect {| 4 |}]

  let%expect_test "mem_id" =
    let istate = IS.init () in
    IS.mem_id istate (uid_of_id 1) |> Bool.to_string |> print_endline ;
    [%expect {| false |}] ;
    let istate = init_log istate in
    IS.mem_id istate (uid_of_id 1) |> Bool.to_string |> print_endline ;
    [%expect {| true |}]

  let%expect_test "entries_after_inc" =
    let istate = IS.init () |> init_log in
    let entries = IS.entries_after_inc istate Int64.(of_int 2) in
    entries |> [%sexp_of: log_entry list] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      (((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
        (term 3))
       ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
        (term 3))
       ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
        (term 1))) |}]

  let%expect_test "entries_after_inc_size" =
    let istate = IS.init () |> init_log in
    let entries = IS.entries_after_inc_size istate Int64.(of_int 2) in
    entries |> [%sexp_of: log_entry list * int64] |> Sexp.to_string_hum
    |> print_endline ;
    [%expect
      {|
      ((((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
         (term 3))
        ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
         (term 3))
        ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
         (term 1)))
       3) |}]

  let%expect_test "add_cmd" =
    let istate = IS.init () |> init_log in
    let istate =
      IS.add_cmd istate
        ~cmd:Types.Command.{op= Read "cmd1"; id= uid_of_id 11}
        ~term:10
    in
    istate |> [%sexp_of: IS.t] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      ((data
        ((current_term 0)
         (log
          ((store
            (((command
               ((op (Read cmd1)) (id de051608-79c8-4077-8363-33f296ef67e7)))
              (term 10))
             ((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
              (term 3))
             ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
              (term 3))
             ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
              (term 1))
             ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
              (term 1))))
           (command_set
            (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
             6c4ac624-e62a-45ee-c2f5-f327ad1a21f6
             da9280e5-0845-4466-e4bb-94e2f401c14a
             de051608-79c8-4077-8363-33f296ef67e7
             eed8f731-aab8-4baf-f515-521eff34be65))
           (length 5)))))
       (ops
        ((Log
          (Add
           ((command ((op (Read cmd1)) (id de051608-79c8-4077-8363-33f296ef67e7)))
            (term 10))))
         (Log
          (Add
           ((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
            (term 3))))
         (Log
          (Add
           ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
            (term 3))))
         (Log
          (Add
           ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
            (term 1))))
         (Log
          (Add
           ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
            (term 1))))))) |}]

  let%expect_test "add_cmds" =
    let istate = IS.init () |> init_log in
    let istate =
      IS.add_cmds istate
        ~cmds:
          Types.Command.
            [ {op= Read "cmd1"; id= uid_of_id 11}
            ; {op= Read "cmd2"; id= uid_of_id 12} ]
        ~term:10
    in
    istate |> [%sexp_of: IS.t] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      ((data
        ((current_term 0)
         (log
          ((store
            (((command
               ((op (Read cmd2)) (id 279f4de6-d672-44fa-c70b-8750bc0619e3)))
              (term 10))
             ((command
               ((op (Read cmd1)) (id de051608-79c8-4077-8363-33f296ef67e7)))
              (term 10))
             ((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
              (term 3))
             ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
              (term 3))
             ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
              (term 1))
             ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
              (term 1))))
           (command_set
            (279f4de6-d672-44fa-c70b-8750bc0619e3
             63fe4ae3-a440-4e26-7774-3cf575ca5dd0
             6c4ac624-e62a-45ee-c2f5-f327ad1a21f6
             da9280e5-0845-4466-e4bb-94e2f401c14a
             de051608-79c8-4077-8363-33f296ef67e7
             eed8f731-aab8-4baf-f515-521eff34be65))
           (length 6)))))
       (ops
        ((Log
          (Add
           ((command ((op (Read cmd2)) (id 279f4de6-d672-44fa-c70b-8750bc0619e3)))
            (term 10))))
         (Log
          (Add
           ((command ((op (Read cmd1)) (id de051608-79c8-4077-8363-33f296ef67e7)))
            (term 10))))
         (Log
          (Add
           ((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
            (term 3))))
         (Log
          (Add
           ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
            (term 3))))
         (Log
          (Add
           ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
            (term 1))))
         (Log
          (Add
           ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
            (term 1))))))) |}]
end

module MStoreTest = struct
  open! Async

  let with_file f path =
    let%bind s = PS.of_path path in
    let%bind () = f s in
    PS.close s

  let init_log istate =
    let ops =
      [(1, 1); (1, 2); (3, 3); (3, 4)]
      |> List.mapi ~f:(fun id (term, key) -> make_entry (id + 1) term key)
    in
    List.fold_left ops ~init:istate ~f:(fun istate entry ->
        IS.add_entry istate ~entry )

  let%expect_test "end-to-end" =
    let f store =
      let istate = PS.get_state store in
      let entries =
        List.map [(5, 10, 5); (6, 11, 6)] ~f:(fun (a, b, c) -> make_entry a b c)
      in
      let istate = istate |> init_log in
      PS.update store istate |> ignore ;
      let istate = IS.reset_ops istate in
      let%bind idx = PS.datasync store in
      idx |> [%sexp_of: int64] |> Sexp.to_string_hum |> print_endline ;
      let%bind () = [%expect {| 4 |}] in
      let istate' =
        List.fold_left entries ~init:istate ~f:(fun s entry ->
            IS.add_entry s ~entry )
        |> IS.update_term ~term:100
      in
      let ops = List.rev @@ IS.get_ops istate' in
      let istate'' = List.fold_left ops ~f:IS.apply ~init:istate in
      [%message (istate : IS.t) (istate' : IS.t) (istate'' : IS.t)]
      |> Sexp.to_string_hum |> print_endline ;
      let%bind () =
        [%expect
          {|
        ((istate
          ((data
            ((current_term 0)
             (log
              ((store
                (((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
                  (term 3))
                 ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
                  (term 3))
                 ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                  (term 1))
                 ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                  (term 1))))
               (command_set
                (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
                 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6
                 da9280e5-0845-4466-e4bb-94e2f401c14a
                 eed8f731-aab8-4baf-f515-521eff34be65))
               (length 4)))))
           (ops ())))
         (istate'
          ((data
            ((current_term 100)
             (log
              ((store
                (((command ((op (Read 6)) (id f3ff37c6-ef29-4d50-6f65-48022624a717)))
                  (term 11))
                 ((command ((op (Read 5)) (id dbb2a9af-5f11-427b-238c-63c074946538)))
                  (term 10))
                 ((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
                  (term 3))
                 ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
                  (term 3))
                 ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                  (term 1))
                 ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                  (term 1))))
               (command_set
                (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
                 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6
                 da9280e5-0845-4466-e4bb-94e2f401c14a
                 dbb2a9af-5f11-427b-238c-63c074946538
                 eed8f731-aab8-4baf-f515-521eff34be65
                 f3ff37c6-ef29-4d50-6f65-48022624a717))
               (length 6)))))
           (ops
            ((Term 100)
             (Log
              (Add
               ((command ((op (Read 6)) (id f3ff37c6-ef29-4d50-6f65-48022624a717)))
                (term 11))))
             (Log
              (Add
               ((command ((op (Read 5)) (id dbb2a9af-5f11-427b-238c-63c074946538)))
                (term 10))))))))
         (istate''
          ((data
            ((current_term 100)
             (log
              ((store
                (((command ((op (Read 6)) (id f3ff37c6-ef29-4d50-6f65-48022624a717)))
                  (term 11))
                 ((command ((op (Read 5)) (id dbb2a9af-5f11-427b-238c-63c074946538)))
                  (term 10))
                 ((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
                  (term 3))
                 ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
                  (term 3))
                 ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                  (term 1))
                 ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                  (term 1))))
               (command_set
                (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
                 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6
                 da9280e5-0845-4466-e4bb-94e2f401c14a
                 dbb2a9af-5f11-427b-238c-63c074946538
                 eed8f731-aab8-4baf-f515-521eff34be65
                 f3ff37c6-ef29-4d50-6f65-48022624a717))
               (length 6)))))
           (ops ())))) |}]
      in
      [%message
        ( [%compare.equal: IS.data] (IS.get_data istate') (IS.get_data istate'')
          : bool )]
      |> Sexp.to_string_hum |> print_endline ;
      let%bind () =
        [%expect
          {|
        ("([%compare.equal : IS.data]) (IS.get_data istate') (IS.get_data istate'')"
         true) |}]
      in
      PS.update store istate' |> ignore ;
      let%bind idx = PS.datasync store in
      idx |> [%sexp_of: int64] |> Sexp.to_string_hum |> print_endline ;
      [%expect {| 6 |}]
    in
    let%bind () = with_file f "end-to-end" in
    let f' store =
      PS.get_state store |> [%sexp_of: IS.t] |> Sexp.to_string_hum
      |> print_endline ;
      [%expect
        {|
        ((data
          ((current_term 100)
           (log
            ((store
              (((command ((op (Read 6)) (id f3ff37c6-ef29-4d50-6f65-48022624a717)))
                (term 11))
               ((command ((op (Read 5)) (id dbb2a9af-5f11-427b-238c-63c074946538)))
                (term 10))
               ((command ((op (Read 4)) (id eed8f731-aab8-4baf-f515-521eff34be65)))
                (term 3))
               ((command ((op (Read 3)) (id 6c4ac624-e62a-45ee-c2f5-f327ad1a21f6)))
                (term 3))
               ((command ((op (Read 2)) (id da9280e5-0845-4466-e4bb-94e2f401c14a)))
                (term 1))
               ((command ((op (Read 1)) (id 63fe4ae3-a440-4e26-7774-3cf575ca5dd0)))
                (term 1))))
             (command_set
              (63fe4ae3-a440-4e26-7774-3cf575ca5dd0
               6c4ac624-e62a-45ee-c2f5-f327ad1a21f6
               da9280e5-0845-4466-e4bb-94e2f401c14a
               dbb2a9af-5f11-427b-238c-63c074946538
               eed8f731-aab8-4baf-f515-521eff34be65
               f3ff37c6-ef29-4d50-6f65-48022624a717))
             (length 6)))))
         (ops ())) |}]
    in
    with_file f' "end-to-end"
end

open! Core
open! Ocamlpaxos
open Types
module L = Types.ILog
module T = Types.ITerm
module IS = Types.IStorage
module PS = Types.MutableStorage (IS)

let make_entry id term key =
  let id = Types.Id.of_int_exn id in
  let command = Types.Command.{op= Read (Int.to_string key); id} in
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
             (((command ((op (Read 4)) (id 4))) (term 3))
              ((command ((op (Read 3)) (id 3))) (term 3))
              ((command ((op (Read 2)) (id 2))) (term 1))
              ((command ((op (Read 1)) (id 1))) (term 1))))
            (command_set (1 2 3 4)) (length 4)))))
        (ops
         ((Log (Add ((command ((op (Read 4)) (id 4))) (term 3))))
          (Log (Add ((command ((op (Read 3)) (id 3))) (term 3))))
          (Log (Add ((command ((op (Read 2)) (id 2))) (term 1))))
          (Log (Add ((command ((op (Read 1)) (id 1))) (term 1)))))))) |}] ;
    istate |> [%sexp_of: IS.t] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      ((data
        ((current_term 0)
         (log
          ((store
            (((command ((op (Read 4)) (id 4))) (term 3))
             ((command ((op (Read 3)) (id 3))) (term 3))
             ((command ((op (Read 2)) (id 2))) (term 1))
             ((command ((op (Read 1)) (id 1))) (term 1))))
           (command_set (1 2 3 4)) (length 4)))))
       (ops
        ((Log (Add ((command ((op (Read 4)) (id 4))) (term 3))))
         (Log (Add ((command ((op (Read 3)) (id 3))) (term 3))))
         (Log (Add ((command ((op (Read 2)) (id 2))) (term 1))))
         (Log (Add ((command ((op (Read 1)) (id 1))) (term 1))))))) |}]

  let%expect_test "get_ops" =
    let istate = IS.init () |> init_log in
    IS.get_ops istate |> [%sexp_of: IS.op list] |> Sexp.to_string_hum
    |> print_endline ;
    [%expect
      {|
      ((Log (Add ((command ((op (Read 4)) (id 4))) (term 3))))
       (Log (Add ((command ((op (Read 3)) (id 3))) (term 3))))
       (Log (Add ((command ((op (Read 2)) (id 2))) (term 1))))
       (Log (Add ((command ((op (Read 1)) (id 1))) (term 1))))) |}]

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
            (((command ((op (Read 4)) (id 4))) (term 3))
             ((command ((op (Read 3)) (id 3))) (term 3))
             ((command ((op (Read 2)) (id 2))) (term 1))
             ((command ((op (Read 1)) (id 1))) (term 1))))
           (command_set (1 2 3 4)) (length 4)))))
       (ops ())) |}]

  let%expect_test "update_term" =
    let istate = IS.init () |> init_log in
    istate |> [%sexp_of: IS.t] |> Sexp.to_string_hum |> print_endline ;
    let istate = IS.update_term istate ~term:5 in
    istate |> [%sexp_of: IS.t] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      ((data
        ((current_term 0)
         (log
          ((store
            (((command ((op (Read 4)) (id 4))) (term 3))
             ((command ((op (Read 3)) (id 3))) (term 3))
             ((command ((op (Read 2)) (id 2))) (term 1))
             ((command ((op (Read 1)) (id 1))) (term 1))))
           (command_set (1 2 3 4)) (length 4)))))
       (ops
        ((Log (Add ((command ((op (Read 4)) (id 4))) (term 3))))
         (Log (Add ((command ((op (Read 3)) (id 3))) (term 3))))
         (Log (Add ((command ((op (Read 2)) (id 2))) (term 1))))
         (Log (Add ((command ((op (Read 1)) (id 1))) (term 1)))))))
      ((data
        ((current_term 5)
         (log
          ((store
            (((command ((op (Read 4)) (id 4))) (term 3))
             ((command ((op (Read 3)) (id 3))) (term 3))
             ((command ((op (Read 2)) (id 2))) (term 1))
             ((command ((op (Read 1)) (id 1))) (term 1))))
           (command_set (1 2 3 4)) (length 4)))))
       (ops
        ((Term 5) (Log (Add ((command ((op (Read 4)) (id 4))) (term 3))))
         (Log (Add ((command ((op (Read 3)) (id 3))) (term 3))))
         (Log (Add ((command ((op (Read 2)) (id 2))) (term 1))))
         (Log (Add ((command ((op (Read 1)) (id 1))) (term 1))))))) |}]

  let%expect_test "add_entry" =
    let istate = IS.init () in
    istate |> [%sexp_of: IS.t] |> Sexp.to_string_hum |> print_endline ;
    let istate = IS.add_entry istate ~entry:(make_entry 5 4 3) in
    istate |> [%sexp_of: IS.t] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      ((data ((current_term 0) (log ((store ()) (command_set ()) (length 0)))))
       (ops ()))
      ((data
        ((current_term 0)
         (log
          ((store (((command ((op (Read 3)) (id 5))) (term 4)))) (command_set (5))
           (length 1)))))
       (ops ((Log (Add ((command ((op (Read 3)) (id 5))) (term 4))))))) |}]

  let%expect_test "remove_geq" =
    let istate = IS.init () |> init_log in
    let istate = IS.remove_geq istate ~index:Int64.(of_int 3) in
    istate |> [%sexp_of: IS.t] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      ((data
        ((current_term 0)
         (log
          ((store
            (((command ((op (Read 2)) (id 2))) (term 1))
             ((command ((op (Read 1)) (id 1))) (term 1))))
           (command_set (1 2)) (length 2)))))
       (ops
        ((Log (RemoveGEQ 3))
         (Log (Add ((command ((op (Read 4)) (id 4))) (term 3))))
         (Log (Add ((command ((op (Read 3)) (id 3))) (term 3))))
         (Log (Add ((command ((op (Read 2)) (id 2))) (term 1))))
         (Log (Add ((command ((op (Read 1)) (id 1))) (term 1))))))) |}]

  let%expect_test "get_index" =
    let istate = IS.init () |> init_log in
    IS.get_index istate (Int64.of_int 0)
    |> [%sexp_of: (Types.log_entry, exn) Result.t] |> Sexp.to_string_hum
    |> print_endline ;
    [%expect {| (Error (Not_found_s 0)) |}] ;
    IS.get_index_exn istate (Int64.of_int 1)
    |> [%sexp_of: Types.log_entry] |> Sexp.to_string_hum |> print_endline ;
    [%expect {| ((command ((op (Read 1)) (id 1))) (term 1)) |}] ;
    IS.get_index_exn istate (Int64.of_int 2)
    |> [%sexp_of: Types.log_entry] |> Sexp.to_string_hum |> print_endline ;
    [%expect {| ((command ((op (Read 2)) (id 2))) (term 1)) |}]

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
    IS.mem_id istate (Types.Id.of_int_exn 1) |> Bool.to_string |> print_endline ;
    [%expect {| false |}] ;
    let istate = init_log istate in
    IS.mem_id istate (Types.Id.of_int_exn 1) |> Bool.to_string |> print_endline ;
    [%expect {| true |}]

  let%expect_test "entries_after_inc" =
    let istate = IS.init () |> init_log in
    let entries = IS.entries_after_inc istate Int64.(of_int 2) in
    entries |> [%sexp_of: log_entry list] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      (((command ((op (Read 4)) (id 4))) (term 3))
       ((command ((op (Read 3)) (id 3))) (term 3))
       ((command ((op (Read 2)) (id 2))) (term 1))) |}]

  let%expect_test "entries_after_inc_size" =
    let istate = IS.init () |> init_log in
    let entries = IS.entries_after_inc_size istate Int64.(of_int 2) in
    entries |> [%sexp_of: log_entry list * int64] |> Sexp.to_string_hum
    |> print_endline ;
    [%expect
      {|
      ((((command ((op (Read 4)) (id 4))) (term 3))
        ((command ((op (Read 3)) (id 3))) (term 3))
        ((command ((op (Read 2)) (id 2))) (term 1)))
       3) |}]

  let%expect_test "add_entries_rem_conflicts" =
    let istate = IS.init () |> init_log in
    istate |> IS.to_string |> print_endline ;
    [%expect
      {|
      (t
       ((data
         ((current_term 0)
          (log
           ((store
             (((command ((op (Read 4)) (id 4))) (term 3))
              ((command ((op (Read 3)) (id 3))) (term 3))
              ((command ((op (Read 2)) (id 2))) (term 1))
              ((command ((op (Read 1)) (id 1))) (term 1))))
            (command_set (1 2 3 4)) (length 4)))))
        (ops
         ((Log (Add ((command ((op (Read 4)) (id 4))) (term 3))))
          (Log (Add ((command ((op (Read 3)) (id 3))) (term 3))))
          (Log (Add ((command ((op (Read 2)) (id 2))) (term 1))))
          (Log (Add ((command ((op (Read 1)) (id 1))) (term 1)))))))) |}] ;
    let entries =
      List.map [(3, 4, 3); (2, 1, 2)] ~f:(fun (a, b, c) -> make_entry a b c)
    in
    let istate =
      IS.add_entries_remove_conflicts istate ~start_index:(Int64.of_int 2)
        ~entries
    in
    istate |> IS.to_string |> print_endline ;
    [%expect
      {|
      (t
       ((data
         ((current_term 0)
          (log
           ((store
             (((command ((op (Read 3)) (id 3))) (term 4))
              ((command ((op (Read 2)) (id 2))) (term 1))
              ((command ((op (Read 1)) (id 1))) (term 1))))
            (command_set (1 2 3)) (length 3)))))
        (ops
         ((Log (Add ((command ((op (Read 3)) (id 3))) (term 4))))
          (Log (RemoveGEQ 3))
          (Log (Add ((command ((op (Read 4)) (id 4))) (term 3))))
          (Log (Add ((command ((op (Read 3)) (id 3))) (term 3))))
          (Log (Add ((command ((op (Read 2)) (id 2))) (term 1))))
          (Log (Add ((command ((op (Read 1)) (id 1))) (term 1)))))))) |}]

  let%expect_test "add_cmd" =
    let istate = IS.init () |> init_log in
    let istate =
      IS.add_cmd istate
        ~cmd:Types.Command.{op= Read "cmd1"; id= Id.of_int_exn 11}
        ~term:10
    in
    istate |> [%sexp_of: IS.t] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      ((data
        ((current_term 0)
         (log
          ((store
            (((command ((op (Read cmd1)) (id 11))) (term 10))
             ((command ((op (Read 4)) (id 4))) (term 3))
             ((command ((op (Read 3)) (id 3))) (term 3))
             ((command ((op (Read 2)) (id 2))) (term 1))
             ((command ((op (Read 1)) (id 1))) (term 1))))
           (command_set (1 2 3 4 11)) (length 5)))))
       (ops
        ((Log (Add ((command ((op (Read cmd1)) (id 11))) (term 10))))
         (Log (Add ((command ((op (Read 4)) (id 4))) (term 3))))
         (Log (Add ((command ((op (Read 3)) (id 3))) (term 3))))
         (Log (Add ((command ((op (Read 2)) (id 2))) (term 1))))
         (Log (Add ((command ((op (Read 1)) (id 1))) (term 1))))))) |}]

  let%expect_test "add_cmds" =
    let istate = IS.init () |> init_log in
    let istate =
      IS.add_cmds istate
        ~cmds:
          Types.Command.
            [ {op= Read "cmd1"; id= Id.of_int_exn 11}
            ; {op= Read "cmd2"; id= Id.of_int_exn 12} ]
        ~term:10
    in
    istate |> [%sexp_of: IS.t] |> Sexp.to_string_hum |> print_endline ;
    [%expect
      {|
      ((data
        ((current_term 0)
         (log
          ((store
            (((command ((op (Read cmd2)) (id 12))) (term 10))
             ((command ((op (Read cmd1)) (id 11))) (term 10))
             ((command ((op (Read 4)) (id 4))) (term 3))
             ((command ((op (Read 3)) (id 3))) (term 3))
             ((command ((op (Read 2)) (id 2))) (term 1))
             ((command ((op (Read 1)) (id 1))) (term 1))))
           (command_set (1 2 3 4 11 12)) (length 6)))))
       (ops
        ((Log (Add ((command ((op (Read cmd2)) (id 12))) (term 10))))
         (Log (Add ((command ((op (Read cmd1)) (id 11))) (term 10))))
         (Log (Add ((command ((op (Read 4)) (id 4))) (term 3))))
         (Log (Add ((command ((op (Read 3)) (id 3))) (term 3))))
         (Log (Add ((command ((op (Read 2)) (id 2))) (term 1))))
         (Log (Add ((command ((op (Read 1)) (id 1))) (term 1))))))) |}]
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
        List.map [(3, 4, 3); (2, 1, 2)] ~f:(fun (a, b, c) -> make_entry a b c)
      in
      let istate = istate |> init_log in
      PS.update store istate |> ignore ;
      let istate = IS.reset_ops istate in
      let%bind idx = PS.datasync store in
      idx |> [%sexp_of: int64] |> Sexp.to_string_hum |> print_endline ;
      let%bind () = [%expect {| 4 |}] in
      let istate' =
        IS.add_entries_remove_conflicts istate ~start_index:(Int64.of_int 2)
          ~entries
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
                (((command ((op (Read 4)) (id 4))) (term 3))
                 ((command ((op (Read 3)) (id 3))) (term 3))
                 ((command ((op (Read 2)) (id 2))) (term 1))
                 ((command ((op (Read 1)) (id 1))) (term 1))))
               (command_set (1 2 3 4)) (length 4)))))
           (ops ())))
         (istate'
          ((data
            ((current_term 100)
             (log
              ((store
                (((command ((op (Read 3)) (id 3))) (term 4))
                 ((command ((op (Read 2)) (id 2))) (term 1))
                 ((command ((op (Read 1)) (id 1))) (term 1))))
               (command_set (1 2 3)) (length 3)))))
           (ops
            ((Term 100) (Log (Add ((command ((op (Read 3)) (id 3))) (term 4))))
             (Log (RemoveGEQ 3))))))
         (istate''
          ((data
            ((current_term 100)
             (log
              ((store
                (((command ((op (Read 3)) (id 3))) (term 4))
                 ((command ((op (Read 2)) (id 2))) (term 1))
                 ((command ((op (Read 1)) (id 1))) (term 1))))
               (command_set (1 2 3)) (length 3)))))
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
      [%expect {| 3 |}]
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
              (((command ((op (Read 3)) (id 3))) (term 4))
               ((command ((op (Read 2)) (id 2))) (term 1))
               ((command ((op (Read 1)) (id 1))) (term 1))))
             (command_set (1 2 3)) (length 3)))))
         (ops ())) |}]
    in
    with_file f' "end-to-end"
end

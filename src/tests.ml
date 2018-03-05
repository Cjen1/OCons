open OUnit
open Core.List
open Types
open Ballot

(* Tests for ballot equality *)
let ballot_equality_test test_ctx =
  let b = Ballot.Bottom and b' = Ballot.Bottom in
  assert_bool "Foo" (equal b b')

(* Test if a number and a bottom are equal *)
let ballot_equality_test2 test_ctx =
  assert_bool "Foo" (equal (Ballot.Bottom) (Ballot.Number(0,create_id ())))


(*
   Bottom, bottom
   Bottom, number and id

   Same number, same id
   Different number, same id
   same id, different number
   Different number, different id

   Negative ids
*)

let ballot_equality_tests = [
  "..." >:: 
  (fun ctx ->
    equal Bottom Bottom |> assert_bool "Foo");
  
  "..." >:: 
  (fun ctx ->
    equal Bottom (Number(0,create_id())) |> not |> assert_bool "Foo");

  "..." >:: 
  (fun ctx -> let id = create_id () in    
    equal (Number(0,id)) (Number(0,id)) |> assert_bool "Foo");

  "..." >:: 
  (fun ctx ->
    equal (Number(0,create_id())) (Number(0,create_id())) |> not |> assert_bool "Foo");

  "Different number, different id" >:: 
  (fun ctx ->
    equal (Number(0,create_id())) (Number(1,create_id())) |> not |> assert_bool "Foo");
]

(*
   Check that bottom is less than everything

   Check the lexicographic ordering, i.e.
      - Given that for whatever the ids are, the numbers order them ballots
      - Only when the numbers are equal does id matter
*)

(* Helper function generates a pair of unique ids such that
   the first is less than the second *)
let two_ordered_ids () =
  let id1, id2 = create_id (), create_id () in
  if Core.Uuid.compare id1 id2 < 0 
  then id1, id2
  else id2, id1

let ballot_ordering_tests = [
  "Bottom is least ballot" >::
  (fun ctx ->
    let tst b = less_than Bottom b in
    let bs = init 10 ~f:(fun x -> Number(x,create_id())) in 
    for_all bs ~f:tst |> assert_bool "Foo");

  "Check lexicographical ordering of ballots with the same id" >::
  (fun ctx ->
    let id = create_id () in
    (less_than (Number(0,id)) (Number(1,id)) &&
    (less_than (Number(1,id)) (Number(0,id)) |> not)) |> assert_bool "Foo");

  "Check lexicographical ordering of ballots with different ids" >::
  (fun ctx ->
    let id1, id2 = two_ordered_ids () in
    (less_than (Number(0,id1)) (Number(1,id2)) &&
     less_than (Number(1,id1)) (Number(0,id2)) |> not &&
     less_than (Number(0,id2)) (Number(1,id1)) && 
     less_than (Number(1,id2)) (Number(0,id1)) |> not) |> assert_bool "Foo");

  "Check that given same integer, ballots are ordered by their ids" >::
  (fun ctx ->
     let id1, id2 = two_ordered_ids () in
     (less_than (Number(0,id1)) (Number(0,id2)) &&
      less_than (Number(0,id2)) (Number(0,id1)) |> not) |> assert_bool "Foo");

  "Check that comparisons with bottom yield expected integer result" >::
  (fun ctx ->
     let id = create_id () in
     Ballot.compare Bottom (Number(0,id)) |> assert_equal (-1));

  "Check that comparisons with bottom yield expected integer result" >::
  (fun ctx ->
     Ballot.compare Bottom Bottom |> assert_equal 0);

  "Check that comparisons with bottom yield expected integer result" >::
  (fun ctx ->
     let id = create_id () in
     Ballot.compare (Number(0,id)) Bottom |> assert_equal 1);

  "Check comparison is made first based on integer part of ballot" >::
  (fun ctx ->
     let id1, id2 = create_id (), create_id () in
     (((Ballot.compare (Number(0,id1)) (Number(1,id2))) < 0 ) &&
     ((Ballot.compare (Number(0,id2)) (Number(1,id1))) < 0 )) |> assert_bool "Foo");

  "Check comparison is made first based on integer part of ballot" >::
  (fun ctx ->
     let id1, id2 = create_id (), create_id () in
     (((Ballot.compare (Number(1,id1)) (Number(0,id2))) > 0 ) &&
      ((Ballot.compare (Number(1,id2)) (Number(0,id1))) > 0 )) |> assert_bool "Foo"); 

  "Check comparison is made first based on integer part of ballot" >::
  (fun ctx ->
     let id = create_id () in
     ((Ballot.compare (Number(0,id)) (Number(0,id))) = 0 ) |> assert_bool "Foo");
]




let ballot_test_suite = "Ballot test suite" >::: 
                        concat [ballot_equality_tests; ballot_ordering_tests]


(* 
   x << y returns all the elements in y and all the elements of x not in y


   - Strategy: 

   [] << [] == []
   xs << [] == xs
   xs << ys == xs, for-all ys not in xs
*)


(* Tests for operations on proposals *)
open Leader



let check_update xs ys res = (xs << ys) = res

let f () = 
  let id1, id2, id3 = create_id (), create_id (), create_id () in
  [(0,(id1,1,Create(1,"foo")));
   (0,(id2,1,Read(10)));
   (1,(id1,2,Create(2,"bar")));]




let proposal_list_tests = [
  "Test << operator on empty list" >::
  (fun ctx ->
    [] << [] |> assert_equal []);
]







let run_tests = run_test_tt ballot_test_suite

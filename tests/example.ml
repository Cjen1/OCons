open OUnit2

(* An example test *)
let example_test1 text_ctxt =
  assert_equal ~ctxt:text_ctxt "Hello, world!\n" (Foo.hello_world ())
    
(* Gather test(s) up into a test suite *)
let example_suite1 =
  "First example test suite" >::: ["Example test 1" >:: example_test1]

(* Run the example test suite containing one test *)
let () =
  run_test_tt_main example_suite1

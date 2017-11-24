open OUnit2;;
open Core;;

open Client;;

(* Check uniqueness of two client ids *)
let unique_id_client_test test_ctxt =
  let c1_id = Client.getid (Client.new_client ()) in
  let c2_id = Client.getid (Client.new_client ()) in
  let out_string = "Failure implies ids " ^ (Core.Uuid.to_string c1_id) ^ " and " ^ (Core.Uuid.to_string c2_id)  ^ " are equal" in
  assert_bool out_string (not (Core.phys_equal c1_id c2_id));;

(* As an evolution of the first simple test, check uniqeness of 100 clients *)
let unique_id_large_client_test text_ctxt =
  let rec gen_client_ids_list n = match n with
  | 0 -> []
  | n -> Client.getid (Client.new_client ()) :: gen_client_ids_list (n-1) in
  let client_ids = gen_client_ids_list 100 in
  let test_result = (List.find_all_dups client_ids) = [] in
  assert_bool "Failure implies non-uniqueness of client ids: \n"  test_result;;  

(* Gather test(s) up into a test suite *)
let client_test_suite =
  "Tests pertaining to clients and request and response messages" >::: 
  ["Test uniqueness of two client ids" >:: unique_id_client_test;
   "Test uniqueness of 100 client ids" >:: unique_id_large_client_test]

(* Run the example test suite containing one test *)
let () =
  run_test_tt_main client_test_suite

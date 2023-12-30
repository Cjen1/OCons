open Core
open Core_bench

type data = Nil | Node of data

let rec make_data = function 0 -> Nil | n -> Node (make_data (n - 1))

let alloc_test =
  let test `init =
    let large = make_data 1000 in
    let extern = ref None in
    fun () -> extern := Some large
  in
  Bench.Test.create_with_initialization ~name:"alloc_with_reference" test

let () = Command_unix.run (Bench.make_command [alloc_test])

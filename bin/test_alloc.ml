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

let alloc_return_tuple =
  let extern = ref 0 in
  let f x = (x + 1, x + 2, x + 3) in
  let test () =
    let a, b, c = f 1 in
    extern := a + b + c
  in
  Bench.Test.create ~name:"alloc_return_tuple" test

type foo = {mutable a: int; mutable b: int; mutable c: int}

let alloc_return_struct =
  let extern = ref 0 in
  let f x r =
    r.a <- x + 1 ;
    r.b <- x + 2 ;
    r.c <- x + 3
  in
  let test `init =
    let foo = {a= 0; b= 0; c= 0} in
    fun () ->
      f 1 foo ;
      extern := foo.a + foo.b + foo.c
  in
  Bench.Test.create_with_initialization ~name:"alloc_return_struct" test

let () =
  Command_unix.run
    (Bench.make_command [alloc_test; alloc_return_tuple; alloc_return_struct])

open! Core
open! Async
open! Ocamlpaxos
module P = Paxos_core
open Core_bench

let cmd_of_int i = Types.{op= Read (Int.to_string i); id= Types.Id.of_int_exn i}

let single_config =
  P.
    { phase1majority= 1
    ; phase2majority= 1
    ; other_nodes= []
    ; num_nodes= 1
    ; node_id= 1
    ; election_timeout= 1 }

let get_ok = function
  | Error (`Msg s) ->
      raise @@ Invalid_argument s
  | Ok v ->
      v

let empty_lt () =
  let l = Types.Log.L.init () in
  let t = Types.Term.T.init () in
  (l, t)

let empty_core =
  let t =
    let l, t = empty_lt () in
    P.create_node single_config l t
  in
  let t, _ = P.advance t `Tick |> get_ok in
  t

let n = 1000 * 1000

let log_addition batch_size =
  let ops = List.init batch_size ~f:(fun i -> cmd_of_int i) in
  let f () =
    let n = n / batch_size in
    let rec apply core = function
      | 0 ->
          ()
      | n -> (
        match P.advance core (`Commands ops) with
        | Ok (core, _) ->
            apply core (n - 1)
        | Error _ ->
            assert false )
    in
    apply empty_core n
  in
  Bench.Test.create ~name:(Fmt.str "log_addition_%d" batch_size) f

let bench_command =
  let tests = List.map [1; 10; 100; 1000; 10000] ~f:log_addition in
  Bench.make_command tests

let () = Command.run bench_command

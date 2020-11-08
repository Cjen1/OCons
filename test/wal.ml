open! Core
open! Ocamlpaxos
open Owal
open Async

module T_p = struct
  type t = int list

  let init () = []

  type op = Write of int [@@deriving bin_io]

  let apply t (Write i) = i :: t
end

module T = Persistant (T_p)

let with_timeout f =
  let p = f () in
  let timeout = Time.Span.of_sec 1. in
  match%bind Async.with_timeout timeout p with
  | `Result a ->
      return a
  | `Timeout ->
      failwith "timed out"

let%expect_test "persist data" =
  with_timeout
  @@ fun () ->
  let open T in
  let file_size = Int64.of_int 16 in
  let path = "test.wal" in
  let%bind wal, t = of_path ~file_size path in
  [%sexp_of: int list] t |> Sexp.to_string_hum |> print_endline ;
  let%bind () = [%expect {| () |}] in
  let t =
    List.fold_left [1; 2; 3; 4] ~init:t ~f:(fun t i ->
        let op = T_p.Write i in
        write wal op ; T_p.apply t op)
  in
  let%bind () = datasync wal in
  [%sexp_of: int list] t |> Sexp.to_string_hum |> print_endline ;
  let%bind () = [%expect {| (4 3 2 1) |}] in
  let%bind () = close wal in
  let%bind wal, t = of_path ~file_size path in
  [%sexp_of: int list] t |> Sexp.to_string_hum |> print_endline ;
  let%bind () = [%expect {| (4 3 2 1) |}] in
  let%bind () = close wal in
  return ()

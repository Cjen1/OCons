open! Core
open! Async
open Ocamlpaxos
module C = Client

let bytes = Command.Arg_type.create @@ Bytes.of_string

let print_res res = res |> [%sexp_of: Types.op_result] |> Sexp.to_string_hum

let put =
  Command.async_spec ~summary:"Put request"
    Command.Spec.(
      empty
      +> flag "-p" ~doc:"ports list" (listed string)
      +> anon ("key" %: bytes)
      +> anon ("value" %: bytes))
    (fun ps k v ->
      let%bind c = C.new_client ps in
      C.op_write c k v |> [%sexp_of: Types.op_result] |> Sexp.to_string_hum
      |> print_endline)

let get =
  Command.async_spec ~summary:"Get request"
    Command.Spec.(
      empty
      +> flag "-p" ~doc:"ports list" (listed string)
      +> anon ("key" %: bytes))
    (fun ps k v ->
      let%bind c = C.new_client ps in
      C.op_read c k |> [%sexp_of: Types.op_result] |> Sexp.to_string_hum
      |> print_endline)

let reporter =
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

(* Handle the command line arguments and run application is specified mode *)
let cmd =
  Command.group ~summary:"Cli client for Ocaml Paxos"
    [("put", put); ("get", get)]

let () =
  Fmt_tty.setup_std_outputs () ;
  Logs.(set_level (Some Debug)) ;
  Logs.set_reporter reporter ; Core.Command.run cmd

(* main.ml *)
open Core
open Lib.Client
open Lib.State_machine

let write =
  Command.basic ~summary:"Creation operation"
    Command.Let_syntax.(
      let%map_open endpoints = anon ("endpoints" %: string)
      and key = anon ("key" %: string)
      and value = anon ("value" %: string) in
      fun () ->
        let endpoints = String.split ~on:',' endpoints in
        let c = new_client endpoints in
        op_write c key value |> StateMachine.sexp_of_op_result
        |> Sexp.to_string_hum |> print_endline)

let read =
  Command.basic ~summary:"Read operation"
    Command.Let_syntax.(
      let%map_open endpoints = anon ("endpoints" %: string)
      and key = anon ("key" %: string) in
      fun () ->
        let endpoints = String.split ~on:',' endpoints in
        let c = new_client endpoints in
        op_read c key |> StateMachine.sexp_of_op_result |> Sexp.to_string_hum
        |> print_endline)

(* Handle the command line arguments and run application is specified mode *)
let cmd =
  Command.group ~summary:"Cli client for Ocaml Paxos"
    [("write", write); ("read", read)]

let () = Command.run cmd

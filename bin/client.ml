(* main.ml *)
open Core
open Lib.Client
open Lib.State_machine

let write =
  Command.basic ~summary:"Creation operation"
    Command.Let_syntax.(
      let%map_open pub_endpoints = anon ("pub_endpoints" %: string)
      and sub_endpoints = anon ("sub_endpoints" %: string)
      and key = anon ("key" %: string)
      and value = anon ("value" %: string) in
      fun () ->
        let pub_endpoints = String.split ~on:',' pub_endpoints in
        let sub_endpoints = String.split ~on:',' sub_endpoints in
        let c = new_client ~pub_endpoints ~sub_endpoints in
        op_write c key value |> StateMachine.sexp_of_op_result
        |> Sexp.to_string_hum |> print_endline)

let read =
  Command.basic ~summary:"Read operation"
    Command.Let_syntax.(
      let%map_open pub_endpoints = anon ("pub_endpoints" %: string)
      and sub_endpoints = anon ("sub_endpoints" %: string)
      and key = anon ("key" %: string) in
      fun () ->
        let pub_endpoints = String.split ~on:',' pub_endpoints in
        let sub_endpoints = String.split ~on:',' sub_endpoints in
        let c = new_client ~pub_endpoints ~sub_endpoints in
        op_read c key |> StateMachine.sexp_of_op_result |> Sexp.to_string_hum
        |> print_endline)

(* Handle the command line arguments and run application is specified mode *)
let cmd =
  Command.group ~summary:"Cli client for Ocaml Paxos"
    [("write", write); ("read", read)]

let () = Command.run cmd

(* main.ml *)
open Core
open Lib.Client
open Lib.State_machine

let write =
  Command.basic ~summary:"Creation operation"
    Command.Let_syntax.(
      let%map_open rep_endpoints = anon ("rep_endpoints" %: string)
      and req_endpoints = anon ("req_endpoints" %: string)
      and key = anon ("key" %: string)
      and value = anon ("value" %: string) in
      fun () ->
        let rep_endpoints = String.split ~on:',' rep_endpoints in
        let req_endpoints = String.split ~on:',' req_endpoints in
        let c, p = new_client ~cid:"Base" ~rep_endpoints ~req_endpoints () in
        Lwt_main.run
        @@ Lwt.choose
             [ p
             ; (let%lwt res =
                  op_write c key value 
                in res |> StateMachine.sexp_of_op_result
                  |> Sexp.to_string_hum |> print_endline; Lwt.return_unit) ])

let read =
  Command.basic ~summary:"Read operation"
    Command.Let_syntax.(
      let%map_open rep_endpoints = anon ("rep_endpoints" %: string)
      and req_endpoints = anon ("req_endpoints" %: string)
      and key = anon ("key" %: string) in
      fun () ->
        let rep_endpoints = String.split ~on:',' rep_endpoints in
        let req_endpoints = String.split ~on:',' req_endpoints in
        let c, p = new_client ~cid:"Base" ~rep_endpoints ~req_endpoints () in
        Lwt_main.run
        @@ Lwt.choose
             [ p
             ; (let%lwt res =
                  op_read c key 
                in res |> StateMachine.sexp_of_op_result
                  |> Sexp.to_string_hum |> print_endline; Lwt.return_unit) ])

(* Handle the command line arguments and run application is specified mode *)
let cmd =
  Command.group ~summary:"Cli client for Ocaml Paxos"
    [("write", write); ("read", read)]

let () = Command.run cmd

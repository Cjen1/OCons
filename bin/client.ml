(* main.ml *)
open Core
open Lib.Types
open Lib.Client

let nop = 
  Command.basic ~summary:"Nop operation"
  Command.Let_syntax.(
    let%map_open endpoints = anon ("endpoints" %: string) in
    fun () ->
      let c = new_client endpoints in
      op_nop c 
      |> result_to_string
      |> print_endline
  )

let create =
  Command.basic ~summary:"Creation operation"
  Command.Let_syntax.(
    let%map_open endpoints = anon ("endpoints" %: string)
    and key = anon ("key" %: string)
    and value = anon ("value" %: string)
    in
    fun () ->
      let c = new_client endpoints in
      op_create c key value
      |> result_to_string
      |> print_endline
  )

let read =
  Command.basic ~summary:"Read operation"
  Command.Let_syntax.(
    let%map_open endpoints = anon ("endpoints" %: string)
    and key = anon ("key" %: string)
    in
    fun () ->
      let c = new_client endpoints in
      op_read c key 
      |> result_to_string
      |> print_endline
  )

let update =
  Command.basic ~summary:"Update operation"
  Command.Let_syntax.(
    let%map_open endpoints = anon ("endpoints" %: string)
    and key = anon ("key" %: string)
    and value = anon ("value" %: string)
    in
    fun () ->
      let c = new_client endpoints in
      op_update c key value
      |> result_to_string
      |> print_endline
  )

let remove =
  Command.basic ~summary:"Remove operation"
  Command.Let_syntax.(
    let%map_open endpoints = anon ("endpoints" %: string)
    and key = anon ("key" %: string)
    in
    fun () ->
      let c = new_client endpoints in
      op_remove c key
      |> result_to_string
      |> print_endline
  )

(* Handle the command line arguments and run application is specified mode *)
let cmd =
  Command.group ~summary:"Cli client for Ocaml Paxos"
  [ "nop" , nop
  ; "create", create
  ; "read" , read
  ; "update", update
  ; "remove", remove]

let () = Command.run cmd

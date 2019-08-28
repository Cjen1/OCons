(* main.ml *)
open Lib 

open Lwt.Infix
open Core
open Log


(* Sample acceptor code *)
let run_acceptor host port log_dir =
  let log_directory = log_dir ^ "op_acceptor-" ^ host ^ "-" ^ (string_of_int port) in
  Lwt_main.run (
    Logger.initialize_default log_directory >>= fun () ->
    Acceptor.new_acceptor host port)

(* Handle the command line arguments and run application is specified mode *)
let command =
  Command.basic
    ~summary:"Acceptor for Ocaml Paxos"
    Command.Let_syntax.(
      let%map_open
          log_dir = anon("log_directory" %: string)
      in fun () -> 
          run_acceptor "127.0.0.1" 2379 log_dir
    )

let () =
  Command.run command

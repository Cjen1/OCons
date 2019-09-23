(* main.ml *)
open Lib
open Lwt.Infix
open Core
open Log

(* Sample leader code *)
let run_leader host port replica_uris leader_uris acceptor_uris log_dir =
  let log_directory =
    log_dir ^ "op_leader-" ^ host ^ "-" ^ string_of_int port
  in
  Lwt_main.run
    ( Logger.initialize_default log_directory
    >>= fun () ->
    Leader.new_leader host port replica_uris leader_uris acceptor_uris )

(* Handle the command line arguments and run application is specified mode *)
let command =
  Command.basic ~summary:"Leader for Ocaml Paxos"
    Command.Let_syntax.(
      let%map_open endpoints = anon ("endpoints" %: string)
      and log_dir = anon ("log_directory" %: string) in
      fun () ->
        let ips = String.split endpoints ~on:',' in
        let replica_uris =
          List.map ips ~f:(fun ip -> Lib.Message.uri_from_address ip 2381)
        and leader_uris =
          List.map ips ~f:(fun ip -> Lib.Message.uri_from_address ip 2380)
        and acceptor_uris =
          List.map ips ~f:(fun ip -> Lib.Message.uri_from_address ip 2379)
        in
        run_leader "127.0.0.1" 2380 replica_uris leader_uris acceptor_uris
          log_dir)

let () = Command.run command

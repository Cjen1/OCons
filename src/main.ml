(* main.ml *)

open Lwt.Infix
open Unix
open Core
open Client
open Types
open Replica
open Leader.Leader;;

(* Sample replica code *)
let run_replica host port uris =
  Lwt_main.run( Replica.new_replica host port uris );;

(* Sample client code -
   run ten random commands with random parameters, with a random sleep
   time in-between *)
let run_client replica_uri =
  Lwt_main.run (
    Lwt_io.printl "Spinning up a client" >>= fun () ->
    
    let client = Client.new_client () in
    Client.add_replica_uri replica_uri client;
    
    let rec commands n = 
      match n with 
      | 0 -> Lwt.return_unit
      | n ->
        Random.self_init ();
      
        let rand_cmd = (match Random.int 5 with
        | 0 -> Nop
        | 1 -> Create (Random.int 10, string_of_int (Random.int 10))
        | 2 -> Read (Random.int 10)
        | 3 -> Update (Random.int 10, string_of_int (Random.int 10))
        | _ -> Remove (Random.int 10)) in

        (List.hd_exn (Client.send_request_message client rand_cmd)) >>= fun (cid, result) ->

        (match result with
        | Success -> Lwt_io.printl "Success"
        | Failure -> Lwt_io.printl "Failure"
        | ReadSuccess x -> Lwt_io.printl ("Read a value " ^ x)) >>= fun () ->

        Lwt_unix.sleep (float_of_int (Random.int 10)) >>= fun () -> (commands (n-1))
    in
      commands 10
);;


let run_leader host port = 
  Lwt_main.run (
    let (leader, l_wt) = Leader.Leader.new_leader host port in
    Lwt.join [
      l_wt;

      (* Form a sort-of REPL loop for adding new Capnp URIs of replicas to leaders *)
      (let rec new_uri b = match b with _ ->
        (Lwt_io.read_line Lwt_io.stdin >>= fun line ->
         leader.replica_uris <- (Uri.of_string line)::(leader.replica_uris);
         new_uri true
        )
       in new_uri true);
    ]);;

(* Exception to raise if invalid command line arguments are supplied *)
exception InvalidArgs;;

(* Handle the command line arguments and run application is specified mode *)
let command =
  Command.basic
    ~summary:"Foo foo foo foo bar"
    Command.Spec.(
      empty
      +> flag "--node" (optional string) ~doc:"Specify node type (client, replica)"
      +> flag "--host" (optional string) ~doc:"Specify host IP"
      +> flag "--port" (optional string) ~doc:"Specify port number"
      +> flag "--uri" (listed string) ~doc:"Specify URI"
    )
    (fun some_node_string some_host_string some_port_string some_uri_string_list () -> 
       match some_node_string with
       | Some node_string -> 
         (match (String.lowercase node_string) with
          | "replica" ->
            (match some_uri_string_list with
            | [] -> raise InvalidArgs
            | uris ->
              let uris_string = List.map uris ~f:(Uri.of_string) in
              (match (some_host_string, some_port_string) with
              | (Some host_string, Some port_string) ->
                run_replica host_string (int_of_string port_string) uris_string
              | (Some host_string, None) ->
                run_replica host_string 7000 uris_string
              | (None, Some port_string) ->
                run_replica "127.0.0.1" (int_of_string port_string) uris_string
              | (None, None) ->
                run_replica "127.0.0.1" 7000 uris_string ))
          | "leader" ->
            (match (some_host_string, some_port_string) with
              | (Some host_string, Some port_string) ->
                run_leader host_string (int_of_string port_string)
              | (Some host_string, None) ->
                run_leader host_string 7010
              | (None, Some port_string) ->
                run_leader "127.0.0.1" (int_of_string port_string)
              | (None, None) ->
                run_leader "127.0.0.1" 7010 )
          | "client"  ->
            (match some_uri_string_list with
            | [] -> raise InvalidArgs
            | [uri] -> run_client (Uri.of_string uri) 
            | x :: xs -> raise InvalidArgs ) (* Add support for multiple replicas *)
          | _         -> raise InvalidArgs)
       | None -> raise InvalidArgs);;

let () = Command.run command

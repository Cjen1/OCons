(* main.ml *)

open Lwt.Infix
open Unix
open Core
open Client
open Types
open Replica

let run_replica host port leader_uri =
  Lwt_main.run( Replica.new_replica host port [leader_uri] );;

let run_client replica_uri =
  Lwt_main.run (
    Lwt_io.printl "Spinning up a client" >>= fun () ->
    
    let client = Client.add_replica_uri replica_uri (Client.new_client()) in
    
    (List.hd_exn (Client.send_request_message client (Nop))) >>= fun (cid, result) ->
    
    (match result with
    | Success -> Lwt_io.printl "Success"
    | Failure -> Lwt_io.printl "Failure"
    | ReadSuccess x -> Lwt_io.printl ("Read a value " ^ x)) >>= fun () ->

    Lwt_unix.sleep 4.0 >>= fun () ->

    (List.hd_exn (Client.send_request_message client (Create(10,"x"))) >>= fun (cid', result') ->
    
     match result with
    | Success -> Lwt_io.printl "Success"
    | Failure -> Lwt_io.printl "Failure"
    | ReadSuccess x -> Lwt_io.printl ("Read a value " ^ x))
                         
  );;

let run_leader () = 
  Lwt_main.run (
    let (l_ref, l_wt) = Leader.Leader.new_leader "127.0.0.1" 8000 in
    Lwt.join [
      l_wt;

      (* Form a sort-of REPL loop for adding new Capnp URIs of replicas to leaders *)
      (let rec new_uri b = match b with _ ->
        (Lwt_io.read_line Lwt_io.stdin >>= fun line ->
         Lwt.return (Leader.Leader.set_uris !l_ref ((Uri.of_string line)::(Leader.Leader.get_uris !l_ref))) >>= fun _ ->
         new_uri true
        )
       in new_uri true);
    ]);;

exception InvalidArgs;;

let command =
  Command.basic
    ~summary:"Foo foo foo foo bar"
    Command.Spec.(
      empty
      +> flag "--node" (optional string) ~doc:"Specify node type (client, replica)"
      +> flag "--host" (optional string) ~doc:"Specify host IP"
      +> flag "--port" (optional string) ~doc:"Specify port number"
      +> flag "--uri" (optional string) ~doc:"Specify URI"
    )
    (fun some_node_string some_host_string some_port_string some_uri_string () -> 
       match some_node_string with
       | Some node_string -> 
         (match (String.lowercase node_string) with
          | "replica" ->
            (match some_uri_string with
            | Some uri -> 
              (match (some_host_string, some_port_string) with
              | (Some host_string, Some port_string) ->
                run_replica host_string (int_of_string port_string) (Uri.of_string uri)
              | (Some host_string, None) ->
                run_replica host_string 7000 (Uri.of_string uri)
              | (None, Some port_string) ->
                run_replica "127.0.0.1" (int_of_string port_string) (Uri.of_string uri)
              | (None, None) ->
                run_replica "127.0.0.1" 7000 (Uri.of_string uri))
            | None -> raise InvalidArgs)
          | "leader" -> run_leader ()
          | "client"  ->
            (match some_uri_string with
            | Some uri -> run_client (Uri.of_string uri)
            | None -> raise InvalidArgs)
          | _         -> raise InvalidArgs)
       | None -> raise InvalidArgs);;

let () = Command.run command

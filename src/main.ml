(* main.ml *)

open Lwt.Infix
open Unix
open Core
open Client
open Types
open Replica

let run_replica () =
  Lwt_main.run( Replica.new_replica "127.0.0.1" 7000 );;

let run_client replica_uri =
  Lwt_main.run (
    Lwt_io.printl "Spinning up a client" >>= fun () ->
    
    let client = Client.add_replica_uri replica_uri (Client.new_client()) in
    
    (List.hd_exn (Client.send_request_message client (Nop))) >>= fun (cid, result) ->
    
    (match result with
    | Success -> Lwt_io.printl "Success"
    | Failure -> Lwt_io.printl "Failure"
    | ReadSuccess x -> Lwt_io.printl ("Read a value " ^ x)) >>= fun () ->

    (List.hd_exn (Client.send_request_message client (Create(10,"x"))) >>= fun (cid', result') ->
    
     match result with
    | Success -> Lwt_io.printl "Success"
    | Failure -> Lwt_io.printl "Failure"
    | ReadSuccess x -> Lwt_io.printl ("Read a value " ^ x))
                         
  );;

let run_leader replica_uri = 
  Lwt_main.run (Message.send_request (Message.DecisionMessage (0,(Core.Uuid.create (),0,Nop))) replica_uri
  >>= fun response -> match response with
  | Message.ClientRequestResponse(cid,result) ->
    (match result with
    | Success -> Lwt_io.printl "Success"
    | Failure -> Lwt_io.printl "Failure"
    | ReadSuccess x -> Lwt_io.printl ("Read a value " ^ x))
  | Message.DecisionMessageResponse -> Lwt_io.printl "Decision response received!");;

exception InvalidArgs;;

let command =
  Command.basic
    ~summary:"Foo foo foo foo bar"
    Command.Spec.(
      empty
      +> flag "--node" (optional string) ~doc:"Specify node type (client, replica)"
      +> flag "--uri" (optional string) ~doc:"Specify URI"
    )
    (fun some_node_string some_uri_string () -> 
       match some_node_string with
       | Some node_string -> 
         (match (String.lowercase node_string) with
          | "replica" -> run_replica ()
          | "leader"  -> 
            (match some_uri_string with
            | Some uri -> run_leader (Uri.of_string uri)
            | None -> raise InvalidArgs)
          | "client"  ->
            (match some_uri_string with
            | Some uri -> run_client (Uri.of_string uri)
            | None -> raise InvalidArgs)
          | _         -> raise InvalidArgs)
       | None -> raise InvalidArgs);;

let () = Command.run command

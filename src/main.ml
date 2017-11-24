open Lwt.Infix
open Unix
open Core

open Client
open Types
open Replica

(* Example instantiation of a list of replicas *)
(*
let replicas = List.map ["1";"2";"3";"4";"5"] 
  (fun x -> (Replica.new_replica ("127.0.0." ^ x) 7000));;
*)

(*
let client_lwt =
    Replica.new_replica "127.0.0.1" 7000 >>= fun replica_uri ->
    Replica.new_replica "127.0.0.1" 8000 >>= fun replica_uri2 ->    
    Lwt.return (Client.add_replica_uri (replica_uri2) 
      (Client.add_replica_uri (replica_uri) (Client.new_client ())));;

let () = Lwt_main.run (
  client_lwt >>= fun client ->
  Lwt.return (List.iter (Client.send_request_message client Nop) 
                (fun responses -> print_endline "a response again")));;
*)

(*
let () = 
  Lwt_main.run (
  Replica.new_replica "127.0.0.1" 7000 >>= fun replica_uri ->
  let client = Client.add_replica_uri replica_uri (Client.new_client()) in
  (List.hd_exn (Client.send_request_message client (Nop))) >>= fun (cid, result) ->
  match result with
  | Success -> Lwt_io.printl "Success"
  | Failure -> Lwt_io.printl "Failure"
  | ReadSuccess x -> Lwt_io.printl ("Read a value " ^ x));;
*)



let rec sleep_repeat n =
  match n with
  | 0 -> Lwt.return_unit
  | n -> Lwt_io.printl "Test" >>= fun () -> 
         Lwt_unix.sleep 1.0 >>= fun () -> 
         sleep_repeat (n - 1);;
(*
let run_replica () = 
  Lwt_main.run(
    Lwt.join [
      (Replica.new_replica "127.0.0.1" 7000 >>= fun replica_uri -> 
      Lwt_io.printl ("Connecting to: " ^ (Uri.to_string replica_uri)) >>= fun () ->
      fst @@ Lwt.wait () );

      sleep_repeat 10
    ]);;
*)

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
          | "client"  -> 
            (match some_uri_string with
            | Some uri -> run_client (Uri.of_string uri)
            | None -> raise InvalidArgs)
          | _         -> raise InvalidArgs)
       | None -> raise InvalidArgs);;

let () = Command.run command

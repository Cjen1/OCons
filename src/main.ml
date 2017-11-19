open Lwt.Infix
open Unix
open Core

open Client
open Types
open Replica

(* Example instantiation of a list of replicas *)
let replicas = List.map ["1";"2";"3";"4";"5"] 
  (fun x -> (Replica.new_replica ("127.0.0." ^ x) 7000));;

let client_lwt =
    Replica.new_replica "127.0.0.1" 7000 >>= fun replica_uri ->
    Replica.new_replica "127.0.0.1" 8000 >>= fun replica_uri2 ->    
    Lwt.return (Client.add_replica_uri (replica_uri2) 
      (Client.add_replica_uri (replica_uri) (Client.new_client ())));;

let () = Lwt_main.run (
  client_lwt >>= fun client ->
  Lwt.return (List.iter (Client.send_request_message client Nop) 
                (fun responses -> print_endline "a response")));;

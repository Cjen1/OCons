(* main.ml *)

open Lwt.Infix
open Unix
open Core
open Client
open Types
open Replica
open Leader.Leader;;
open Config;;

(* Sample replica code *)
let run_replica' host port uris =
  Lwt_main.run( Replica.new_replica host port uris );;

(* Sample client code -
   run ten random commands with random parameters, with a random sleep
   time in-between *)
let run_client' uris =
  Lwt_main.run (
    Lwt_io.printl "Spinning up a client" >>= fun () ->
    
    let client = Client.new_client () in
    
    List.iter uris ~f:(fun uri ->
        Client.add_replica_uri uri client);
 
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


let run_leader'' host port = 
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

let run_leader' host port uris =
  Lwt_main.run(
    let (leader, l_lwt) = Leader.Leader.new_leader host port in
    Lwt.join [
      l_lwt;
      Lwt.return (leader.replica_uris <- uris)
    ]);;

(* TODO To plug this all in and make it work:
   
      - Modify the functions above so that they accept (host,port)
        pairs instead of URI lists.

      - Modify the actual new_leader / new_replica / etc functions
        so they accept host, port pairs and then establish their
        services based on them.

      - Print some logging.
*)

(* Run this application as a client, serving over the (host,port) address
   under a global configuration given in config *)
let run_client host port config = 
  let replica_uris = List.map config.replica_addrs 
      ~f:(fun (host,port) -> Message.uri_from_address host port) in
  run_client' replica_uris;;

(* Run this application as a replica, serving over the (host,port) address
   under a global configuration given in config *)
let run_replica host port config = 
  (* Get a list of URIs of leaders *)
  let leader_uris = List.map config.leader_addrs 
      ~f:(fun (host,port) -> Message.uri_from_address host port) in
  run_replica' host port leader_uris;;

(* Run this application as a leader, serving over the (host,port) address
   under a global configuration given in config *)
let run_leader host port config = 
  (* Get a list of URIs of replicas *)
  let replica_uris = List.map config.replica_addrs 
      ~f:(fun (host,port) -> Message.uri_from_address host port) in
  run_leader' host port replica_uris;;

(* Function start calls the function specific to running each of the nodes.
   Each one receives the host and port it should serve on and the config
   of the overall system.

   Note that the config is overkill at this point. A client needs only
   know replica addresses and a leader needs only know replica addresses.
   And no node need know the addresses of the nodes of the same type.
*)
let start node host port config =
  match node with
  | "client" ->
    run_client host port config
  | "replica" ->
    run_replica host port config
  | "leader" ->
    run_leader host port config
  | _ -> ();;

let sanitise_node (node_string : string) : string =
  match node_string with
  | "client"  -> node_string
  | "replica" -> node_string
  | "leader"  -> node_string
  | _         -> raise (Invalid_argument "Invalid type of node given");;

let sanitise_port (port_string : string) : int =
  try let port_int = int_of_string port_string in
    if port_int > 1024 && port_int <= 65535 then
      port_int
    else raise (Invalid_argument "Port number must be in range 1025-65535")
  with Failure _ -> raise (Invalid_argument "Port number must be an integer");;

(*
let sanitise_host (host_string : string) : string =
  let regex = Str.regexp 
  "(((2[0-5][0-5])|(1[0-9][0-9])|([0-9][0-9])|[0-9])\.){3}((2[0-5][0-5])|(1[0-9][0-9])|([0-9][0-9])|[0-9])" in
  if Str.string_match regex host_string 0 then
    host_string
  else
    raise (Invalid_argument "Malformed host IP address supplied (must be IPV4)");;
*)

(* TODO: This is just temporary until the library issue with regexps is resolved *)
let sanitise_host host_string = host_string;;

(* TODO: More thorough checks on the exceptions produced and hence
   a more detailed error message *)
let sanitise_config (config_path : string) : Config.addr_info = 
  try Config.read_settings config_path
  with _ -> raise (Invalid_argument "Malformed path / JSON file");;

(* Exception to raise if invalid command line arguments are supplied *)
exception InvalidArgs;;

(* Handle the command line arguments and run application is specified mode *)
let command =
  Command.basic
    ~summary:"Foo foo foo foo bar"
    Command.Spec.(
      empty
      +> flag "--node" (optional string) ~doc:""
      +> flag "--host" (optional string) ~doc:""
      +> flag "--port" (optional string) ~doc:""
      +> flag "--config" (optional string) ~doc:""
    )
    (fun some_node_string some_host_string some_port_string some_config_path () ->
  match some_node_string with
  | None -> raise (Invalid_argument "No node type supplied")
  | Some node_string ->
    match some_config_path with
    | None -> raise (Invalid_argument "No path to config file supplied")
    | Some config_path ->
      match some_host_string, some_port_string with
      | (Some host_string, Some port_string) ->
        start (sanitise_node node_string)
          (sanitise_host host_string)
          (sanitise_port port_string)
          (sanitise_config config_path)
      | (_, _) -> raise (Invalid_argument "Host / port not supplied"));;

let () = Command.run command;;

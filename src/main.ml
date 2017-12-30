(* main.ml *)

open Lwt.Infix;;
open Unix;;
open Core;;

open Types;;
open Config;;
open Client;;
open Replica;;
open Leader;;

open Log

(* Sample client code -

   run ten random commands with random parameters, with a random sleep
   time in-between 
*)
let run_client' host port uris =
  Lwt_main.run (
    Lwt_io.printl "Spinning up a client" >>= fun () ->
    
    Client.new_client host port uris >>= fun client ->
 
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
        
        Client.send_request_message client rand_cmd >>= fun () ->
        Lwt_unix.sleep (float_of_int (Random.int 10)) >>= fun () -> (commands (n-1))
    in
    commands 1 >>= fun () ->
    fst @@ Lwt.wait ()
);;

(* Sample leader code *)
let run_leader' host port replica_uris acceptor_uris =
  let log_directory = "leader-" ^ host ^ "-" ^ (string_of_int port) in
  Lwt_main.run (
    Logger.initialize_default log_directory >>= fun () ->
    Leader.new_leader host port replica_uris acceptor_uris)

(* Sample replica code *)
let run_replica' host port uris =
  let log_directory = "replica-" ^ host ^ "-" ^ (string_of_int port) in
  Lwt_main.run (
    Logger.initialize_default log_directory >>= fun () ->  
    Replica.new_replica host port uris )

(* Sample acceptor code *)
let run_acceptor' host port =
  let log_directory = "acceptor-" ^ host ^ "-" ^ (string_of_int port) in
  Lwt_main.run (
    Logger.initialize_default log_directory >>= fun () ->
    Acceptor.new_acceptor host port)

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
  run_client' host port replica_uris;;

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
  let acceptor_uris = List.map config.acceptor_addrs 
      ~f:(fun (host,port) -> Message.uri_from_address host port) in
  run_leader' host port replica_uris acceptor_uris;;

(* Run this application as an acceptor, serving over the (host,port) address
   under a global configuration given in config *)
let run_acceptor host port config =
  run_acceptor' host port;;       

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
  | "acceptor" ->
    run_acceptor host port config
  | _ -> ();;

let sanitise_node (node_string : string) : string =
  match node_string with
  | "client"  -> node_string
  | "replica" -> node_string
  | "leader"  -> node_string
  | "acceptor" -> node_string
  | _         -> raise (Invalid_argument "Invalid type of node given");;

let sanitise_port (port_string : string) : int =
  try let port_int = int_of_string port_string in
    if port_int > 1024 && port_int <= 65535 then
      port_int
    else raise (Invalid_argument "Port number must be in range 1025-65535")
  with Failure _ -> raise (Invalid_argument "Port number must be an integer");;

(* TODO: Sanitise IP addresses *)
let sanitise_host host_string = host_string;;

(* TODO: More thorough checks on the exceptions produced and hence
   a more deta
   iled error message *)
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
        | (_, _) -> raise (Invalid_argument "Host / port not supplied"))

let () = Command.run command

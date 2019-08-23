(* main.ml *)
open Lib 

open Lwt.Infix
open Core
open Types
open Config
open Log


(* Sample client code -

   run ten random commands with random parameters, with a random sleep
   time in-between 
*)
let run_client' host port uris =
  let log_directory = "client-" ^ host ^ "-" ^ (string_of_int port) in  
  Lwt_main.run begin
    Logger.initialize_default log_directory >>= fun () ->  
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
        
        Client.send_request_message client rand_cmd >>= fun () -> (commands (n-1))
    in
    commands 5 >>= fun () ->
    fst @@ Lwt.wait ()
  end

(* Run the trace client program, sending n Nop commands and measuring the latency in
   receiving a response *)
let run_trace_client' host port uris n =
  let log_directory = "client-" ^ host ^ "-" ^ (string_of_int port) in  
  Lwt_main.run begin
    Logger.initialize_default log_directory >>= fun () ->  
    Lwt_io.printl "Spinning up a client" >>= fun () ->
    let (waiter, _) = Lwt.wait () in    
    Client.new_client host port uris >>= fun client ->
    let rec commands m = 
      match m with
      | 0  -> Lwt.return_unit
      | m' -> Client.send_timed_request client (n + 1 - m') >>= fun () ->
              Lwt_unix.sleep 0.5 >>= fun () ->
              commands (m'-1) 
    in
    commands n >>= fun () ->
    waiter
  end

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

(* Run this application as a client, serving over the (host,port) address
   under a global configuration given in config *)
let run_client host port config = 
  let replica_uris = List.map config.replica_addrs 
      ~f:(fun (host,port) -> Message.uri_from_address host port) in
  run_client' host port replica_uris

(* Run this application as a client, serving over the (host,port) address
   under a global configuration given in config *)
let run_trace_client host port config n = 
  let replica_uris = List.map config.replica_addrs 
      ~f:(fun (host,port) -> Message.uri_from_address host port) in
  run_trace_client' host port replica_uris n

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
let run_acceptor host port _ =
  run_acceptor' host port;;       

(* Function start calls the function specific to running each of the nodes.
   Each one receives the host and port it should serve on and the config
   of the overall system.

   Note that the config is overkill at this point. A client needs only
   know replica addresses and a leader needs only know replica addresses.
   And no node need know the addresses of the nodes of the same type.
*)
let start ?trace node host port config =
  match node with
  | "client" ->
    (match trace with
    | None -> run_client host port config
    | Some n -> run_trace_client host port config n)
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
    Command.Let_syntax.(
      let%map_open
            some_node_string = flag "--node"   (optional string) ~doc:""
        and some_host_string = flag "--host"   (optional string) ~doc:""
        and some_port_string = flag "--port"   (optional string) ~doc:""
        and some_config_path = flag "--config" (optional string) ~doc:""
        and some_trace_num   = flag "--trace"  (optional int)    ~doc:"Please supply an int"
        in
        fun () -> 
        match some_node_string with
        | None -> raise (Invalid_argument "No node type supplied")
        | Some node_string ->
          match some_config_path with
          | None -> raise (Invalid_argument "No path to config file supplied")
          | Some config_path ->
            match some_host_string, some_port_string with
            | (Some host_string, Some port_string) ->
              (match some_trace_num with
              | Some n ->
                start ~trace:n
                      (sanitise_node node_string)
                      (sanitise_host host_string)
                      (sanitise_port port_string)
                      (sanitise_config config_path)
              | None ->
                start (sanitise_node node_string)
                      (sanitise_host host_string)
                      (sanitise_port port_string)
                      (sanitise_config config_path))
            | (_, _) -> raise (Invalid_argument "Host / port not supplied")
    )

let () =
  Command.run command

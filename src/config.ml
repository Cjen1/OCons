open Lwt.Infix;;

(* Type of record that stores the (host,port) pairs of
   each node type in the system.

   This is provided in a config.json file that is read in below and
   is split into groups for clients, replicas and leaders
*)
type addr_info = {
  client_addrs : (string * int) list;
  replica_addrs : (string * int) list;
  leader_addrs : (string * int) list;
  acceptor_addrs : (string * int) list;
};;

(* Read all lines of a file into a single string *)
let rec read_as_string file_str chan =
  Lwt_io.read_line_opt chan >>= function
  | None -> Lwt.return file_str
  | Some line -> read_as_string (file_str ^ line ^ "\n") chan;;

(* Open a file from a given path and read into a string Lwt.t *)
let read_string_lwt (path : Lwt_io.file_name) : string Lwt.t =
  Lwt_io.open_file Lwt_io.Input path >>= fun chan ->
  read_as_string "" chan;;

(* read_settings takes a file path as a string and returns
   the corresponding configuration information of the system *)
let read_settings (path : string) : addr_info =
  let open Yojson.Basic in

  let config_json = from_file path in  
  
  let get_host_port_pairs role_str =
   List.map (fun json ->
      let host = Util.to_string (Util.member "host" json) in
      let port = Util.to_int (Util.member "port" json) in
        (host,port)
  ) (Util.to_list (Util.member role_str config_json)) in
  
  { client_addrs = get_host_port_pairs "clients";
    replica_addrs = get_host_port_pairs "replicas";
    leader_addrs = get_host_port_pairs "leaders";
    acceptor_addrs = get_host_port_pairs "acceptors" };;

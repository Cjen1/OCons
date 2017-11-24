(* replica.ml *)

open Types;;
open Core;;
open Lwt.Infix;;

module Replica = struct
  (* Size of reconfiguration window *)
  let window = 5;;

  (* Type of replicas *)
  type t = {
    (* Id of the replica node *)
    id : Types.replica_id;
    
    (* The state of the application it is replicating *)
    app_state : unit; (* Temporary state of application type *)
    
    (* The slot number for the next empty slot to which the replica
       will propose the next command *)
    slot_in : slot_number;
    
    (* The slot number for the next slot in which a decision needs to be
       made about which command to commit to that slot before the 
       application state can be updated *)
    slot_out : slot_number;
    
    (* Set of request commands sent to the replica *)
    requests : Types.command list;
    
    (* Set of commands proposed by the replica *)
    proposals : Types.proposal list;
    
    (* Set of commands upon which a slot has been decided *)
    decisions : Types.proposal list;
    
    (* Set of leader ids that the client has in its current configuration *)
    leaders : Types.leader_id list; (* Temporary type *)
  };;

  (* Function new_replica returns a new replica given a list of leader ids *)
  let initialize leader_ids (initial_state : unit) = {
    id = Core.Uuid.create ();
    app_state = initial_state;
    slot_in   = 1;
    slot_out  = 1;
    requests  = [];
    proposals = [];
    decisions = [];
    leaders   = leader_ids;
  };;

  (* Getter and setter functions for replica-related information *)

  let get_id replica = match replica with {
    id; app_state; slot_in; slot_out;
    requests; proposals; decisions; leaders } -> id;;

  let get_app_state replica = match replica with {
    id; app_state; slot_in; slot_out;
    requests; proposals; decisions; leaders } -> app_state;;

  let set_app_state replica new_app_state = 
    { replica with app_state = new_app_state };;

  let get_slot_in replica = match replica with {
    id; app_state; slot_in; slot_out;
    requests; proposals; decisions; leaders } -> slot_in;;

  let set_slot_in replica new_slot_in =
    { replica with slot_in = new_slot_in };;

  let get_slot_out replica = match replica with {
    id; app_state; slot_in; slot_out;
    requests; proposals; decisions; leaders } -> slot_out;;

  let set_slot_out replica new_slot_out = 
    { replica with slot_out = new_slot_out };;

  let get_proposals replica = match replica with {
    id; app_state; slot_in; slot_out;
    requests; proposals; decisions; leaders } -> proposals;;

  let set_proposals replica new_proposals =
    { replica with proposals = new_proposals };;
 
  let get_requests replica = match replica with {
    id; app_state; slot_in; slot_out;
    requests; proposals; decisions; leaders } -> requests;;

  let set_requests replica new_requests =
    { replica with requests = new_requests };;

  let get_decisions replica = match replica with {
    id; app_state; slot_in; slot_out;
    requests; proposals; decisions; leaders } -> decisions;;  

  let set_decisions replica new_decisions = 
    { replica with decisions = new_decisions };;

  let get_leaders replica = match replica with {
    id; app_state; slot_in; slot_out;
    requests; proposals; decisions; leaders } -> leaders;;

  let set_leaders replica new_leaders = 
    { replica with leaders = new_leaders };;

  (* Print debug information pertaining to a replica *)
  let print_replica replica =
    let list_of_cmds cmds =
      Lwt.return (List.iter cmds ~f:(fun cmd ->
      Lwt.ignore_result (Lwt_io.print (" " ^ (Types.string_of_command cmd) ^ "\n"))))
    in let list_of_proposals proposals =
      Lwt.return (List.iter proposals ~f:(fun p ->
      Lwt.ignore_result (Lwt_io.print (" " ^ (Types.string_of_proposal p) ^ "\n"))))
    in
    Lwt_io.printl ("Replica id " ^ (Core.Uuid.to_string (get_id replica))) >>= fun () ->
    Lwt_io.printl ("Slot-in: " ^
                   (string_of_int (get_slot_in replica)) ^
                   " | Slot-out: " ^ 
                   (string_of_int (get_slot_out replica))) >>= fun () ->
    Lwt_io.printl "Requests: [" >>= fun () -> 
    list_of_cmds (get_requests replica) >>= fun () ->
    Lwt_io.printl "]" >>= fun () ->
    Lwt_io.printl "Proposals: [" >>= fun () ->
    list_of_proposals (get_proposals replica) >>= fun () ->
    Lwt_io.printl "]" >>= fun () ->
    Lwt_io.printl "Decisions: [" >>= fun () ->
    list_of_proposals (get_decisions replica) >>= fun () ->
    Lwt_io.printl "]";;
  
  (* Print line describing URI to terminal *)
  let print_uri uri =
    Lwt_io.printl ("Spinning up a replica with URI: " ^ (Uri.to_string uri));;

  (* Simple function that maps from requests to results *)
  (* Actual replica implementations will have to propose these
     for sequence slots etc.
     But for now this is just to simulate message passing capability
  *)
  let test (replica : t ref) (cmd : command)  : (command_id * Types.result) =
    (* Add the command to the end of set of requests.
       This is an expensive append operation for now - perhaps change? *)
    replica := set_requests (!replica) (List.append (get_requests !replica) [cmd]);

    (* Do this silly stuff that doesn't need to happen at the moment *)    
    let (client_id, command_id, operation) = cmd in
    let result = match operation with
      | Nop      -> Success
      | Create _ -> Success
      | Read x   -> ReadSuccess("test in replica.ml line 22") (* Test ... *)
      | Update _ -> Success
      | Remove _ -> Success
    in (command_id, result);;

  (* Starts a server that will run the service
     This is mostly Capnproto boilerplate *)
  let start_server (replica_ref : t ref) (host : string) (port : int) =
    let listen_address = `TCP (host, port) in
    let config = Capnp_rpc_unix.Vat_config.create `Ephemeral listen_address in
    let service_id = Capnp_rpc_unix.Vat_config.derived_id config "main" in
    let restore = Capnp_rpc_lwt.Restorer.single service_id (Message.local (test replica_ref)) in
    Capnp_rpc_unix.serve config ~restore >|= fun vat ->
    Capnp_rpc_unix.Vat.sturdy_uri vat service_id;;

  (* Function attempts to take one request and propose it.

     A command can be proposed if its in the set of requests and there is a
     slot free in the configuration window.

     The command may also be a re-config command, in which case a reconfig
     is performed in which a new set of leaders is introduced.
  *)
  let propose replica_ref =
    let slot_in, slot_out = get_slot_in !replica_ref, get_slot_out !replica_ref in
    if (slot_in < slot_out + window) && (not (List.is_empty (get_requests !replica_ref)))
    then
      (* Select a request *)
      let c = List.hd_exn (get_requests !replica_ref) in
      
      (* See if there is a command in the window that is a reconfig *)
      (* If there is then perform a reconfiguration *)

      (* See if there exists a command currently committed for slot_in *)
      (match List.Assoc.find 
              (get_decisions !replica_ref) ~equal:(=) 
              (get_slot_in !replica_ref) with
      | None ->
        (* Remove c from the list of requests *)
        let new_requests = List.filter (get_requests !replica_ref) ~f:(fun c' -> not (Core.phys_equal c c')) in

        (* Add <slot_in,c> to the list of proposals *) 
        let new_proposals = (get_slot_in !replica_ref, c) :: (get_proposals !replica_ref) in

        (* Set the replica to include these updated lists *)
        replica_ref := set_requests (set_proposals !replica_ref new_proposals) new_requests;

        (* Finally broadcast a message to all of the leaders notifying them of proposal *)
        (* ... *)
        (* ... *)
      | Some _ -> 
        (* If there is a command already committed to this slot do nothing *)
        () );

      (* Increment slot_in on the replica *)
      replica_ref := set_slot_in !replica_ref ((get_slot_in !replica_ref) + 1);
      Lwt.return_unit
    else
      Lwt.return_unit;;

  (* Repeatedly call the propose function:
     This is so that as requests arrive we can attempt to propose them *)
  let rec propose_lwt (replica_ref : t ref) =
    propose replica_ref >>= fun () ->
    Lwt_unix.sleep 1.0 >>= fun () ->           (* Put this sleep in for now *)
    print_replica !replica_ref >>= fun () ->   (* Print replica information periodically *)
    propose_lwt replica_ref;;
  
  (* Initialize a new replica and its lwt threads *)
  let new_replica host port =  
    let replica = ref (initialize [] ()) (* Temporary arguments *) in 
    Lwt.join [
      (* Start the server on the specified (host,port) pair.
         Print the URI representing the Capnp sturdy ref
         and then wait forever *)
      (start_server replica host port >>= 
      fun uri -> print_uri uri >>=
      fun () -> fst @@ Lwt.wait ());
      
      (* Start the proposal lwt thread *)
      propose_lwt replica
    ];;
end;;

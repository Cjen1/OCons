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
    app_state : Types.app_state; (* Temporary state of application type *)
    
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
    leaders : Uri.t list;
  };;

  (* Function new_replica returns a new replica given a list of leader ids *)
  let initialize leader_ids = {
    id = Core.Uuid.create ();
    app_state = Types.initial_state;
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

  let list_of_cmds cmds =
    Lwt.return (List.iter cmds ~f:(fun cmd ->
    Lwt.ignore_result (Lwt_io.print (" " ^ (Types.string_of_command cmd) ^ "\n"))));;
    
  let list_of_proposals proposals =
    Lwt.return (List.iter proposals ~f:(fun p ->
    Lwt.ignore_result (Lwt_io.print (" " ^ (Types.string_of_proposal p) ^ "\n"))));;


  let print_replica replica =
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









(*--------------------------------------------------------------------------*)
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
    (* In reality this needs to block here until a decision has been committed
       we can return 
    
       OR ALTERNATIVELY split client request/response into two separate
       message schemas *)
    let (client_id, command_id, operation) = cmd in
    let result = match operation with
      | Nop      -> Success
      | Create _ -> Success
      | Read x   -> ReadSuccess("test in replica.ml line 22") (* Test ... *)
      | Update _ -> Success
      | Remove _ -> Success
    in (command_id, result);;

  (* We won't yet worry about reconfigurations *)
  let isreconfig op = false;;

  (* Stub perform function.
     At the moment we only want to check that each command is performed in
     same order at all replicas. A print statement is enough to show this *)
  let perform replica_ref c =
    let slot_out = get_slot_out !replica_ref in
    let (_,_,op) = c in
    let decisions_inv = List.Assoc.inverse (get_decisions !replica_ref) in

    let is_lower_slot =
    (match List.Assoc.find decisions_inv ~equal:(fun c1 -> fun c2 -> Types.commands_equal c1 c2) c with
    | None -> false
    | Some s -> s < slot_out) in
    if is_lower_slot || isreconfig op  then
      replica_ref := set_slot_out !replica_ref (slot_out + 1) 
    else 
      (* ATOMIC *)
      
      (* Update application state *)
      let next_state, results = Types.apply (get_app_state !replica_ref) op in
      replica_ref := set_app_state !replica_ref (next_state);
      replica_ref := set_slot_out !replica_ref (slot_out + 1)
      (* END ATOMIC *)

      (* Send a response message to client *)
      
  let rec try_execute (replica_ref : t ref) (p : proposal) =
    (* Find a decision corresponding to <slot_out, _>
       Such decisions are possibly ready to have their commands applied
       (given there are no other commands proposed for slot_out) *)
    let slot_out = get_slot_out !replica_ref in 
    match List.Assoc.find (get_decisions !replica_ref) ~equal:(=) slot_out with
    | None -> ()
    | Some c' ->  (* A command c' is possibly ready *)
      Lwt.ignore_result (Lwt_io.printl ("There is a command " ^ (Types.string_of_command c') ^ " in slot " ^ (string_of_int slot_out)));
      
      (* Check next if there is another command c'' that this replica has
         proposed for slot_out. *)
      (match List.Assoc.find (get_proposals !replica_ref) ~equal:(=) slot_out with
       | None -> ()
       | Some c'' ->
          Lwt.ignore_result (Lwt_io.printl "Remove the proposal from slot");
      
          (* Remove the proposal <slot_out, c''> from set of proposals *)
          let new_proposals = List.filter (get_proposals !replica_ref) 
              ~f:(fun p' -> 
                  Lwt.ignore_result (Lwt_io.printl ("Comparing " ^ (Types.string_of_proposal (slot_out, c'')) ^ " and " ^ (Types.string_of_proposal p')));
                  not (Types.proposals_equal (slot_out, c'') p')
              ) in

          Lwt.ignore_result (list_of_proposals new_proposals);
          replica_ref := set_proposals !replica_ref new_proposals;
          Lwt.ignore_result (print_replica !replica_ref );
          

          (* If c' = c'' then we don't need to re-propose this command,
             since we're about to commit <slot_out, c'> anyway.
        
             However, if c' =/= c'' then we add c'' back into the set of
             requests so that it can be proposed again at a later time
             by this replica *)
          if not (Types.commands_equal c' c'') then
            (* Add c'' to requests *)
            replica_ref := set_requests (!replica_ref) (List.append (get_requests !replica_ref) [c''])
          else ()); (* Do nothing *)

      (* Perform the operation of command c' on the application state *)
      perform replica_ref c';
      
      try_execute replica_ref p;;

  (* Another test function for decision receipt *)
  (* This is where application state will be updated if decisions are received
     etc *)
  let test2 (replica_ref : t ref) (p : proposal ) : unit =
    Lwt.ignore_result (Lwt_io.printl ("Received a decision " ^ (Types.string_of_proposal p)));
    Lwt.ignore_result (print_replica !replica_ref );

    (* Append the received proposal to the set of decisions *)
    replica_ref := set_decisions (!replica_ref) (List.append (get_decisions !replica_ref) [p]);

    Lwt.ignore_result (Lwt_io.printl ("Add decision to decisions " ^ (Types.string_of_proposal p)));
    Lwt.ignore_result (print_replica !replica_ref );
  
    (* Try to execute commands ... *)
    try_execute replica_ref p;;
  


(*--------------------------------------------------------------------------*)













  (* Starts a server that will run the service
     This is mostly Capnproto boilerplate *)
  let start_server (replica_ref : t ref) (host : string) (port : int) =
    let listen_address = `TCP (host, port) in
    let config = Capnp_rpc_unix.Vat_config.create ~serve_tls:false ~secret_key:`Ephemeral listen_address  in
    let service_id = Capnp_rpc_unix.Vat_config.derived_id config "main" in
    let restore = Capnp_rpc_lwt.Restorer.single service_id 
        (Message.local (Some (test replica_ref)) (Some (test2 replica_ref))) in
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
        List.iter (get_leaders !replica_ref) ~f:(fun uri ->
          let msg = Message.ProposalMessage (slot_in, c) in  
          (Message.send_request msg uri >>=
            function Message.ProposalMessageResponse -> Lwt.return_unit
                   | _ -> raise Message.Invalid_response)
          |> Lwt.ignore_result);
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
    Lwt_unix.sleep 1.0 >>=  fun () ->           (* Put this sleep in for now *)
    print_replica !replica_ref >>= fun () ->    (* Print replica information periodically *)
    Lwt_io.printl (Types.string_of_state (get_app_state !replica_ref)) >>= fun () ->
    propose_lwt replica_ref;;
  
  (* Initialize a new replica and its lwt threads *)
  let new_replica host port leader_uris =  
    let replica = ref (initialize leader_uris) in 
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

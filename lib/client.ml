(* client.ml *)

open Core
open Core.Unix
open Lwt.Infix
open Types
open Message
open Log.Logger

(* Type of clients *)
type t = {
  id : client_id;
  mutable next_command_id : command_id;
  mutable replica_uri_list : Uri.t list;
}

(* Create a new record of client information *)
let initialize client_uri replica_uris = {
  id = (Uuid_unix.create (), client_uri);
  next_command_id = 1;
  replica_uri_list = replica_uris;
}

(* This is all rough-stuff for measuring latency.
   Will get its own module soon... *)
type measurement = Empty
                 | Request of Core.Time.t
                 | Responded
let request_no = 1001
let request_times = Core.Array.create ~len:request_no Empty
let latency_of_times t0 t1 =
  string_of_float (Core.Time.Span.to_ms (Core.Time.abs_diff t0 t1))
let bandwidth_of_times t0 t1 n =
  string_of_float ((float_of_int n) /. (Core.Time.Span.to_sec (Core.Time.abs_diff t0 t1)))

let t0 = ref @@ Core.Time.now ()

(* ... *)
let result_callback (response : Types.command_id * Types.result) =
  let (cid, result) = response in
  Lwt_main.run (
    match Core.Array.get request_times cid with
  | Empty -> failwith "We shouldn't reach here"
  | Request t ->
      let%lwt () = write_to_log WARN ("Receive first response " ^ (string_of_int cid)) in
      let t1 = Core.Time.now () in
      let%lwt () = write_to_log TRACE (latency_of_times t t1) in
      let%lwt () = write_to_log INFO (bandwidth_of_times !t0 t1 cid) in
      Lwt.return (Core.Array.set request_times cid Responded)
  | Responded -> Lwt.return ()
  )

(* Create a new client record and start a server for such a client *)
let new_client host port replica_uris  =
  Message.start_new_server ~response_callback:result_callback  host port >>= fun uri ->
  Lwt.return (initialize uri replica_uris)

(* Send a clientrequestmessage RPC of a given operation by a given client *)
let send_request_message client operation =
  let command_id = client.next_command_id in
  (* Increment the command id *)
  client.next_command_id <- client.next_command_id + 1;
  (* Iterate over the replica URIs and send a client request message to each, with the same
     command *)
  Lwt_list.iter_p (fun uri ->
    let message = (ClientRequestMessage(client.id,command_id,operation)) in
    Message.send_request message uri) client.replica_uri_list

(* Send a timed request message with the timed response callback *)
(* The latency measurement stuff should all be wrapped up in its own module soon *)
let send_timed_request client cid =
  (if cid=1 then
     t0 := Core.Time.now () );
  let%lwt () = send_request_message client Types.Nop in
  let%lwt () = write_to_log WARN ("Sending message " ^ (string_of_int cid)) in 
  Core.Array.set request_times cid (Request (Core.Time.now ())) |> Lwt.return

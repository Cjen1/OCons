open Base
open Types
open Messaging

let client = Logs.Src.create "Client" ~doc:"Client module"

module CLog = (val Logs.src_log client : Logs.LOG)

type t = {cid: string; endpoints: Send.client_serv list}

let send t op =
  let id = Random.int32 Int32.max_value |> Int32.to_int_exn in 
  let cmd : command = {op; id} in
  let ps = List.map t.endpoints ~f:(fun cap -> Send.clientReq cap cmd) in
  match%lwt Lwt.pick ps with
  | StateMachine.Success -> Lwt.return_ok `Success
  | StateMachine.ReadSuccess v -> Lwt.return_ok (`ReadSuccess v)
  | StateMachine.Failure -> Lwt.return_error (`Msg "Application failed on cluster")

(* Return first result, cancel all others *)

let op_read t k = 
  try%lwt
      send t @@ StateMachine.Read k
  with _e -> `Msg "Failed" |> Lwt.return_error

let op_write t k v = 
  try%lwt
      send t @@ StateMachine.Write (k,v)
  with _e -> `Msg "Failed" |> Lwt.return_error

let new_client ?(cid = Types.create_id ()) ~client_files () =
  let vat = Capnp_rpc_unix.client_only_vat () in
  let%lwt endpoints =
    Lwt_list.map_p
      (fun path ->
        let%lwt sr = Send.get_sr_from_path path vat in
        let%lwt cap = Send.connect sr in
        Lwt.return cap)
      client_files
  in
  let t = {cid= Int.to_string cid; endpoints} in
  Lwt.return t

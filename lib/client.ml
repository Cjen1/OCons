open Utils

type t = {leader_uris: Lwt_unix.sockaddr list}

let new_client endpoints =
  { leader_uris=
      endpoints |> Base.String.split ~on:','
      |> Base.List.map ~f:Utils.uri_of_string }

let serialise_request operation =
  let cid = Types.create_id () in
  let (c : Messaging.client_request) = {command= (cid, operation)} in
  c |> Protobuf.Encoder.encode_exn Messaging.client_request_to_protobuf

let deserialise_response response =
  ( response |> Bytes.of_string
  |> Protobuf.Decoder.decode_exn Messaging.client_response_from_protobuf )
    .result

let send_to_all_replicas t msg =
  let msg = serialise_request msg in
  Lwt_main.run
  @@ Lwt.pick
       (Base.List.map t.leader_uris ~f:(fun uri ->
            let* resp = Utils.comm uri msg in
            Lwt.return @@ deserialise_response resp))

let op_nop t =
  let op = Types.Nop in
  send_to_all_replicas t op

let op_create t k v =
  let op = Types.Create (k, v) in
  send_to_all_replicas t op

let op_read t k =
  let op = Types.Read k in
  send_to_all_replicas t op

let op_update t k v =
  let op = Types.Update (k, v) in
  send_to_all_replicas t op

let op_remove t k =
  let op = Types.Remove k in
  send_to_all_replicas t op

module C_Lwt = struct
  let send_to_all_replicas t msg =
    let msg = serialise_request msg in
    Lwt.pick
      (Base.List.map t.leader_uris ~f:(fun uri ->
           let* resp = Utils.comm uri msg in
           Lwt.return @@ deserialise_response resp))

  let op_nop t =
    let op = Types.Nop in
    send_to_all_replicas t op

  let op_create t k v =
    let op = Types.Create (k, v) in
    send_to_all_replicas t op

  let op_read t k =
    let op = Types.Read k in
    send_to_all_replicas t op

  let op_update t k v =
    let op = Types.Update (k, v) in
    send_to_all_replicas t op

  let op_remove t k =
    let op = Types.Remove k in
    send_to_all_replicas t op
end

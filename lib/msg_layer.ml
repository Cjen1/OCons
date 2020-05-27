open Lwt.Infix

let src = Logs.Src.create "Msg_layer" ~doc:"messaging layer"

module Log = (val Logs.src_log src : Logs.LOG)

module Messages = struct end

let compression = `None

module Outgoing_socket = struct
  type t = {fd: Lwt_unix.file_descr; send_mtx: Lwt_mutex.t; switch: Lwt_switch.t}

  let create ?switch fd =
    let send_mtx = Lwt_mutex.create () in
    let switch =
      match switch with Some switch -> switch | None -> Lwt_switch.create ()
    in
    {fd; send_mtx; switch}

  let send_raw t size cont = 
    let buf = Lwt_bytes.create size in
    assert (size = cont buf);
    Log.debug (fun m -> m "Sending") ;
    Log.debug (fun m ->
        m "hexdump_send:\n%s"
          (Lwt_bytes.to_string buf |> Hex.of_string |> Hex.hexdump_s)) ;
    let rec send_loop offset len () =
      Lwt_bytes.write t.fd buf offset len
      >>= fun len' ->
      if len' < len then send_loop (offset + len') (len - len') ()
      else Lwt.return_unit
    in
    Lwt_mutex.with_lock t.send_mtx (send_loop 0 size)

  let send t msg =
    if Lwt_switch.is_on t.switch then (
      let blit dst src ~dst_pos ~len =
        Lwt_bytes.blit_from_bytes src 0 dst dst_pos len
      in
      let size, cont = Capnp.Codecs.serialize_generator msg blit in
      send_raw t size cont
      >>= fun () -> Lwt.return_ok () )
    else Lwt.return_error `Closed
end

module Incomming_socket = struct
  type t =
    { fd: Lwt_unix.file_descr
    ; decoder: Capnp.Codecs.FramedStream.t
    ; switch: Lwt_switch.t
    ; recv_cond: unit Lwt_condition.t }

  let rec recv t =
    match Capnp.Codecs.FramedStream.get_next_frame t.decoder with
    | _ when not (Lwt_switch.is_on t.switch) ->
        Lwt.return_error `Closed
    | Ok msg ->
        Lwt.return (Ok (Capnp.BytesMessage.Message.readonly msg))
    | Error Capnp.Codecs.FramingError.Unsupported ->
        failwith "Unsupported Cap'n'Proto frame received"
    | Error Capnp.Codecs.FramingError.Incomplete ->
        Log.debug (fun f -> f "Incomplete; waiting for more data...") ;
        Lwt_condition.wait t.recv_cond >>= fun () -> recv t

  let recv_thread ?(buf_size = 4096) t =
    let handler recv_buffer len =
      if len > 0 then (
        Log.debug (fun m -> m "Read %d bytes" len) ;
        let buf = Bytes.sub recv_buffer 0 len in
        Log.debug (fun m ->
            m "hexdump_recv:\n%s" (buf |> Hex.of_bytes |> Hex.hexdump_s)) ;
        Capnp.Codecs.FramedStream.add_fragment t.decoder
          (Bytes.unsafe_to_string buf) ;
        Lwt_condition.broadcast t.recv_cond () ;
        Bytes.fill recv_buffer 0 buf_size '\x00' ;
        (*Maybe only to len?*)
        Ok () )
      else Error `EOF
    in
    let recv_buffer = Bytes.create buf_size in
    let rec iter () =
      Lwt_unix.read t.fd recv_buffer 0 buf_size
      >>= fun len ->
      match handler recv_buffer len with
      | Ok () ->
          iter ()
      | Error `EOF ->
          Lwt.return_unit
    in
    iter ()

  let create ?switch fd =
    let decoder = Capnp.Codecs.FramedStream.empty compression in
    let switch =
      match switch with Some switch -> switch | None -> Lwt_switch.create ()
    in
    let recv_cond = Lwt_condition.create () in
    let t = {fd; decoder; switch; recv_cond} in
    let recv_thread = recv_thread t in
    Lwt_switch.add_hook (Some switch) (fun () ->
        Lwt.cancel recv_thread ; Lwt.return_unit) ;
    t
end

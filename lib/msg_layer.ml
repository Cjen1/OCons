

open Lwt.Infix
let ( >>>= ) = Lwt_result.bind

let src = Logs.Src.create "Msg_layer" ~doc:"messaging layer"

module Log = (val Logs.src_log src : Logs.LOG)

module Messages = struct end

let compression = `None

module Outgoing_socket = struct
  type t =
    { fd: Lwt_unix.file_descr
    ; xmit_queue: (int * (Lwt_bytes.t -> int)) Queue.t
    ; xmit_cond: unit Lwt_condition.t
    ; switch: Lwt_switch.t }

  let send_thread t =
    let rec send buf offset len () =
      Utils.catch (fun () -> Lwt_bytes.write t.fd buf offset len)
      >>= function
      | Ok len' ->
          Log.debug (fun m -> m "Wrote %d to fd" len') ;
          if len' < len then send buf (offset + len') (len - len') ()
          else Lwt.return_unit
      | Error e ->
          Log.err (fun m -> m "Failed to send %a" Fmt.exn e) ;
          Lwt.return_unit
    in
    let rec loop () =
      let rec get_msg_loop () =
        match Queue.take_opt t.xmit_queue with
        | _ when not (Lwt_switch.is_on t.switch) ->
            Log.warn (fun m -> m "Switch closed exiting send loop");
            Lwt.return_error `Closed
        | Some v ->
            Lwt.return_ok v
        | None ->
            Lwt_condition.wait t.xmit_cond >>= fun () -> get_msg_loop ()
      in
      get_msg_loop ()
      >>>= fun (size, cont) ->
      let buf = Lwt_bytes.create size in
      let written = cont buf in
      assert (size = written) ;
      send buf 0 size () >>= loop
    in
    loop ()

  let create ?switch fd =
    let switch =
      match switch with Some switch -> switch | None -> Lwt_switch.create ()
    in
    let xmit_cond = Lwt_condition.create () in
    Lwt_switch.add_hook (Some switch) (fun () ->
        Lwt_condition.broadcast xmit_cond () ;
        Lwt.return_unit) ;
    let xmit_queue = Queue.create () in
    let t = {fd; xmit_queue; xmit_cond; switch} in
    Lwt.async (fun () ->
        send_thread t
        >>= function
        | Ok () ->
            Log.err (fun m -> m "thread failed") ;
            failwith "Thread closed unexpectedly"
        | Error _ ->
            Lwt.return_unit) ;
    t

  let send t msg =
    if Lwt_switch.is_on t.switch then (
      Log.debug (fun m -> m "Trying to send") ;
      let blit dst src ~offset ~len =
        Lwt_bytes.blit_from_bytes src 0 dst offset len
      in
      try
        let size, cont = Capnp.Codecs.serialize_generator msg blit in
        Queue.add (size, cont) t.xmit_queue ;
        Lwt_condition.broadcast t.xmit_cond () ;
        Ok ()
      with e -> Fmt.failwith "Failed while sending with: %a" Fmt.exn e )
    else Error `Closed
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

  let recv_thread ?(buf_size = 256) t =
    let handler recv_buffer len =
      if len > 0 then (
        let buf = Bytes.sub recv_buffer 0 len in
        Capnp.Codecs.FramedStream.add_fragment t.decoder
          (Bytes.unsafe_to_string buf) ;
        Lwt_condition.broadcast t.recv_cond () ;
        (* don't need to wipe buffer since will be wiped by Bytes.sub *)
        Ok () )
      else Error `EOF
    in
    let recv_buffer = Bytes.create buf_size in
    let rec loop () =
      Utils.catch (fun () -> Lwt_unix.read t.fd recv_buffer 0 buf_size)
      >>= function
      | Error e ->
          Fmt.failwith "Failed to recv with %a" Fmt.exn e
      | Ok len -> (
          Log.debug (fun m -> m "Recieved data from socket") ;
          match handler recv_buffer len with
          | Ok () ->
              loop ()
          | Error `EOF ->
              Lwt.return_unit )
    in
    loop ()

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

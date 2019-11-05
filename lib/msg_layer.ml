open Base

let msg_layer = Logs.Src.create "Msg_layer" ~doc:"messaging layer"

module MLog = (val Logs.src_log msg_layer : Logs.LOG)

type t =
  { nodes: string list ; local_location: string
  ; last_rec: (string, float) Base.Hashtbl.t
  ; alive_timeout: float
  ; context: Zmq.Context.t
  ; pub: [`Pub] Zmq.Socket.t
  ; sub: [`Sub] Zmq.Socket.t
  ; subs: (string, (string -> unit Lwt.t) list) Base.Hashtbl.t }

let create_sub_socket nodes ctx =
  let socket = Zmq.Socket.create ctx Zmq.Socket.sub in
  Base.List.iter nodes ~f:(fun uri ->
      Zmq.Socket.connect socket ("tcp://" ^ uri)) ;
  socket

let create_pub_socket local ctx =
  let socket = Zmq.Socket.create ctx Zmq.Socket.pub in
  Zmq.Socket.bind socket ("tcp://" ^ local) ;
  socket

let run t () =
  let open Zmq_lwt in
  let sock = Socket.of_socket t.sub in
  let rec loop () =
    let%lwt filter = Socket.recv sock in
    let%lwt node_name = Socket.recv sock in
    Base.Hashtbl.set t.last_rec ~key:node_name ~data:(Unix.time ()) ;
    let%lwt msg = Socket.recv sock in
    let%lwt () =
      (* Cannot throw error since if subscribed then it exists *)
      Base.Hashtbl.find_exn t.subs filter
      |> Lwt_list.iter_p (fun f ->
             MLog.debug (fun m -> m "Found a callback for filter %s" filter) ;
             f msg)
    in
    loop ()
  in
  MLog.debug (fun m -> m "Spooling up msg layer") ;
  loop ()

let attach_watch t ~filter ~callback =
  Base.Hashtbl.change t.subs filter ~f:(function
    | Some ls ->
        Some (callback :: ls)
    | None ->
        Some [callback]) ;
  Zmq.Socket.subscribe t.sub filter ;
  MLog.debug (fun m -> m "Attached watch to %s" filter)

let retry ?(tag = "") ~finished ~timeout f =
  let rec loop timeout =
    let tmout =
      let%lwt () = Random.float_range 0. timeout |> Lwt_unix.sleep in
      Lwt.return_none
    in
    f () ;
    let%lwt res =
      Lwt.pick
        [ (let%lwt res = finished in
           Lwt.return_some res)
        ; tmout ]
    in
    match res with
    | Some res ->
        Lwt.return res
    | None ->
        MLog.debug (fun m ->
            m "%s Request timed out retrying. t = %f" tag timeout) ;
        loop (timeout *. 1.2)
  in
  loop timeout

let send_msg t ?(timeout = 1.) ?(finished = Lwt.return_unit) ~filter msg =
  let f () = Zmq.Socket.send_all t.pub [filter; t.local_location; msg] in
  retry ~tag:filter ~finished ~timeout f

let last_rec_lookup t node =
  match Base.Hashtbl.find t.last_rec node with
  | Some v ->
      Base.Ok v
  | None ->
      Base.Error `NodeNotFound

let node_alive t ~node =
  let ( >>= ) v f = Base.Result.bind v ~f in
  last_rec_lookup t node
  >>= fun last_rec ->
  Ok (Base.Float.( < ) (Unix.time () -. last_rec) t.alive_timeout)

let node_dead_watch t ~node ~callback =
  let ( >>= ) v f = Base.Result.bind v ~f in
  last_rec_lookup t node (* Ensure an error isn't thrown *)
  >>= fun _ ->
  let p () =
    let rec loop () =
      match
        t.alive_timeout +. Hashtbl.find_exn t.last_rec node -. Unix.time ()
      with
      | n when Float.( > ) n 0. ->
          let%lwt () = Lwt_unix.sleep n in
          loop ()
      | _ ->
          MLog.debug (fun m ->
              m "Node: %s timed out, calling leader election" node) ;
          callback ()
    in
    loop ()
  in
  MLog.debug (fun m -> m "Attached dead_node watch to %s" node) ;
  Ok (Lwt.async p)

let one_use_socket ~callback ~address_sub ~address_pub t =
  let sub_socket = Zmq.Socket.create t.context Zmq.Socket.sub in
  let pub_socket = Zmq.Socket.create t.context Zmq.Socket.pub in
  let () = Zmq.Socket.bind pub_socket ("tcp://" ^ address_pub) in
  let () = Zmq.Socket.bind sub_socket ("tcp://" ^ address_sub) in
  Zmq.Socket.subscribe sub_socket "*";
  let sub_socket = Zmq_lwt.Socket.of_socket sub_socket in
  let pub_socket = Zmq_lwt.Socket.of_socket pub_socket in
  let open Zmq_lwt in
  let rec loop () =
    MLog.debug (fun m -> m "one_use_socket: awaiting conn on %s" address_sub) ;
    let%lwt addr, msg = 
      let%lwt resp = Socket.recv_all sub_socket in
      match resp with
      | [addr; msg] -> Lwt.return @@ (addr,msg)
      | _ -> assert false
    in 
    MLog.debug (fun m -> m "one_use_socket: got req from %s" addr) ;
    let%lwt () = callback msg (fun msg -> Socket.send_all pub_socket [addr;msg]) in
    loop ()
  in
  loop ()

let keep_alive t = 
  let () = attach_watch t ~filter:"keepalive" ~callback:(fun _ -> Lwt.return_unit) in
  let rec loop () = 
    Lwt.async(fun () -> send_msg t ~filter:"keepalive" "");
    let%lwt () = Lwt_unix.sleep (t.alive_timeout /. 2.) in
    loop ()
  in loop ()


let create ~node_list ~local ~alive_timeout =
  let ctx = Zmq.Context.create () in
  let open Base in
  let t =
    { nodes= node_list
    ; local_location= local
    ; last_rec= Hashtbl.create (module String)
    ; alive_timeout
    ; context= ctx
    ; pub= create_pub_socket local ctx
    ; sub= create_sub_socket node_list ctx
    ; subs= Hashtbl.create (module String) }
  in
  (t, Lwt.join [run t (); keep_alive t])


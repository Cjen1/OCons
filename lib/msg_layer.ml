open Base

let msg_layer = Logs.Src.create "Msg_layer" ~doc:"messaging layer"

module MLog = (val Logs.src_log msg_layer : Logs.LOG)

type t =
  { nodes: string list
  ; local_location: string
  ; last_rec: (string, float) Base.Hashtbl.t
  ; alive_timeout: float
  ; context: Zmq.Context.t
  ; pub: [`Pub] Zmq.Socket.t
  ; sub: [`Sub] Zmq.Socket.t
  ; subs: (string, (string -> unit Lwt.t) list) Base.Hashtbl.t
  ; mutable node_dead_watch: unit Lwt.t option }

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
  let sock = Zmq_lwt.Socket.of_socket t.sub in
  let rec loop () =
    let%lwt filter = Zmq_lwt.Socket.recv sock in
    MLog.debug (fun m -> m "Got filter: %s" filter) ;
    let%lwt node_name = Zmq_lwt.Socket.recv sock in
    let%lwt msg = Zmq_lwt.Socket.recv sock in
    Base.Hashtbl.set t.last_rec ~key:node_name ~data:(Unix.time ()) ;
    let%lwt () =
      match Base.Hashtbl.find t.subs filter with
      | Some v ->
          Lwt_list.iter_p (fun f -> f msg) v
      | None ->
          Lwt.return_unit
    in
    loop ()
  in
  MLog.debug (fun m -> m "Spooling up msg layer") ;
  loop ()

let attach_watch_untyped t ~filter ~callback =
  Base.Hashtbl.change t.subs filter ~f:(function
    | Some ls ->
        Some (callback :: ls)
    | None ->
        Some [callback]) ;
  Zmq.Socket.subscribe t.sub filter ;
  MLog.debug (fun m -> m "Attached watch to %s" filter)

let attach_watch t ~filter ~callback ~typ =
  let callback msg = msg |> Messaging.from_string typ |> callback in
  attach_watch_untyped t ~filter ~callback

let retry ?(tag = "") ~finished ~timeout f =
  let rec loop timeout =
    let tmout =
      let%lwt () = Random.float_range 0. timeout |> Lwt_unix.sleep in
      Lwt.return_none
    in
    f () ;
    let%lwt res =
      Lwt.choose
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
        loop (timeout *. 1.1)
  in
  loop timeout

let send_msg t ?(timeout = 1.) ?(finished = Lwt.return_unit) ~filter msg =
  let f () =
    Lwt.async (fun () ->
        Zmq_lwt.Socket.send_all
          (Zmq_lwt.Socket.of_socket t.pub)
          [filter; t.local_location; msg])
  in
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
  ( match t.node_dead_watch with
  | Some p ->
      MLog.debug (fun m -> m "Cancelling current node_dead_watch") ;
      Lwt.cancel p
  | None ->
      () ) ;
  let ( >>= ) v f = Base.Result.bind v ~f in
  last_rec_lookup t node (* Ensure an error isn't thrown *)
  >>= fun _ ->
  let p =
    let rec loop () =
      let timeout_time = Hashtbl.find_exn t.last_rec node +. t.alive_timeout in
      let current_time = Unix.time () in
      match Float.(timeout_time > current_time) with
      | true ->
          let%lwt () = Lwt_unix.sleep (timeout_time -. current_time) in
          loop ()
      | _ ->
          MLog.debug (fun m ->
              m
                "Node: %s timed out, would have expired at %f, executing \
                 callback"
                node timeout_time) ;
          callback ()
    in
    try%lwt loop () with Lwt.Canceled -> Lwt.return_unit
  in
  t.node_dead_watch <- Some p ;
  MLog.debug (fun m -> m "Attached dead_node watch to %s" node) ;
  Ok (Lwt.async (fun () -> p))

let client_socket ~callback ~address_req ~address_rep t =
  let sub_socket = Zmq.Socket.create t.context Zmq.Socket.sub in
  let router_socket = Zmq.Socket.create t.context Zmq.Socket.router in
  let () = Zmq.Socket.bind sub_socket ("tcp://" ^ address_req) in
  let () = Zmq.Socket.bind router_socket ("tcp://" ^ address_rep) in
  Zmq.Socket.subscribe sub_socket "" ;
  let sub_socket = Zmq_lwt.Socket.of_socket sub_socket in
  let router_socket = Zmq_lwt.Socket.of_socket router_socket in
  let rec loop () =
    MLog.debug (fun m -> m "one_use_socket: awaiting conn on %s" address_req) ;
    let%lwt resp = Zmq_lwt.Socket.recv_all sub_socket in
    let addr, rid, msg =
      match resp with
      | [addr; rid; msg] ->
          (addr, rid, msg)
      | _ ->
          MLog.err (fun m -> m "FAILED: one use socket") ;
          assert false
    in
    MLog.debug (fun m -> m "one_use_socket: got req from %s" addr) ;
    Lwt.async (fun () ->
        callback msg (fun msg ->
            MLog.debug (fun m -> m "client_socket: sending [%s;%s]" addr rid) ;
            Zmq_lwt.Socket.send_all router_socket [addr; rid; msg])) ;
    loop ()
  in
  loop ()

let keep_alive t =
  Zmq.Socket.subscribe t.sub "keepalive" ;
  let rec loop () =
    Lwt.async (fun () ->
        MLog.debug (fun m -> m "sending keepalive") ;
        send_msg t ~filter:"keepalive" "") ;
    let%lwt () = Lwt_unix.sleep (t.alive_timeout /. 2.) in
    loop ()
  in
  loop ()

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
    ; subs= Hashtbl.create (module String)
    ; node_dead_watch= None }
  in
  let curr_time = Unix.time () in
  List.iter node_list ~f:(fun node ->
      Hashtbl.set t.last_rec ~key:node ~data:curr_time) ;
  (t, Lwt.join [run t (); keep_alive t])

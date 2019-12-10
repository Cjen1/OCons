open Base
open Messaging

let msg_layer = Logs.Src.create "Msg_layer" ~doc:"messaging layer"

module MLog = (val Logs.src_log msg_layer : Logs.LOG)

module Msg_Queue = struct
  open Utils

  type msg = string

  type filter = string

  type t =
    { socket: [`Router] Zmq_lwt.Socket.t
    ; dest: string
    ; src: string
    ; queue: (filter * msg) Utils.Queue.t }

  let create socket dest src = {socket; dest; queue= Queue.create (); src}

  let send t msg = Queue.add msg t.queue

  let retry_timeout = 10.

  let rec loop t =
    let%lwt filter, msg = Queue.take t.queue in
    let rec retry_loop () =
      try%lwt
        let%lwt () =
          Messaging.send_inc ~sock:t.socket ~dest:t.dest ~filter ~src:t.src ~msg
        in
        Lwt.return_unit
      with Unix.Unix_error (EHOSTUNREACH, _, _) ->
        MLog.err (fun m -> m "Host unreachable, retrying") ;
        let%lwt () = Lwt_unix.sleep retry_timeout in
        retry_loop ()
    in
    let%lwt () = retry_loop () in
    loop t
end

type t =
  { endpoints: (string, string) Base.Hashtbl.t
  ; last_rec: (string, float) Base.Hashtbl.t
  ; id: string
  ; alive_timeout: float
  ; context: Zmq.Context.t
  ; incomming: [`Router] Zmq_lwt.Socket.t
  ; outgoing: [`Router] Zmq_lwt.Socket.t
  ; msg_queues: (string, Msg_Queue.t) Base.Hashtbl.t
  ; subs: (string, (string -> unit Lwt.t) list) Base.Hashtbl.t
  ; mutable node_dead_watch: unit Lwt.t option }

let create_incoming id port ctx =
  let socket = Zmq.Socket.create ctx Zmq.Socket.router in
  MLog.debug (fun m -> m "Setting identity for %s = %s" port id) ;
  Zmq.Socket.set_identity socket id ;
  Zmq.Socket.set_router_mandatory socket true ;
  let address = "tcp://*:" ^ port in
  Zmq.Socket.bind socket address ;
  Zmq_lwt.Socket.of_socket socket

let create_outgoing endpoints ctx =
  let socket = Zmq.Socket.create ctx Zmq.Socket.router in
  Zmq.Socket.set_router_mandatory socket true ;
  Hashtbl.iter endpoints ~f:(fun uri ->
      Zmq.Socket.connect socket ("tcp://" ^ uri)) ;
  Zmq_lwt.Socket.of_socket socket

let create_msg_queues nodes sock id =
  let tbl = Hashtbl.create (module String) in
  Hashtbl.iteri nodes ~f:(fun ~key ~data:_ ->
      Hashtbl.set tbl ~key ~data:(Msg_Queue.create sock key id)) ;
  tbl

let run t () =
  let rec loop () =
    let%lwt resp = recv_inc ~sock:t.incomming in
    ( match resp with
    | Ok (filter, node_name, msg) ->
        MLog.debug (fun m -> m "Got filter: %s" filter) ;
        Base.Hashtbl.set t.last_rec ~key:node_name ~data:(Unix.time ()) ;
        Lwt.async (fun () ->
            match Base.Hashtbl.find t.subs filter with
            | Some v ->
                Lwt_list.iter_p (fun f -> f msg) v
            | None ->
                Lwt.return_unit)
    | Error _ ->
        () ) ;
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
  MLog.debug (fun m -> m "Attached watch to %s" filter)

let attach_watch t ~msg_filter ~callback =
  let callback msg = msg |> from_string msg_filter.typ |> callback in
  attach_watch_untyped t ~filter:msg_filter.filter ~callback

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
        loop (timeout *. 1. (*.1*))
  in
  loop timeout

let send_msg_untyped t ~filter msg =
  Hashtbl.iter t.msg_queues ~f:(fun queue -> Msg_Queue.send queue (filter, msg))

let send_msg t ~msg_filter msg =
  let msg = to_string msg_filter.typ msg in
  send_msg_untyped t ~filter:msg_filter.filter msg

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

let client_socket t ~callback ~port =
  let sock = Zmq.Socket.create t.context Zmq.Socket.router in
  let () =
    MLog.debug (fun m -> m "Setting identity for %s to %s" t.id port) ;
    Zmq.Socket.set_identity sock t.id ;
    Zmq.Socket.set_router_mandatory sock true ;
    Zmq.Socket.bind sock ("tcp://*:" ^ port)
  in
  let open Zmq_lwt.Socket in
  let sock = of_socket sock in
  let rec loop () =
    MLog.debug (fun m -> m "client_sock: awaiting conn on %s" port) ;
    let%lwt resp = recv_client_req ~sock in
    ( match resp with
    | Ok (addr, rid, msg) ->
        MLog.debug (fun m -> m "one_use_socket: got req from %s" addr) ;
        let writer msg =
          MLog.debug (fun m -> m "client_socket: sending [%s;%s]" addr rid) ;
          send_client_rep ~dest:addr ~rid ~msg ~sock
        in
        Lwt.async (callback msg writer)
    | Error _ ->
        MLog.debug (fun m -> m "client_sock: failed to parse") ;
        () ) ;
    loop ()
  in
  loop ()

let keep_alive t =
  let rec loop () =
    MLog.debug (fun m -> m "sending keepalive") ;
    send_msg_untyped t ~filter:"keepalive" "" ;
    let%lwt () = Lwt_unix.sleep (t.alive_timeout /. 2.) in
    loop ()
  in
  loop ()

let create ~node_list ~id ~alive_timeout =
  let ctx = Zmq.Context.create () in
  let endpoints = Hashtbl.of_alist_exn (module String) node_list in
  let system_port =
    Hashtbl.find_exn endpoints id
    |> String.split ~on:':'
    |> fun lst -> List.nth_exn lst 1
  in
  let incomming = create_incoming id system_port ctx in
  let outgoing = create_outgoing endpoints ctx in
  let last_rec =
    let res = Hashtbl.create (module String) in
    let curr_time = Unix.time () in
    Hashtbl.iter_keys endpoints ~f:(fun node ->
        Hashtbl.set res ~key:node ~data:curr_time) ;
    res
  in
  let%lwt () = Lwt_unix.sleep 10. in
  (* Allows other nodes to establish binds thus allowing bootstraping *)
  let msg_queues = create_msg_queues endpoints outgoing id in
  let t =
    { endpoints
    ; id
    ; last_rec
    ; alive_timeout
    ; context= ctx
    ; incomming
    ; outgoing
    ; msg_queues
    ; subs= Hashtbl.create (module String)
    ; node_dead_watch= None }
  in
  let curr_time = Unix.time () in
  Hashtbl.iter_keys endpoints ~f:(fun node ->
      Hashtbl.set t.last_rec ~key:node ~data:curr_time) ;
  let mq_ps =
    Hashtbl.fold msg_queues ~init:[] ~f:(fun ~key:_ ~data acc ->
        let p = Msg_Queue.loop data in
        p :: acc)
  in
  Lwt.return (t, Lwt.join ([run t (); keep_alive t] @ mq_ps))

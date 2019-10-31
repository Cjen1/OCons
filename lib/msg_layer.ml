open Base

type t =
  { nodes: string list
  ; pub: [`Pub] Zmq.Socket.t
  ; sub: [`Sub] Zmq.Socket.t
  ; subs: (string, (string -> unit Lwt.t) list) Base.Hashtbl.t }

let create_sub_socket nodes ctx =
  let socket = Zmq.Socket.create ctx Zmq.Socket.sub in
  List.iter nodes ~f:(fun uri -> Zmq.Socket.connect socket ("tcp://" ^ uri)) ;
  socket

let create_pub_socket local ctx =
  let socket = Zmq.Socket.create ctx Zmq.Socket.pub in
  Zmq.Socket.bind socket ("tcp://" ^ local) ;
  socket

let rec run t () =
  let open Zmq_lwt in
  let sock = Socket.of_socket t.sub in
  let%lwt filter = Socket.recv sock in
  let%lwt msg = Socket.recv sock in
  let%lwt () =
    (* Cannot throw error since if subscribed then it exists *)
    Hashtbl.find_exn t.subs filter |> Lwt_list.iter_p (fun f -> f msg)
  in
  run t ()

let create ~node_list ~local =
  let ctx = Zmq.Context.create () in
  let t =
    { nodes= node_list
    ; pub= create_pub_socket local ctx
    ; sub= create_sub_socket node_list ctx
    ; subs= Hashtbl.create (module String) }
  in
  Lwt.async (run t) ;
  t

let attach_watch t ~filter ~callback =
  Hashtbl.change t.subs filter ~f:(function
    | Some ls ->
        Some (callback :: ls)
    | None ->
        Some [callback]) ;
  Zmq.Socket.subscribe t.sub filter

let retry ~finished ~timeout f =
  let rec loop () =
    let tmout =
      let%lwt () = timeout |> Lwt_unix.sleep in
      Lwt.return_none
    in
    f () ;
    let%lwt res =
      Lwt.choose
        [ (let%lwt res = finished in
           Lwt.return_some res)
        ; tmout ]
    in
    match res with Some res -> Lwt.return res | None -> loop ()
  in
  loop ()

let send_msg t ?(timeout = 10.) ?(finished = Lwt.return_unit) ~filter msg =
  let f () = Zmq.Socket.send_all t.pub [filter; msg] in
  retry ~finished ~timeout f

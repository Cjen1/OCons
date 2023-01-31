open! Eio
module P = Persistent_conn
module IdMap = Map.Make (Int)
open! Util

type resolver = P.resolver

type id = int

type 'a iter = ('a -> unit) -> unit

type 'a t =
  { conns_map: P.t IdMap.t
  ; conns_list: (id * P.t) list
  ; reader_channel: (id * 'a) Stream.t
  ; recv_cond: Condition.t }

let connect_connected ~sw conns_list connected =
  connected
  |> Option.iter (fun connected ->
         Fiber.fork ~sw (fun () ->
             conns_list
             |> List.map (fun p () -> Promise.await p)
             |> Fiber.any ;
             Promise.resolve (snd connected) () ) )

let create ?(max_recv_buf = 1024) ?connected ~sw resolvers parse =
  let conns_list_prom =
    Fiber.List.map
      (fun (id, r) ->
        let conn = Promise.create () in
        ((id, P.create ~connected:conn ~sw r), fst conn) )
      resolvers
  in
  let prom_list = List.map snd conns_list_prom in
  connect_connected ~sw (prom_list) connected ;
  let conns_list = List.map fst conns_list_prom in
  let conns_map = conns_list |> List.to_seq |> IdMap.of_seq in
  let reader_channel = Stream.create max_recv_buf in
  let t =
    {conns_list; conns_map; reader_channel; recv_cond= Condition.create ()}
  in
  List.iter
    (fun (id, c) ->
      Fiber.fork_daemon ~sw (fun () ->
          while P.is_open c do
            dtraceln "Waiting to recv on %d" id ;
            let v = P.recv c parse in
            dtraceln "Recvd a value" ;
            Stream.add reader_channel (id, v) ;
            Condition.broadcast t.recv_cond
          done ;
          assert false ) )
    conns_list ;
  t

let close t = t.conns_list |> Fiber.List.iter (fun (_, c) -> P.close c)

let send ?(blocking = false) t id cs =
  let c = IdMap.find id t.conns_map in
  P.send ~block_until_open:blocking c cs

let broadcast ?(max_fibers = max_int) t cs =
  Fiber.List.iter ~max_fibers (fun (i, _) -> send t i cs) t.conns_list

let send_blit ?(blocking = false) t id bf =
  let c = IdMap.find id t.conns_map in
  P.send_blit ~block_until_open:blocking c bf

let broadcast_blit ?(max_fibers = max_int) t bf =
  Fiber.List.iter ~max_fibers (fun (i, _) -> send_blit t i bf) t.conns_list

let rec recv_any ?(force = false) t iter =
  match Stream.take_nonblocking t.reader_channel with
  | Some v ->
      dtraceln "Recvd something" ; iter v ; recv_any t iter
  | None when force ->
      dtraceln "Nothing to recv, waiting on recv_cond" ;
      Condition.await_no_mutex t.recv_cond ;
      dtraceln "Recv cond await complete" ;
      recv_any ~force t iter
  | None ->
      ()

let flush_all t =
  let f (_, c) = P.flush c in
  Fiber.List.iter f t.conns_list

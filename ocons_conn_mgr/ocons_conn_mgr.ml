open! Eio
module P = Persistent_conn
module IdMap = Map.Make (Int)

type resolver = P.resolver

type id = int

type 'a iter = ('a -> unit) -> unit

type 'a t =
  { conns_map: P.t IdMap.t
  ; conns_list: (id * P.t) list
  ; reader_channel: (id * 'a) Stream.t }

let create ?(max_recv_buf = 1024) ~sw resolvers parse =
  let conns_list =
    Fiber.List.map (fun (id, r) -> (id, P.create ~sw r)) resolvers
  in
  let conns_map = conns_list |> List.to_seq |> IdMap.of_seq in
  let reader_channel = Stream.create max_recv_buf in
  List.iter
    (fun (id, c) ->
      Fiber.fork_daemon ~sw (fun () ->
          while true do
            let v = P.recv c parse in
            Stream.add reader_channel (id, v)
          done ;
          assert false ) )
    conns_list ;
  {conns_list; conns_map; reader_channel}

let close t = 
  t.conns_list |> Fiber.List.iter (fun (_,c) -> P.close c)

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

let rec recv_any t iter =
  match Stream.take_nonblocking t.reader_channel with
  | None ->
      ()
  | Some v ->
      iter v ; recv_any t iter

let flush_all t =
  let f (_, c) = P.flush c in
  Fiber.List.iter f t.conns_list

open! Eio
module PCon = Persistent_conn
module IdMap = Map.Make (Int)
open! Util

type resolver = PCon.resolver

type id = int

type 'a iter = ('a -> unit) -> unit

type 'a kind = Iter of (id * 'a -> unit) | Recv of {max_recv_buf: int}

type 'a kind_impl = IIter | IRecv of (id * 'a) Eio.Stream.t

type 'a t =
  { conns_map: PCon.t IdMap.t
  ; conns_list: (id * PCon.t) list
  ; recv_cond: Condition.t
  ; kind: 'a kind_impl }

let connect_connected conns_list = function
  | None ->
      ()
  | Some connected ->
      conns_list |> List.map (fun p () -> Promise.await p) |> Fiber.any ;
      Promise.resolve (snd connected) ()

let make_kind_impl ~sw conns parse kind =
  match kind with
  | Iter f ->
      Fiber.fork_daemon ~sw (fun () ->
          conns
          |> List.map (fun (id, p) () ->
                 PCon.recv_iter p parse (fun v -> f (id, v)) )
          |> Fiber.all ;
          Fiber.await_cancel () ) ;
      IIter
  | Recv {max_recv_buf} ->
      let reader_channel = Stream.create max_recv_buf in
      Fiber.fork_daemon ~sw (fun () ->
          let f id v = Stream.add reader_channel (id, v) in
          conns
          |> List.map (fun (id, p) () -> PCon.recv_iter p parse (f id))
          |> Fiber.all ;
          Fiber.await_cancel () ) ;
      IRecv reader_channel

let create ?(kind = Iter ignore) ?connected ~sw resolvers parse delayer =
  let conns_list_prom =
    StdLabels.List.map resolvers ~f:(fun (id, r) ->
        let conn = Promise.create () in
        ((id, PCon.create ~connected:conn ~sw r delayer), fst conn) )
  in
  let prom_list = List.map snd conns_list_prom in
  connect_connected prom_list connected ;
  let conns_list = List.map fst conns_list_prom in
  let conns_map = conns_list |> List.to_seq |> IdMap.of_seq in
  { conns_list
  ; conns_map
  ; recv_cond= Condition.create ()
  ; kind= make_kind_impl ~sw conns_list parse kind }

let close t = t.conns_list |> List.iter (fun (_, c) -> PCon.close c)

let send ?(blocking = false) t id cs =
  let c = IdMap.find id t.conns_map in
  PCon.send ~block_until_open:blocking c cs

let broadcast ?(max_fibers = max_int) t cs =
  Fiber.List.iter ~max_fibers (fun (i, _) -> send t i cs) t.conns_list

let send_blit ?(blocking = false) t id bf =
  let c = IdMap.find id t.conns_map in
  PCon.send_blit ~block_until_open:blocking c bf

let broadcast_blit ?(max_fibers = max_int) t bf =
  Fiber.List.iter ~max_fibers (fun (i, _) -> send_blit t i bf) t.conns_list

let rec recv_any ?(force = false) t f =
  match t.kind with
  | IIter ->
      Fmt.invalid_arg "Recv_any on Iter cmgr"
  | IRecv reader_channel ->
      ( match Stream.take_nonblocking reader_channel with
      | Some v ->
          f v
      | None when force ->
          f (Stream.take reader_channel)
      | None ->
          () ) ;
      recv_any ~force:false t f

let flush_all t =
  let f (_, c) = PCon.flush c in
  Fiber.List.iter f t.conns_list

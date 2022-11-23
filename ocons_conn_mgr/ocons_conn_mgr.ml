open! Eio
module P = Persistent_conn
module IdMap = Map.Make (Int)

type resolver = unit -> Flow.two_way

type id = int

type t = {conns_map: P.t IdMap.t; conns_list: (int * P.t) list}

let create ~sw resolvers =
  let conns_list =
    Fiber.List.map (fun (id, r) -> (id, P.create ~sw r)) resolvers
  in
  let conns_map = conns_list |> List.to_seq |> IdMap.of_seq in
  {conns_list; conns_map}

let broadcast ?(max_fibers = max_int) t cs =
  Fiber.List.iter ~max_fibers (fun (_, v) -> P.send v cs) t.conns_list

let send ?(blocking = false) t id cs =
  let c = IdMap.find id t.conns_map in
  P.send ~block_until_open:blocking c cs

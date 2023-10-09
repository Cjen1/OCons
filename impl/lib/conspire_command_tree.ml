open! Core
open! Types
open! Utils

module type Value = sig
  type t [@@deriving compare, show, bin_io, hash]
end

module CommandTree (Value : Value) = struct
  module Key = struct 
    include Md5
    let pp ppf v =
      Fmt.pf ppf "%s" (Md5.to_hex v)
  end
  type key = Key.t [@@deriving show, bin_io, equal, compare]

  let make_key =
    let open struct 
      type relevant_key_data = key * Value.t [@@deriving bin_io]
    end in
    fun parent_key value ->
    Md5.digest_bin_prot bin_writer_relevant_key_data (parent_key, value)

  (* Map of vector clocks to values
     Aim to replicate this to other nodes

     So changes to send:
       Additional heads (branches)
       Extensions to heads
  *)

  type node = int * key * Value.t [@@deriving show, bin_io]

  type parent_ref_node =
    {node: node; parent: parent_ref_node option [@opaque]; key: key}
  [@@deriving show {with_path= false}]

  type t =
    { ctree: parent_ref_node option Map.M(Key).t
          [@printer
            pp_map Key.pp (Fmt.option ~none:(Fmt.any "Root") pp_parent_ref_node)]
    ; root: key }
  [@@deriving show {with_path= false}]

  let root_key = Md5.digest_string ""

  let get_key_of_node node = match node with None -> root_key | Some {key; _} -> key

  let get_idx_of_node node =
    match node with None -> 0 | Some {node= idx, _, _; _} -> idx

  let get_idx t clk = Map.find_exn t.ctree clk |> get_idx_of_node

  let get_value_of_node node =
    match node with None -> None | Some {node= _, _, v; _} -> Some v

  let get_value t clk = Map.find_exn t.ctree clk |> get_value_of_node

  let get_parent_vc t clock =
    match Map.find_exn t.ctree clock with
    | None ->
        clock
    | Some {node= _, parent, _; _} ->
        parent

  let get t clock = Map.find_exn t.ctree clock

  let prefix t ha hb =
    let rec aux a b =
      match (a, b) with
      | None, None ->
          true
      | Some {key= ha; _}, Some {key= hb; _} when [%equal: Md5.t] ha hb ->
          true
      | _, Some {parent; _} when get_idx_of_node a < get_idx_of_node b ->
          aux a parent
      | _ ->
          false
    in
    aux (get t ha) (get t hb)

  type update = {new_head: key; extension: node list}
  [@@deriving show {with_path= false}, bin_io]

  let apply_update t update =
    match () with
    | _ when Map.mem t.ctree update.new_head ->
        Ok t
    | _ -> (
      match update.extension with
      | [] ->
          Fmt.failwith "All updates should be non-empty"
      | (_, par, _) :: _ when not (Map.mem t.ctree par) ->
          Fmt.error_msg "Missing %a" Key.pp par
      | (_, par, _) :: _ as extension ->
          let rec aux (ctree : parent_ref_node option Map.M(Key).t)
              (extension : node list) parent =
            match extension with
            | [] ->
                ctree
            | [a] ->
                Map.set ctree ~key:update.new_head
                  ~data:(Some {node= a; parent; key= update.new_head})
            | a :: ((_, clk, _) :: _ as rem) ->
                let node = Some {node= a; parent; key= clk} in
                let ctree = Map.set ctree ~key:clk ~data:node in
                aux ctree rem node
          in
          let ctree = aux t.ctree extension (get t par) in
          Ok {t with ctree} )

  let apply_update_exn t update =
    match apply_update t update with
    | Ok t ->
        t
    | Error (`Msg s) ->
        Fmt.failwith "%s" s

  let addv t ~node:_ ~parent vi =
    let parent_key = parent in
    let parent_node = get t parent_key in
    let ctree = t.ctree in
    let ctree, _, new_head =
      IterLabels.fold vi ~init:(ctree, parent_node, parent_key)
        ~f:(fun (ctree, parent, parent_key) value ->
          let key = make_key (get_key_of_node parent) value in
          let node =
            Some
              { node= (get_idx_of_node parent + 1, parent_key, value)
              ; parent
              ; key }
          in
          let ctree = Map.set ctree ~key ~data:node in
          (ctree, node, key) )
    in
    ({t with ctree}, new_head)

  let make_update t target_node (other : t) =
    let rec aux curr acc =
      match curr with
      | None ->
          acc
      | Some {key; _} when Map.mem other.ctree key ->
          acc
      | Some {parent; node; _} ->
          aux parent (node :: acc)
    in
    {new_head= target_node; extension= aux (get t target_node) []}

  let copy_update src dst upto =
    let rec aux dsttree curr =
      match curr with
      | None ->
          dsttree
      | Some {key; _} when Map.mem dst.ctree key ->
          dsttree
      | Some {node= _; parent; key} ->
          let dctree' = Map.set dsttree ~key ~data:curr in
          aux dctree' parent
    in
    {dst with ctree= aux dst.ctree (get src upto)}

  let empty =
    let root_clock = get_key_of_node None in
    let ctree = Map.empty (module Key) |> Map.set ~key:root_clock ~data:None in
    {ctree; root= root_clock}

  (* finds the highest index where >= [threshold] of [clks] are the same *)
  let greatest_sufficiently_common_prefix t clks threshold =
    let hds = clks |> List.map ~f:(get t) in
    let%map.Option maximum_possible_idx =
      (* [4,3,2,1,0]. idx which ensure 3 exists geq = List.nth ls 2 *)
      let hds =
        hds
        |> List.map ~f:get_idx_of_node
        |> List.sort ~compare:(fun a b -> Int.neg @@ Int.compare a b)
      in
      List.nth hds (threshold - 1)
    in
    let filter_to_leq_idx idx node =
      let rec aux node idx =
        match node with
        | None ->
            None
        | Some {parent; node= nidx, _, _; _} when nidx > idx ->
            aux parent idx
        | Some _ as node ->
            node
      in
      aux node idx
    in
    let hds = Array.of_list hds in
    let counts = Hashtbl.create ~size:(Array.length hds) (module Key) in
    let rec aux idx =
      Hashtbl.clear counts ;
      Array.map_inplace hds ~f:(filter_to_leq_idx idx) ;
      Array.iter hds ~f:(function
        | None ->
            Hashtbl.incr counts t.root
        | Some {key; _} ->
            Hashtbl.incr counts key ) ;
      let res =
        Hashtbl.fold counts ~init:None ~f:(fun ~key ~data acc ->
            match acc with
            | Some _ ->
                acc
            | None when data >= threshold ->
                Some key
            | None ->
                None )
      in
      match res with Some key -> key | None -> aux (idx - 1)
    in
    aux maximum_possible_idx

  let compare_keys t ka kb =
    let a, b = (get t ka, get t kb) in
    match (a, b) with
    | None, None ->
        Some EQ
    | None, Some _ ->
        Some LT
    | Some _, None ->
        Some GT
    | Some ({node= ia, _, _; _} as na), Some ({node= ib, _, _; _} as nb) -> (
        let rec on_path t curr ({key= kt; node= it, _, _; _} as target) =
          assert (get_idx_of_node curr >= it);
          match curr with
          | None ->
              false
          | Some {key; _} when [%equal: Key.t] key kt ->
              true
          | Some {node= i, _, _; _} when i = it ->
              false
          | Some {parent; node= i, _, _; _} ->
              assert (i > it) ;
              on_path t parent target
        in
        match () with
        | () when [%equal: Key.t] ka kb ->
            Some EQ
        | () when ia < ib ->
            Option.some_if (on_path t b na) LT
        | () when ia > ib ->
            Option.some_if (on_path t a nb) GT
        | () ->
            assert (ia = ib) ;
            assert (not @@ [%equal: Key.t] ka kb) ;
            None )

  let path_between t rt_vc hd_vc =
    let rt, hd = (get t rt_vc, get t hd_vc) in
    let rec aux curr acc : node list =
      match (curr, rt) with
      | None, Some _ ->
          Fmt.invalid_arg "%a not on path to %a" Key.pp rt_vc Key.pp hd_vc
      | None, None ->
          acc
      | Some curr, Some rt when [%equal: Key.t] curr.key rt.key ->
          acc
      | Some curr, _ ->
          aux curr.parent (curr.node :: acc)
    in
    aux hd []

  let mem t vc = Map.mem t.ctree vc

  let tree_invariant t =
    let rec path_to_root t c acc =
      let par = get_parent_vc t c in
      match () with
      | _ when [%equal: Key.t] par c ->
          Set.add acc c
      | _ when Set.mem acc par ->
          Fmt.failwith "Loop detected around %a" Key.pp c
      | _ ->
          path_to_root t par (Set.add acc c)
    in
    Map.key_set t.ctree
    |> Set.fold
         ~init:(Set.empty (module Key))
         ~f:(fun visited vc ->
           if Set.mem visited vc then visited
           else Set.union visited (path_to_root t vc (Set.empty (module Key)))
           )
    |> ignore
end

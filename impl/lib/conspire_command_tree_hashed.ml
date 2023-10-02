open! Core
open! Types
open! Utils

let map_pp kpp vpp : _ Map.t Fmt.t =
 fun ppf v ->
  Fmt.pf ppf "%a"
    Fmt.(
      brackets @@ list ~sep:Fmt.semi @@ parens
      @@ pair ~sep:(Fmt.any ":@ ") kpp vpp )
    (Map.to_alist v)

let set_pp pp : _ Set.t Fmt.t =
 fun ppf v ->
  Fmt.pf ppf "%a" Fmt.(brackets @@ list ~sep:comma @@ pp) (Set.to_list v)

(*
module VectorClock = struct
  module T_list = struct
    type t = {term: int; clock: int list}
    [@@deriving sexp, bin_io, hash, compare]

    let pp ppf t =
      let open Fmt in
      pf ppf "%d:%a" t.term (brackets @@ list ~sep:(any ",") @@ int) t.clock

    let empty nodes term =
      assert (
        [%equal: int list]
          (List.sort nodes ~compare:[%compare: int])
          (List.init (List.length nodes) ~f:Fun.id) ) ;
      {term; clock= List.map nodes ~f:(fun _ -> 0)}

    let succ t ?(term = t.term) nid =
      if term < t.term then
        Fmt.invalid_arg "Term should monotonically increase but %d < %d" term
          t.term
      else
        let clock =
          List.mapi t.clock ~f:(fun id c -> if id = nid then c + 1 else c)
        in
        {term; clock}

    let comparable a b =
      let g, l =
        List.fold2_exn a.clock b.clock ~init:(false, false)
          ~f:(fun (g, l) a b -> (g || a > b, l || a < b))
      in
      a.term = b.term && not (g && l)

    let leq a b =
      a.term = b.term
      && List.for_all2_exn a.clock b.clock ~f:(fun a b -> a <= b)

    let compare_clock_po a b =
      assert (a.term = b.term) ;
      let gt, lt =
        List.fold2_exn a.clock b.clock ~init:(false, false)
          ~f:(fun (g, l) a b -> (g || a > b, l || a < b))
      in
      match (lt, gt) with
      | true, true ->
          None
      | false, false ->
          Some 0
      | false, true ->
          Some 1
      | true, false ->
          Some (-1)

    let test_make_clock term clocks = {term; clock= clocks}
  end

  module T_map = struct
    type t = {term: int; clock: int Map.M(Int).t}
    [@@deriving sexp, bin_io, hash, compare]

    let pp ppf t =
      let open Fmt in
      pf ppf "%d:%a" t.term
        (brackets @@ list ~sep:(any ",") @@ int)
        (Map.data t.clock)

    let empty nodes term =
      { term
      ; clock=
          Map.of_alist_exn (module Int) (List.map nodes ~f:(fun n -> (n, 0))) }

    let succ t ?(term = t.term) nid =
      if term < t.term then
        Fmt.invalid_arg "Term should monotonically increase but %d < %d" term
          t.term
      else
        { term
        ; clock=
            Map.update t.clock nid ~f:(function
              | None ->
                  Fmt.invalid_arg "Invalid nid %d not in %a" nid pp t
              | Some i ->
                  i + 1 ) }

    let comparable a b =
      a.term = b.term
      && not
           ( Map.existsi a.clock ~f:(fun ~key:nid ~data:clock ->
                 clock < Map.find_exn b.clock nid )
           && Map.existsi b.clock ~f:(fun ~key:nid ~data:clock ->
                  clock < Map.find_exn a.clock nid ) )

    let leq a b =
      comparable a b
      && Map.for_alli a.clock ~f:(fun ~key:nid ~data:clock ->
             clock <= Map.find_exn b.clock nid )

    let compare_clock_po a b =
      assert (a.term = b.term) ;
      let lt = ref false in
      let gt = ref false in
      Map.iteri a.clock ~f:(fun ~key ~data:da ->
          let db = Map.find_exn b.clock key in
          match () with
          | _ when da < db ->
              lt := true
          | _ when da > db ->
              gt := true
          | _ ->
              () ) ;
      match (!lt, !gt) with
      | true, true ->
          None
      | false, false ->
          Some 0
      | false, true ->
          Some 1
      | true, false ->
          Some (-1)

    let test_make_clock term clocks =
      let clock =
        Map.of_alist_exn
          (module Int)
          (List.mapi clocks ~f:(fun idx v -> (idx, v)))
      in
      {term; clock}
  end

  module T = T_map
  include T
  include Core.Comparable.Make (T)
end
*)

module type Value = sig
  type t [@@deriving compare, show, bin_io, hash]
end

module CommandTree (Value : Value) = struct
  type hash = int [@@deriving show, bin_io]

  (* Map of vector clocks to values
     Aim to replicate this to other nodes

     So changes to send:
       Additional heads (branches)
       Extensions to heads
  *)

  type node = int * hash * Value.t [@@deriving show, bin_io]

  type parent_ref_node =
    {node: node; parent: parent_ref_node option [@opaque]; key: hash}
  [@@deriving show {with_path= false}]

  type t =
    { ctree: parent_ref_node option Map.M(Int).t
          [@printer
            map_pp Int.pp (Fmt.option ~none:(Fmt.any "Root") pp_parent_ref_node)]
    ; root: hash }
  [@@deriving show {with_path= false}]

  let get_key_of_node node = match node with None -> 0 | Some {key; _} -> key

  let get_idx_of_node node =
    match node with None -> 0 | Some {node= idx, _, _; _} -> idx

  let get_idx t clk = Map.find_exn t.ctree clk |> get_idx_of_node

  let get_value t clk =
    match Map.find_exn t.ctree clk with
    | None ->
        None
    | Some {node= _, _, v; _} ->
        Some v

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
      | Some {key= ha; _}, Some {key= hb; _} when ha = hb ->
          true
      | _, Some {parent; _} when get_idx_of_node a < get_idx_of_node b ->
          aux a parent
      | _ ->
          false
    in
    aux (get t ha) (get t hb)

  type update = {new_head: hash; extension: node list}
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
          Fmt.error_msg "Missing %a" Int.pp par
      | (_, par, _) :: _ as extension ->
          let rec aux (ctree : parent_ref_node option Map.M(Int).t)
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

  let make_key =
    let hash = [%hash: int * Value.t] in
    fun parent value ->
      let pk = get_key_of_node parent in
      hash (pk, value)

  let addv t ~node:_ ~parent vi =
    let parent_key = parent in
    let parent_node = get t parent_key in
    let ctree = t.ctree in
    let ctree, _, new_head =
      IterLabels.fold vi ~init:(ctree, parent_node, parent_key)
        ~f:(fun (ctree, parent, parent_key) value ->
          let key = make_key parent value in
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
    let ctree = Map.empty (module Int) |> Map.set ~key:root_clock ~data:None in
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
    let filter_to_leq_idx idx (node) =
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
    (* TODO optimise by using locally allocated lists => cheap *)
    let hds = Array.of_list hds in
    let counts = Hashtbl.create ~size:(Array.length hds) (module Int) in
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

  let path_between t rt_vc hd_vc =
    let rt, hd = (get t rt_vc, get t hd_vc) in
    let rec aux curr acc : node list =
      match (curr, rt) with
      | None, Some _ ->
          Fmt.invalid_arg "%a not on path to %a" Int.pp rt_vc Int.pp hd_vc
      | None, None ->
          acc
      | Some curr, Some rt when [%equal: Int.t] curr.key rt.key ->
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
      | _ when par = c ->
          Set.add acc c
      | _ when Set.mem acc par ->
          Fmt.failwith "Loop detected around %a" Int.pp c
      | _ ->
          path_to_root t par (Set.add acc c)
    in
    Map.key_set t.ctree
    |> Set.fold
         ~init:(Set.empty (module Int))
         ~f:(fun visited vc ->
           if Set.mem visited vc then visited
           else Set.union visited (path_to_root t vc (Set.empty (module Int)))
           )
    |> ignore
end

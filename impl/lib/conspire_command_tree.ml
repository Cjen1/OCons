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

module type Value = sig
  type t [@@deriving compare, show, bin_io]
end

module CommandTree (Value : Value) = struct
  (* Map of vector clocks to values
     Aim to replicate this to other nodes

     So changes to send:
       Additional heads (branches)
       Extensions to heads
  *)

  type node = int * VectorClock.t * Value.t [@@deriving show, bin_io]

  type parent_ref_node =
    {node: node; parent: parent_ref_node option [@opaque]; vc: VectorClock.t}
  [@@deriving show {with_path= false}]

  type t =
    { ctree: parent_ref_node option Map.M(VectorClock).t
          [@printer
            map_pp VectorClock.pp
              (Fmt.option ~none:(Fmt.any "Root") pp_parent_ref_node)]
    ; root: VectorClock.t }
  [@@deriving show {with_path= false}]

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

  let rec get_prev_term_clock t clock =
    match Map.find_exn t.ctree clock with
    | None ->
        None
    | Some {node= _, parent, _; _} when parent.term < clock.term ->
        Some parent
    | Some {node= _, parent, _; _} ->
        get_prev_term_clock t parent

  let rec prefix t (c1 : VectorClock.t) (c2 : VectorClock.t) =
    match () with
    | _ when c1.term = c2.term ->
        VectorClock.leq c1 c2
    | _ when c1.term > c2.term ->
        false
    | _ ->
        assert (c1.term < c2.term) ;
        let c2' = get_prev_term_clock t c2 in
        Option.value_map c2' ~f:(fun c2' -> prefix t c1 c2') ~default:false

  let compare t (c1 : VectorClock.t) (c2 : VectorClock.t) =
    match () with
    | _ when c1.term < c2.term ->
        Option.some_if (prefix t c1 c2) (-1)
    | _ when c1.term > c2.term ->
        Option.some_if (prefix t c2 c1) 1
    | _ ->
        assert (c1.term = c2.term) ;
        VectorClock.compare_clock_po c1 c2

  type update = {new_head: VectorClock.t; extension: node list}
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
          Fmt.error_msg "Missing %a" VectorClock.pp par
      | (_, par, _) :: _ as extension ->
          let rec aux (ctree : parent_ref_node option Map.M(VectorClock).t)
              (extension : node list) parent =
            match extension with
            | [] ->
                ctree
            | [a] ->
                Map.set ctree ~key:update.new_head
                  ~data:(Some {node= a; parent; vc= update.new_head})
            | a :: ((_, clk, _) :: _ as rem) ->
                let node = Some {node= a; parent; vc= clk} in
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

  let addv t ~node ~(parent : VectorClock.t) ?(term = parent.term) vi =
    let idx0 = get_idx t parent in
    let _, new_head, rev_extension =
      IterLabels.fold vi ~init:(idx0, parent, [])
        ~f:(fun (idx, parent, extension) v ->
          let clk = VectorClock.succ ~term parent node in
          let idx = idx + 1 in
          (idx, clk, (idx, parent, v) :: extension) )
    in
    let update = {new_head; extension= List.rev rev_extension} in
    (apply_update_exn t update, new_head)

  let make_update t target_node (other : t) =
    let rec aux curr acc =
      match curr with
      | None ->
          acc
      | Some {vc; _} when Map.mem other.ctree vc ->
          acc
      | Some {parent; node; _} ->
          aux parent (node :: acc)
    in
    {new_head= target_node; extension= aux (get t target_node) []}

  let copy_update src dst (upto : VectorClock.t) =
    let rec aux dsttree curr =
      match curr with
      | None ->
          dsttree
      | Some {vc; _} when Map.mem dst.ctree vc ->
          dsttree
      | Some {node= _; parent; vc} ->
          let dctree' = Map.set dsttree ~key:vc ~data:curr in
          aux dctree' parent
    in
    {dst with ctree= aux dst.ctree (get src upto)}

  let create nodes t0 =
    let root_clock = VectorClock.empty nodes t0 in
    let ctree =
      Map.empty (module VectorClock) |> Map.set ~key:root_clock ~data:None
    in
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
    let counts = Hashtbl.create ~size:(Array.length hds) (module VectorClock) in
    let rec aux idx =
      Hashtbl.clear counts ;
      Array.map_inplace hds ~f:(filter_to_leq_idx idx) ;
      Array.iter hds ~f:(function
        | None ->
            Hashtbl.incr counts t.root
        | Some {vc; _} ->
            Hashtbl.incr counts vc ) ;
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
          Fmt.invalid_arg "%a not on path to %a" VectorClock.pp rt_vc
            VectorClock.pp hd_vc
      | None, None ->
          acc
      | Some curr, Some rt when [%equal: VectorClock.t] curr.vc rt.vc ->
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
      | _ when VectorClock.equal par c ->
          Set.add acc c
      | _ when Set.mem acc par ->
          Fmt.failwith "Loop detected around %a" VectorClock.pp c
      | _ ->
          path_to_root t par (Set.add acc c)
    in
    Map.key_set t.ctree
    |> Set.fold
         ~init:(Set.empty (module VectorClock))
         ~f:(fun visited vc ->
           if Set.mem visited vc then visited
           else
             Set.union visited
               (path_to_root t vc (Set.empty (module VectorClock))) )
    |> ignore
end

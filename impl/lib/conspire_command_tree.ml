open! Core
open! Types
open! Utils

let map_pp kpp vpp : _ Map.t Fmt.t =
 fun ppf v ->
  Fmt.pf ppf "%a"
    Fmt.(
      brackets @@ list ~sep:(Fmt.any ":@ ") @@ parens @@ pair ~sep:comma kpp vpp )
    (Map.to_alist v)

let set_pp pp : _ Set.t Fmt.t =
 fun ppf v ->
  Fmt.pf ppf "%a" Fmt.(brackets @@ list ~sep:comma @@ pp) (Set.to_list v)

module VectorClock = struct
  module T = struct
    type t = {term: int; clock: int Map.M(Int).t}
    [@@deriving sexp, bin_io, compare, hash]

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
  end

  include T

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

  (*  *)

  type node = int * VectorClock.t * Value.t [@@deriving show, bin_io]

  type parent_clock_node = node [@@deriving show, bin_io]

  type t =
    { ctree: parent_clock_node option Map.M(VectorClock).t
          [@printer
            map_pp VectorClock.pp
              (Fmt.option ~none:(Fmt.any "Root") pp_parent_clock_node)] }
  [@@deriving show {with_path= false}]

  let get_idx t clk =
    match Map.find_exn t.ctree clk with None -> 0 | Some (idx, _, _) -> idx

  let get_value t clk =
    match Map.find_exn t.ctree clk with
    | None ->
        None
    | Some (_, _, v) ->
        Some v

  let get_parent t clock =
    match Map.find_exn t.ctree clock with
    | None ->
        clock
    | Some (_, parent, _) ->
        parent

  let rec get_prev_term_clock t clock =
    match Map.find_exn t.ctree clock with
    | None ->
        None
    | Some (_, parent, _) when parent.term < clock.term ->
        Some parent
    | Some (_, parent, _) ->
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

  type update = {new_head: VectorClock.t; extension: parent_clock_node list}
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
      | extension ->
          let rec aux ctree (extension : parent_clock_node list) =
            match extension with
            | [] ->
                ctree
            | [a] ->
                Map.set ctree ~key:update.new_head ~data:(Some a)
            | a :: ((_, clk, _) :: _ as rem) ->
                let ctree = Map.set ctree ~key:clk ~data:(Some a) in
                aux ctree rem
          in
          let ctree = aux t.ctree extension in
          Ok {ctree} )

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
      match Map.find_exn t.ctree curr with
      | None ->
          acc
      | Some ((_, c, _) as v) when Map.mem other.ctree c ->
          v :: acc
      | Some ((_, c, _) as v) ->
          aux c (v :: acc)
    in
    {new_head= target_node; extension= aux target_node []}

  let rec copy_update src dst upto =
    match Map.find_exn src.ctree upto with
    | _ when Map.mem dst.ctree upto ->
        dst
    | None ->
        (* root already there *)
        dst
    | Some (_, par, _) as v ->
        let dctree' = Map.set dst.ctree ~key:upto ~data:v in
        copy_update src {ctree= dctree'} par

  let create nodes t0 =
    let root_clock : VectorClock.t =
      { term= t0
      ; clock=
          Map.of_alist_exn (module Int) (List.map nodes ~f:(fun n -> (n, 0))) }
    in
    let ctree =
      Map.empty (module VectorClock) |> Map.set ~key:root_clock ~data:None
    in
    {ctree}

  let greatest_sufficiently_common_prefix t clks threshold =
    (* reduces the maximum index by one *)
    let prev clks =
      let longest_idx =
        List.fold clks ~init:(-1) ~f:(fun longest clk ->
            max longest @@ get_idx t clk )
      in
      List.map clks ~f:(fun clk ->
          if get_idx t clk >= longest_idx then get_parent t clk else clk )
    in
    let rec aux clks =
      let sorted_clks = List.sort clks ~compare:[%compare: VectorClock.t] in
      let max, count, _, _ =
        List.fold sorted_clks
          ~init:(List.hd_exn sorted_clks, 0, List.hd_exn sorted_clks, 0)
          ~f:(fun (mv, mc, x, count) v ->
            let x, count =
              if [%equal: VectorClock.t] x v then (x, count + 1) else (v, 1)
            in
            let mv, mc = if count > mc then (x, count) else (mv, mc) in
            (mv, mc, x, count) )
      in
      if count >= threshold then max else aux (prev clks)
    in
    aux clks

  let mem t vc = Map.mem t.ctree vc

  let path_between t rt hd : VectorClock.t list =
    let rec aux vc acc =
      match get_parent t vc with
      | _ when [%equal: VectorClock.t] vc rt ->
          vc :: acc
      | par when [%equal: VectorClock.t] par vc ->
          Fmt.failwith "%a not on path to %a" VectorClock.pp rt VectorClock.pp
            vc
      | par ->
          aux par (vc :: acc)
    in
    aux hd []

  let just_path_to t tar =
    let rec aux c acc =
      match Map.find_exn t.ctree c with
      | None ->
          (c, None) :: acc
      | Some (_, par, _) as n ->
          aux par ((c, n) :: acc)
    in
    let path = aux tar [] in
    {ctree= Map.of_alist_exn (module VectorClock) path}

  let tree_invariant t =
    let rec path_to_root t c acc =
      let par = get_parent t c in
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

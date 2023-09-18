open! Core
open! Types
open! Utils

module VectorClock = struct
  module T = struct
    type t = {term: int; clock: int Map.M(Int).t}
    [@@deriving sexp, bin_io, compare]
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

  include Core.Comparable.Make (T)
end

module type Value = sig
  type t [@@deriving compare]
end

module CommandTree (Value : Value) = struct
  (* Map of vector clocks to values
     Aim to replicate this to other nodes

     So changes to send:
       Additional heads (branches)
       Extensions to heads
  *)

  (*  *)

  type node =
    | Extension of VectorClock.t * Value.t
    | TermChange of VectorClock.t

  type parent_clock_node = node

  type t =
    { ctree: parent_clock_node option Map.M(VectorClock).t
    ; clock_has_child: Set.M(VectorClock).t
    ; heads: Set.M(VectorClock).t
    ; local_clock: term * int }

  let rec get_prev_term_clock t clock =
    match Map.find_exn t.ctree clock with
    | Some (TermChange parent) ->
        Some parent
    | Some (Extension (parent, _)) ->
        get_prev_term_clock t parent
    | None ->
        None

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

  type current_clock_node = node

  type update = {head: VectorClock.t; extension: current_clock_node list}

  let apply_update t update =
    match List.last update.extension with
    | None ->
        t
    (* already have extension *)
    | Some (Extension (c, _) | TermChange c) when Map.mem t.ctree c ->
        t
    (* extension is new *)
    | Some (Extension (new_head, _) | TermChange new_head) ->
        let _, ctree =
          List.fold update.extension ~init:(update.head, t.ctree)
            ~f:(fun (parent, ctree) node ->
              match node with
              | Extension (clock, value) ->
                  let ctree' =
                    Map.set ctree ~key:clock
                      ~data:(Option.some @@ Extension (parent, value))
                  in
                  (clock, ctree')
              | TermChange clock ->
                  let ctree' =
                    Map.set ctree ~key:clock
                      ~data:(Option.some @@ TermChange parent)
                  in
                  (clock, ctree') )
        in
        let _, clock_has_child =
          List.fold update.extension ~init:(update.head, t.clock_has_child)
            ~f:(fun (parent, chc) node ->
              match node with
              | Extension (clock, _) | TermChange clock ->
                  let chc' = Set.add chc parent in
                  (clock, chc') )
        in
        let heads = Set.add (Set.remove t.heads update.head) new_head in
        {t with ctree; clock_has_child; heads}

  (* remote_heads is what has been sent already *)
  let make_update t target_node remote_heads =
    let rec aux curr acc =
      match Map.find_exn t.ctree curr with
      | None ->
          (curr, List.rev acc)
      | Some (Extension (c, _) | TermChange c) when Set.mem remote_heads c ->
          (curr, List.rev acc)
      | Some (Extension (c, v)) ->
          aux c (Extension (curr, v) :: acc)
      | Some (TermChange c) ->
          aux c (TermChange curr :: acc)
    in
    let head, extension = aux target_node [] in
    {head; extension}
end

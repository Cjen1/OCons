open Core
open Types
open Utils
open C.Types
open Actions_f
open Ocons_core.Consensus_intf
module IdMap = Iter.Map.Make (Int)

module Value = struct
  type t = command list [@@deriving sexp, hash, bin_io]

  let compare = List.compare (fun a b -> Int.compare a.Command.id b.Command.id)

  let init x = [x]

  let rec union x y =
    match (x, y) with
    | x :: xs, y :: ys when [%compare.equal: command] x y ->
        x :: union xs ys
    | x :: xs, y :: ys when Types.hash_command x < Types.hash_command y ->
        x :: union xs (y :: ys)
    | x :: xs, y :: ys ->
        y :: union (x :: xs) ys
    | xs, [] ->
        xs
    | [], ys ->
        ys

  include (val Comparator.make ~compare ~sexp_of_t)
end

module ProposerSM = struct
  type value = Value.t [@@deriving bin_io]

  type vote = {value: value; term: term}
  type proposal = vote

  type t =
    { committed: value Option.t
    ; term: term
    ; votes: vote Map.M(Int).t
    ; counts: int Map.M(Value).t
    ; uncommitted_vals: Set.M(Value).t }

  let rec handle_message config t src (vote : vote) =
    match vote with
    | _ when Option.is_some t.committed ->
        (t, None)
    | _ when vote.term < t.term ->
        (t, None)
    | {term; value} when term > t.term ->
        check_response config
          { committed= None
          ; term
          ; votes= Map.singleton (module Int) src vote
          ; counts= Map.singleton (module Value) value 1
          ; uncommitted_vals= Set.singleton (module Value) value }
          value
    | {term; _} when Map.mem t.votes src ->
        assert (term = t.term) ;
        (t, None)
    | {term; value} ->
        assert (term = t.term) ;
        check_response config
          { committed= None
          ; term
          ; votes= Map.set t.votes ~key:src ~data:vote
          ; counts=
              Map.update t.counts vote.value ~f:(function
                | None ->
                    1
                | Some c ->
                    c + 1 )
          ; uncommitted_vals= Set.add t.uncommitted_vals vote.value }
          value

  and check_response config t changed_value =
    match Map.find_exn t.counts changed_value with
    | c when c >= config.phase2quorum ->
        ({t with committed= Some changed_value}, None)
    (* could not commit c *)
    | c when c + (config.num_nodes - Map.length t.votes) < config.phase2quorum
      ->
        try_propose
          {t with uncommitted_vals= Set.remove t.uncommitted_vals changed_value}
    | _ ->
        (t, None)

  and try_propose = function
    | t when Set.is_empty t.uncommitted_vals ->
        (t, assert false)
    | t ->
        (t, None)
end

(* Run proposer sm if placed request in it *)
(* Option for perf do bandle optimisation *)
(* Indulgent algorithms *)

module Types = struct
  type value = Value.t [@@deriving bin_io]

  let compare_value = List.compare Types.Command.compare

  type config =
    { node_id: node_id
    ; replica_ids: node_id list
    ; replica_count: int
    ; quorum_size: int
    ; fd_timeout: int
    ; max_outstanding: log_index }

  let make_config ~node_id ~replica_ids ~fd_timeout ?(max_outstanding = 8192) ()
      : config =
    let floor f = f |> Int.of_float in
    assert (List.mem replica_ids node_id ~equal:Int.equal) ;
    let replica_count = List.length replica_ids in
    let quorum_size = floor (2. *. Float.of_int replica_count /. 3.) + 1 in
    assert (3 * quorum_size > 2 * replica_count) ;
    { node_id
    ; replica_ids
    ; replica_count
    ; quorum_size
    ; fd_timeout
    ; max_outstanding }
end

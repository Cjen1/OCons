open! Core
open! Types
open! Async

module type ImmutableStore = sig
  include Owal.Persistable

  val get_index : t -> log_index -> (log_entry, exn) result

  val reset_ops : t -> t

  val get_ops : t -> op list

  val get_max_index : t -> log_index
end

module Make (I : ImmutableStore) : sig
  type wal

  type t = {mutable state: I.t; wal: wal} [@@deriving sexp_of, accessors]

  val get_state : t -> I.t

  val get_index : t -> log_index -> (log_entry, exn) result

  val update : t -> I.t -> [`SyncPossible | `NoSync]

  val of_path : ?file_size:int64 -> string -> t Deferred.t

  val datasync : t -> log_index Deferred.t

  val close : t -> unit Deferred.t
end = struct
  module P = Owal.Persistant (I)

  type wal = P.t

  type t = {mutable state: I.t; wal: wal} [@@deriving accessors]

  let get_state t = t.state

  let sexp_of_t t = [%message (t.state : I.t)]

  let get_index t index = I.get_index(get_state t) index

  let update t i' =
    t.state <- I.reset_ops i' ;
    match I.get_ops i' with
    | [] ->
        `NoSync
    | ops ->
        (* get oldest to newest ordering of ops *)
        List.iter ~f:(P.write t.wal) (List.rev ops) ;
        `SyncPossible

  let of_path ?file_size path =
    let%map wal, state = P.of_path ?file_size path in
    {state; wal}

  let datasync t =
    let max_index = I.get_max_index t.state in
    let%bind () = P.datasync t.wal in
    return max_index

  let close t = P.close t.wal
end

(* Test that Immutable_store :> ImmutableStore *)
let () = 
  let module _ = Make(Immutable_store) in
  ()

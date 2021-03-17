open! Core
open! Async

val logger : Log.t

module type Persistable = sig
  type t

  val init : unit -> t

  type op [@@deriving bin_io, sexp]

  val apply : t -> op -> t
end

module Persistant (P : Persistable) : sig
  type t

  val of_path : ?file_size:int64 -> string -> (t * P.t) Deferred.t

  val write : t -> P.op -> unit

  val datasync : t -> unit Deferred.t

  val close : t -> unit Deferred.t
end

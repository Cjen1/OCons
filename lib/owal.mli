open! Core
open! Async

val logger : Log.t

module type Persistable = sig
  type t

  val init : unit -> t

  type op [@@deriving bin_io]

  val apply : t -> op -> t
end

module Persistant (P : Persistable) : sig
  type t

  val of_path_async : ?file_size:int -> string -> (t * P.t) Deferred.t

  val write : t -> P.op -> unit

  val datasync : t -> unit

  val flush : t -> unit

  val close : t -> unit
end

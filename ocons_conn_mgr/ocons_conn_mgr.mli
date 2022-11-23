open Eio

type t

type resolver = unit -> Flow.two_way

type id = int

val create : sw:Switch.t -> (int * resolver) list -> t

val broadcast : ?max_fibers:int -> t -> Cstruct.t -> unit

val send : ?blocking:bool -> t -> id -> Cstruct.t -> unit

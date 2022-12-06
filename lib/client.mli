(*open! Core
open! Async
open Types

val logger : Async_unix.Log.t
(** Logging source *)

type t

val op_read : t -> bytes -> op_result Deferred.t
(** [op_read t k] reads the value stored in [k] it will retry until there is a value stored there *)

val op_write : t -> k:bytes -> v:bytes -> op_result Deferred.t
(** [op_write t k v] writes [v] to key [k] *)

val op_cas :
  t -> key:bytes -> value:bytes -> value':bytes -> op_result Deferred.t
(** [op_cas t k v v'] writes [v'] to key [k] if the previous value is [v] *)

val new_client : ?retry_delay:Time.Span.t -> string list -> t
(** [new_client addresses] Creates a new client connected to the servers listed in [addresses]. If a server is unreachable it will keep trying to reconnect to it. *)

val close : t -> unit Deferred.t
(** [close t] closes all outgoing connections that t has. *)
*)

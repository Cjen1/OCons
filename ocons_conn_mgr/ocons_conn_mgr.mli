open Eio

type 'a t

type 'a iter = ('a -> unit) -> unit

type resolver = Switch.t -> Flow.two_way_ty Resource.t

type id = int

type 'a kind = Iter of (id * 'a -> unit) | Recv of {max_recv_buf: int}

val create :
     ?kind:'a kind
  -> ?use_domain:Eio.Domain_manager.ty Eio.Domain_manager.t
  -> ?connected:unit Promise.t * unit Promise.u
  -> sw:Switch.t
  -> (id * resolver) list
  -> (Buf_read.t -> 'a)
  -> (unit -> unit)
  -> 'a t

val send : ?blocking:bool -> 'a t -> id -> Cstruct.t -> unit

val broadcast : ?max_fibers:int -> 'a t -> Cstruct.t -> unit

val send_blit :
  ?blocking:bool -> 'a t -> id -> (Eio.Buf_write.t -> unit) -> unit

val broadcast_blit :
  ?max_fibers:int -> 'a t -> (Eio.Buf_write.t -> unit) -> unit

val recv_any : ?force:bool -> 'a t -> (id * 'a) iter

val flush_all : 'a t -> unit

val close : 'a t -> unit

val set_debug_flag : unit -> unit

val can_recv : 'a t -> bool

module PCon = Persistent_conn

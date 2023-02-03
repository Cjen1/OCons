open Eio

type 'a t

type 'a iter = ('a -> unit) -> unit

type resolver = Switch.t -> Flow.two_way

type id = int

val create :
     ?max_recv_buf:id
  -> ?connected:((unit Promise.t * unit Promise.u))
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

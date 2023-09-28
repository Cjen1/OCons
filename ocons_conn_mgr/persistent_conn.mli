type t

type resolver = Eio.Switch.t -> Eio.Flow.two_way_ty Eio.Flow.two_way

val is_open : t -> bool

val send : ?block_until_open:bool -> t -> Cstruct.t -> unit

val send_blit : ?block_until_open:bool -> t -> (Eio.Buf_write.t -> unit) -> unit

val recv : ?default:'a -> t -> (Eio.Buf_read.t -> 'a) -> 'a

val recv_iter : t -> 'a Eio.Buf_read.parser -> ('a -> unit) -> unit

val create :
     ?connected:unit Eio.Promise.t * unit Eio.Promise.u
  -> sw:Eio.Switch.t
  -> resolver
  -> (unit -> unit)
  -> t

val flush : t -> unit

val close : t -> unit

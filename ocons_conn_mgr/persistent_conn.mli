type t

val is_open : t -> bool

val send : ?block_until_open:bool -> t -> Cstruct.t -> unit

val send_blit : ?block_until_open:bool -> t -> (Eio.Buf_write.t -> unit) -> unit

val recv : ?default:'a -> t -> (Eio.Buf_read.t -> 'a) -> 'a

val create : sw:Eio.Switch.t -> (unit -> Eio.Flow.two_way) -> t

val flush : t -> unit

val close : t -> unit

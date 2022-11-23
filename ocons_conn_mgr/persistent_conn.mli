  type t
  val is_open : t -> bool
  val send : ?block_until_open : bool -> t -> Cstruct.t -> unit
  val recv : ?default : 'a -> t -> (Eio.Buf_read.t -> 'a) -> 'a
  val create: sw:Eio.Switch.t -> (unit -> Eio.Flow.two_way) -> t

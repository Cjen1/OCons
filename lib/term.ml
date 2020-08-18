open Odbutils.Owal
open Base

module Term = struct
  type t = int64

  let init () = Int64.of_int 0

  type op = int64

  let encode_blit v =
    let p_len = 8 in
    let blit buf ~offset = EndianBytes.LittleEndian.set_int64 buf offset v in
    (p_len, blit)

  let decode v ~offset = EndianBytes.LittleEndian.get_int64 v offset

  let apply _t op = op
end

module Term_wal = Persistant (Term)

type op = Term.op

let update t op = Term.apply t op, [op]

include Term_wal

type t_wal = Term_wal.t

type t = Term.t

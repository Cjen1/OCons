open Owal
open Base

module Term_t = struct
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

module Term = Persistant (Term_t)
include Term

let update_current_term v t = if Int64.(v = t.t) then t else Term.change v t

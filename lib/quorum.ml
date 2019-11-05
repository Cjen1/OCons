open Base

type 'a t = {add: 'a -> unit; sufficient: unit -> bool}
[@@deriving sexp]

let make_quorum threshold () =
  let set = Base.Hash_set.create (module String) in
  { add= Hash_set.add set
  ; sufficient= (fun () -> Hash_set.length set >= threshold) }

let majority all_options =
  let all = List.length all_options in
  let threshold = Int.of_float @@ Float.round_up (Float.of_int all /. 2.) in
  (make_quorum threshold, make_quorum threshold)

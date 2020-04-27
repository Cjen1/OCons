open Base

type 'a t = {add: 'a -> unit; sufficient: unit -> bool}

let make_quorum_set threshold () =
  let set = Base.Hash_set.create (module String) in
  { add= Hash_set.add set
  ; sufficient= (fun () -> Hash_set.length set >= threshold) }

let make_quorum threshold () =
  let xs = ref [] in
  let add x = xs := x :: !xs in
  {add; sufficient= (fun () -> List.length !xs >= threshold)}

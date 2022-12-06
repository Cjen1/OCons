module AStruct = struct
  type 'a t = {buf: 'a Array.t; off: int; len: int}

  let create ~default len = {buf= Array.init len (fun _ -> default); off= 0; len}

  let create_sub ~off ~len t =
    match () with
    | () when off + len > t.len ->
        raise @@ Invalid_argument "Length is out of bounds"
    | _ ->
        {t with off; len}

  let get t i = Array.get t.buf (t.off + i)

  let set t i v = Array.set t.buf (t.off + i) v

  let iter t ?(l = 0) ~h : 'a Iter.t =
   fun f ->
    for i = l to h do
      f (get t i)
    done
end

module RBuf = struct
  (** A ring buffer which allows elements to be pushed onto it efficiently
    * It supports truncation of everything below some index T
    * Accesses after T and below T + len are valid
    *)

  type 'a t =
    {buf: 'a Array.t; mutable vlo: int; mutable vhi: int; mutable vidx: int}

  (** [truncate t i] results in a buffer where i is the lowest valid index *)
  let truncate t i =
    t.vlo <- i ;
    t.vhi <- i + Array.length t.buf ;
    t.vidx <- max i t.vidx

  let check t i = assert (t.vlo <= i && i < t.vhi)

  let vidx_to_idx t i = i mod Array.length t.buf

  let get t i =
    check t i ;
    Array.get t.buf (vidx_to_idx t i)

  let set t i v =
    check t i ;
    Array.set t.buf (vidx_to_idx t i) v

  let iter t ?(lo = t.vlo) ?(hi = t.vhi - 1) : 'a Iter.t =
   fun f ->
    for i = lo to hi do
      f (get t i)
    done

  let pop t =
    if t.vidx > t.vlo then None
    else
      let v = get t t.vlo in
      truncate t (t.vlo + 1) ;
      Some v

  let pop_exn t =
    if t.vidx > t.vlo then raise @@ Invalid_argument "Popped past valid items" ;
    let v = get t t.vlo in
    truncate t (t.vlo + 1) ;
    v

  let pop_iter t ?(hi = t.vhi - 1) : 'a Iter.t =
    fun f ->
      for i = t.vlo to hi do
        f (get t i)
      done;
      truncate t t.vhi

  let push t v =
    set t t.vidx v ;
    t.vidx <- t.vidx + 1

  let create len default =
    {buf= Array.init len (fun _ -> default); vlo= 0; vhi= 0; vidx= 0}
end

module IntMap = Map.Make(struct type t = int ;; let compare = Int.compare end)

module Quorum = struct
  type t = {elts: unit IntMap.t; threshold: int}

  let empty threshold = {elts= IntMap.empty; threshold}

  let add v t = {t with elts = IntMap.add v () t.elts}

  let satisified t = IntMap.cardinal t.elts >= t.threshold
end

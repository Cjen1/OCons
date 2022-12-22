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

  let iter_unsafe t ?(lo = t.vlo) ?(hi = t.vhi - 1) () =
    let iter f =
      for i = lo to hi do
        f (get t i)
      done
    in
    (iter, hi - lo + 1)

  let iter t ?(lo = t.vlo) ?(hi = t.vhi - 1) () =
    iter_unsafe t ~lo:(max lo t.vlo) ~hi:(min hi (t.vhi - 1))

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
    done ;
    truncate t t.vhi

  let push t v =
    set t t.vidx v ;
    t.vidx <- t.vidx + 1

  let length t = t.vhi - t.vlo

  let space t = Array.length t.buf - length t

  let lowest t = t.vlo

  let highest t = t.vhi

  let create len default =
    {buf= Array.init len (fun _ -> default); vlo= 0; vhi= 0; vidx= 0}
end

module SegmentLog = struct
  type 'a t =
    { segmentsize: int
    ; mutable segments: 'a array list
    ; mutable allocated: int
    ; mutable vhi: int (* highest allocated index *)
    ; init: 'a }

  let allocate t i =
    let already_allocd = i < (t.allocated + 1) * t.segmentsize in
    let within_one_alloc = i < (t.allocated + 2) * t.segmentsize in
    match () with
    | () when already_allocd ->
        ()
    | () when within_one_alloc ->
        t.segments <- Array.make t.segmentsize t.init :: t.segments ;
        t.allocated <- t.allocated + 1
    | _ ->
        raise
          (Invalid_argument
             (Fmt.str
                "Could not allocate new segment for %d, already allocated %d" i
                t.allocated ) )

  let check t i =
    match () with
    | () when i < (t.allocated + 1) * t.segmentsize ->
        ()
    | _ ->
        raise
          (Invalid_argument
             (Fmt.str
                "Segment out of bounds: idx = %d, allocated_upto (non-inc) = %d"
                i
                (t.allocated * t.segmentsize) ) )

  let id_to_seg t i = t.allocated - Int.div i t.segmentsize

  let get t i =
    check t i ;
    Array.get (List.nth t.segments (id_to_seg t i)) (i mod t.segmentsize)

  let set t i v =
    allocate t i ;
    Array.set (List.nth t.segments (id_to_seg t i)) (i mod t.segmentsize) v ;
    if t.vhi < i then t.vhi <- i

  let iteri t ?(lo = 0) ?(hi = t.vhi) : (int * 'a) Iter.t =
    let lo = max 0 lo in
    fun f ->
      for i = lo to hi do
        f (i, get t i)
      done

  let iteri_len t ?(lo = 0) ?(hi = t.vhi) () : (int * 'a) Iter.t * int =
    ((fun f -> iteri t ~lo ~hi f), hi - lo + 1)

  let iter t ?(lo = 0) ?(hi = t.vhi) f = iteri t ~lo ~hi (fun (_, x) -> f x)

  let iter_len t ?(lo = 0) ?(hi = t.vhi) () : 'a Iter.t * int =
    let iter, len = iteri_len t ~lo ~hi () in
    ((fun f -> iter (fun (_, x) -> f x)), len)

  let add t v = set t (t.vhi + 1) v

  let mem t i = i <= t.vhi

  let highest t = t.vhi

  let copy t = {t with segments= t.segments |> List.map Array.copy}

  let cut_after t idx = t.vhi <- idx

  let create ?(segmentsize = 4096) init =
    {segmentsize; segments= []; allocated= -1; vhi= -1; init}
end

module IntMap = Map.Make (struct
  type t = int

  let compare = Int.compare
end)

module Quorum = struct
  type 'a t = {elts: 'a IntMap.t; threshold: int}

  let empty threshold = {elts= IntMap.empty; threshold}

  let add id e t = {t with elts= IntMap.add id e t.elts}

  let satisified t = IntMap.cardinal t.elts >= t.threshold

  let pp : _ t Fmt.t =
   fun ppf t ->
    Fmt.pf ppf "{threshold %d, elts: %a}" t.threshold
      Fmt.(brackets @@ list ~sep:(const string ", ") int)
      (t.elts |> IntMap.bindings |> List.map fst)
end

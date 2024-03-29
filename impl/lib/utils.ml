let traceln = Ocons_core.Utils.traceln

let dtraceln = Ocons_core.Utils.dtraceln

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
  open Core

  module SegTbl = struct
    open Core
    include Hashtbl.Make (Int)

    let equal equal t1 t2 =
      Hashtbl.length t1 = Hashtbl.length t2
      && Hashtbl.for_alli t1 ~f:(fun ~key ~data ->
             Hashtbl.mem t2 key && equal data (Hashtbl.find_exn t2 key) )
  end

  type 'a t =
    { segmentsize: int
    ; segments: 'a array SegTbl.t
    ; mutable allocated: int
    ; mutable vhi: int (* highest set index *)
    ; init: unit -> 'a [@equal.ignore] }
  [@@deriving equal]

  let allocate t i =
    let rec ensure_allocated i =
      let already_allocd () = i < (t.allocated + 1) * t.segmentsize in
      match () with
      | () when already_allocd () ->
          ()
      | () ->
          t.allocated <- t.allocated + 1 ;
          Core.Hashtbl.add_exn t.segments ~key:t.allocated
            ~data:(Array.init t.segmentsize ~f:(fun _ -> t.init ())) ;
          ensure_allocated i
    in
    ensure_allocated i ;
    if t.vhi < i then t.vhi <- i

  let allocate_next t = allocate t (t.vhi + 1)

  let is_alloc t i = i < (t.allocated + 1) * t.segmentsize

  let check t i =
    match () with
    | () when is_alloc t i ->
        ()
    | _ ->
        raise
          (Invalid_argument
             (Fmt.str
                "Segment out of bounds: idx = %d, allocated_upto (non-inc) = %d"
                i
                (t.allocated * t.segmentsize) ) )

  let id_to_seg t i = Int.(i / t.segmentsize)

  let get t i =
    check t i ;
    Array.get
      (Core.Hashtbl.find_exn t.segments (id_to_seg t i))
      (i mod t.segmentsize)

  let set t i v =
    allocate t i ;
    Array.set
      (Core.Hashtbl.find_exn t.segments (id_to_seg t i))
      (i mod t.segmentsize) v ;
    if t.vhi < i then t.vhi <- i

  let norm t lo hi =
    let lo = min lo t.vhi in
    let lo = max lo 0 in
    let hi = min hi t.vhi in
    let hi = max hi (-1) in
    (lo, hi)

  let to_seq t ?(lo = 0) ?(hi = t.vhi) : 'a Seq.t =
    let lo, hi = norm t lo hi in
    Seq.unfold (fun i -> if i <= hi then Some (get t i, i + 1) else None) lo

  let to_seqi t ?(lo = 0) ?(hi = t.vhi) : 'a Seq.t =
    let lo, hi = norm t lo hi in
    Seq.unfold
      (fun i -> if i <= hi then Some ((i, get t i), i + 1) else None)
      lo

  let iteri t ?(lo = 0) ?(hi = t.vhi) : (int * 'a) Iter.t =
    let lo, hi = norm t lo hi in
    fun f ->
      for i = lo to hi do
        f (i, get t i)
      done

  let iteri_len t ?(lo = 0) ?(hi = t.vhi) () : (int * 'a) Iter.t * int =
    let lo, hi = norm t lo hi in
    let iter f = iteri t ~lo ~hi f in
    let len = hi - lo + 1 in
    if not (Iter.length iter = len) then
      Fmt.invalid_arg "hi(%d) - lo(%d) + 1 = %d != len(%d)" hi lo len
        (Iter.length iter)
    else (iter, len)

  let iter t ?(lo = 0) ?(hi = t.vhi) f = iteri t ~lo ~hi (fun (_, x) -> f x)

  let iter_len t ?(lo = 0) ?(hi = t.vhi) () : 'a Iter.t * int =
    let iter, len = iteri_len t ~lo ~hi () in
    ((fun f -> iter (fun (_, x) -> f x)), len)

  let add t v = set t (t.vhi + 1) v

  let allocate_add t =
    allocate t (t.vhi + 1) ;
    get t t.vhi

  let mem t i = 0 <= i && i <= t.vhi

  let find t i = if mem t i then Some (get t i) else None

  let highest t = t.vhi

  let copy t =
    { segmentsize= t.segmentsize
    ; segments= Core.Hashtbl.map t.segments ~f:Array.copy
    ; allocated= t.allocated
    ; vhi= t.vhi
    ; init= t.init }

  let cut_after t idx = t.vhi <- min t.vhi idx

  let create ?(segmentsize = 4096) init =
    { segmentsize
    ; segments= SegTbl.create ()
    ; allocated= -1
    ; vhi= -1
    ; init= (fun () -> init) }

  let create_mut ?(segmentsize = 4096) init =
    {segmentsize; segments= SegTbl.create (); allocated= -1; vhi= -1; init}

  let map t i f = set t i (f (get t i))

  let pp ppv ppf t =
    Fmt.pf ppf "%a" Fmt.(brackets @@ list @@ ppv) (iter t |> Iter.to_list)
end

module CIDHashtbl = Hashtbl.Make (struct
  type t = int

  let equal = Int.equal

  let hash = Fun.id
end)

module IntMap = Map.Make (struct
  type t = int

  let compare = Int.compare
end)

module Quorum = struct
  type 'a t = {elts: 'a IntMap.t; threshold: int}

  let empty threshold = {elts= IntMap.empty; threshold}

  let add id e t = {t with elts= IntMap.add id e t.elts}

  let satisified t = IntMap.cardinal t.elts >= t.threshold

  let satisifed_value t =
    if IntMap.cardinal t.elts > t.threshold then Some t.elts else None

  let pp : _ t Fmt.t =
   fun ppf t ->
    Fmt.pf ppf "{threshold %d, elts: %a}" t.threshold
      Fmt.(brackets @@ list ~sep:(const string ", ") int)
      (t.elts |> IntMap.bindings |> List.map fst)
end

type comp = GT | EQ | LT [@@deriving show]

let comp cmp a b =
  let r = cmp a b in
  if r < 0 then LT else if r > 0 then GT else EQ

let singleton_iter = [%accessor Accessor.getter (fun s -> Iter.singleton s)]

let htbl_iter htbl f = Core.Hashtbl.iter htbl ~f

let htbl_iteri htbl f =
  Core.Hashtbl.iteri htbl ~f:(fun ~key ~data -> f key data)

let rec seq_zip seq_iter () =
  let open Core in
  if Iter.is_empty seq_iter then Seq.Nil
  else
    let vs = seq_iter |> Iter.filter_map Seq.uncons in
    Seq.Cons (Iter.map fst vs, seq_zip (Iter.map snd vs))

let assert_equal pp equal a b =
  if not (equal a b) then
    Fmt.failwith "inequal: @[<1>%a@]"
      Fmt.(pair ~sep:semi (braces pp) (braces pp))
      (a, b)

let pp_hashtbl comp ppk ppv ppf v =
  let open Core in
  Fmt.pf ppf "%a"
    Fmt.(brackets @@ list @@ parens @@ pair ppk ~sep:(any ":@ ") ppv)
    Core.(v |> Hashtbl.to_alist |> List.sort ~compare:comp)

open Core

let pp_map kpp vpp : _ Map.t Fmt.t =
 fun ppf v ->
  Fmt.pf ppf "%a"
    Fmt.(
      brackets @@ list ~sep:Fmt.semi @@ parens
      @@ pair ~sep:(Fmt.any ":@ ") kpp vpp )
    (Map.to_alist v)

let pp_set pp : _ Set.t Fmt.t =
 fun ppf v ->
  Fmt.pf ppf "%a" Fmt.(brackets @@ list ~sep:comma @@ pp) (Set.to_list v)

let float_to_time f =
  f |> Time_float_unix.Span.of_sec |> Time_float_unix.of_span_since_epoch

let time_to_float t =
  t |> Time_float_unix.to_span_since_epoch
  |> Time_float_unix.Span.to_proportional_float

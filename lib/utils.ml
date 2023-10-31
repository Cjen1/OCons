open Types

let debug_flag = false

let set_nodelay ?(should_warn = true) sock =
  match sock |> Eio_unix.Resource.fd_opt with
  | Some fd ->
      Eio_unix.Fd.use ~if_closed:ignore fd (fun fd ->
          Unix.setsockopt fd Unix.TCP_NODELAY true )
  | None ->
      if should_warn then
        Eio.traceln
          "WARNING: unable to set TCP_NODELAY, higher than required latencies \
           may be experienced"

let time_pp ppf t =
  let tm = Unix.localtime t in
  Fmt.pf ppf "%02d:%02d:%02d.%06d" tm.tm_hour tm.tm_min tm.tm_sec
    (int_of_float ((t -. floor t) *. 1_000_000.))

let traceln fmt =
  Eio.traceln ("@[<hov 1>%a: " ^^ fmt ^^ "@]") time_pp (Unix.gettimeofday ())

let ignore_format fmt = Format.ikfprintf ignore Fmt.stderr fmt

let dtraceln fmt = if debug_flag then traceln fmt else ignore_format fmt

let is_not_cancel = function Eio.Cancel.Cancelled _ -> false | _ -> true

let maybe_do ~energy ~f =
  let curr = ref energy in
  fun () ->
    if !curr <= 0 then (
      curr := energy ;
      f () ) ;
    curr := !curr - 1

let maybe_yield = maybe_do ~f:Eio.Fiber.yield

module Quorum = struct
  open! Core

  type 'a t = {elts: 'a list; n: int; threshold: int; eq: 'a -> 'a -> bool}
  [@@deriving sexp_of]

  let empty threshold eq = {elts= []; n= 0; threshold; eq}

  let add v t =
    if List.mem t.elts v ~equal:t.eq then Error `AlreadyInList
    else Ok {t with elts= v :: t.elts; n= t.n + 1}

  let satisified t = t.n >= t.threshold
end

module InternalReporter = struct
  type reporter_pp = time Fmt.t

  type reset = unit -> unit

  let reporters : (reporter_pp * reset * bool ref) list ref = ref []

  let register_reporter pps reset running =
    reporters :=
      pps |> List.map (fun pp -> (pp, reset, running)) |> List.append !reporters

  let run_report period =
    let pp_reporters =
      !reporters
      |> List.filter (fun (_, _, running) -> !running)
      |> List.map (fun (v, _, _) -> v)
    in
    Eio.traceln "---- Report ----" ;
    Eio.traceln "%a" (Fmt.record pp_reporters) period ;
    List.iter (fun (_, r, running) -> if !running then r ()) !reporters

  let run ~sw clock period =
    if period > 0. then
      Eio.Fiber.fork_daemon ~sw (fun () ->
          while true do
            Eio.Fiber.check () ;
            Eio.Time.sleep clock period ;
            run_report period
          done ;
          Eio.Fiber.await_cancel () )

  type 'a state_reporter = {mutable v: 'a; mutable v': 'a}

  type rate_counter = int state_reporter

  type 'a reporter = 'a -> unit

  let rate_reporter name : unit reporter * bool ref =
    let state = ref 0 in
    let reset () = state := 0 in
    let open Fmt in
    let pp =
      [ field name
          (fun p -> Core.Float.(Int.(to_float !state) / p))
          (float_dfrac 3) ]
    in
    let running = ref false in
    let update () = if !running then incr state in
    register_reporter pp reset running ;
    (update, running)

  let fold_reporter ~name ~f ~init ~pp : 'a reporter * bool ref =
    let state = ref [] in
    let reset () = state := [] in
    let open Fmt in
    let pp = [field name (fun p -> (p, List.fold_left f init !state)) pp] in
    let running = ref false in
    let update x = if !running then state := x :: !state in
    register_reporter pp reset running ;
    (update, running)

  type avg_reporter_state =
    { mutable max: float
    ; mutable tdigest: Tdigest.t
    ; mutable sum: float
    ; mutable count: int }

  let avg_reporter : 'a. ('a -> float) -> string -> 'a reporter * bool ref =
   fun conv name ->
    let state = {max= -1.; tdigest= Tdigest.create (); sum= 0.; count= 0} in
    let reset () =
      state.max <- -1. ;
      state.tdigest <- Tdigest.create () ;
      state.sum <- 0. ;
      state.count <- 0
    in
    let open Fmt in
    let pp_stats ppf s =
      let pp =
        let percentile s p =
          Tdigest.percentile s.tdigest p
          |> snd
          |> Core.Option.value ~default:Float.nan
        in
        record
          [ field "avg" (fun s -> s.sum /. Float.of_int s.count) (float_dfrac 3)
          ; field "#" (fun s -> s.count) int
          ; field "50%" (fun s -> percentile s 0.5) (float_dfrac 3)
          ; field "99%" (fun s -> percentile s 0.99) (float_dfrac 3)
          ; field "max" (fun s -> s.max) (float_dfrac 3) ]
      in
      pf ppf "%a" pp s
    in
    let print_enabled = ref false in
    let pp =
      [ field name
          (fun _ ->
            print_enabled := true ;
            state )
          pp_stats ]
    in
    let run_enabled = ref false in
    let update x =
      if !run_enabled && !print_enabled then (
        let dp = conv x in
        state.max <- max dp state.max ;
        state.tdigest <- Tdigest.add ~data:dp state.tdigest ;
        state.count <- state.count + 1 ;
        state.sum <- state.sum +. dp )
    in
    register_reporter pp reset run_enabled ;
    (update, run_enabled)

  let trace_reporter : string -> time reporter * bool ref =
   fun name ->
    let conv t = (Unix.gettimeofday () -. t) *. 1000. in
    avg_reporter conv name

  let command_trace_reporter : string -> Command.t reporter * bool ref =
   fun name ->
    let reporter, runner = trace_reporter name in
    ( (fun c ->
        let st = c.Command.trace_start in
        update_command_time c ; reporter st )
    , runner )
end

module TRACE = struct
  (* External_infra.accept_handler *)
  let cli_ex, run_cli_ex =
    InternalReporter.command_trace_reporter "TRACE:cli->ex"

  let ex_in, run_ex_in = InternalReporter.command_trace_reporter "TRACE:ex->in "

  let commit, run_commit =
    InternalReporter.command_trace_reporter "TRACE:commit "

  let in_ex, run_in_ex = InternalReporter.trace_reporter "TRACE:in->ex "

  let ex_cli, run_ex_cli = InternalReporter.trace_reporter "TRACE:ex->cli"
end

module MockSource = struct
  type t = {q: Cstruct.t Eio.Stream.t; mutable left_over: Cstruct.t option}

  let create q = {left_over= None; q}

  let single_read t buf =
    let copy_and_assign_rem data buf =
      match (Cstruct.length data, Cstruct.length buf) with
      | ld, lb when ld <= lb ->
          Cstruct.blit data 0 buf 0 ld ;
          ld
      | ld, lb ->
          Cstruct.blit data 0 buf 0 lb ;
          let rem = Cstruct.take ~min:(ld - lb) data in
          t.left_over <- Some rem ;
          lb
    in
    match t.left_over with
    | Some data ->
        copy_and_assign_rem data buf
    | None ->
        copy_and_assign_rem (Eio.Stream.take t.q) buf

  let read_methods = []
end

let make_source =
  let ops = Eio.Flow.Pi.source (module MockSource) in
  fun q -> Eio.Resource.T (MockSource.create q, ops)

module MockSink = struct
  type t = Cstruct.t Eio.Stream.t

  let single_write (t : t) bufs =
    List.iter (fun buf -> Eio.Stream.add t buf) bufs ;
    Core.List.sum (module Core.Int) bufs ~f:Cstruct.length

  let copy t ~src = Eio.Flow.Pi.simple_copy ~single_write t ~src
end

let make_sink =
  let ops = Eio.Flow.Pi.sink (module MockSink) in
  fun q -> Eio.Resource.T (q, ops)

let mock_flow () =
  let q = Eio.Stream.create 8 in
  (make_source q, make_sink q)

let prime nth =
  let is_prime primes n =
    let multiple_of n d = Int.div n d * d = n in
    List.for_all (fun p -> not (multiple_of n p)) primes
  in
  let unfold primes =
    let next =
      Iter.iterate Int.succ (Core.List.hd primes |> Core.Option.value ~default:1)
      |> Iter.drop 1
      |> Iter.find_pred_exn (is_prime primes)
    in
    Some (next, next :: primes)
  in
  Iter.unfoldr unfold [] |> Iter.drop (nth - 1) |> Iter.head_exn

let pp_hashtbl comp ppk ppv ppf v =
  let open Core in
  Fmt.pf ppf "%a"
    Fmt.(brackets @@ list @@ parens @@ pair ppk ~sep:(any ":@ ") ppv)
    Core.(v |> Hashtbl.to_alist |> List.sort ~compare:comp)

module RemovableQueue (T : sig
  module Index : Core.Hashtbl.Key

  val pp_index : Index.t Fmt.t

  type t [@@deriving sexp]

  val pp : t Fmt.t

  val to_index : t -> Index.t
end) : sig
  type t [@@deriving sexp, show]

  val create : unit -> t

  val add : t -> T.t -> unit

  val remove : t -> T.Index.t -> unit

  val take : t -> int -> T.t Core.Doubly_linked.t

  val invariant : t -> unit
end = struct
  open Core

  let pp_access =
    let comp (a, _) (b, _) = [%compare: T.Index.t] a b in
    pp_hashtbl comp T.pp_index (Fmt.any "ELT")

  let pp_queue ppf v =
    Fmt.pf ppf "%a"
      Fmt.(braces @@ list ~sep:comma T.pp)
      (Doubly_linked.to_list v)

  type t =
    { access: (T.t Doubly_linked.Elt.t[@sexp.opaque]) Hashtbl.M(T.Index).t
          [@printer pp_access]
    ; queue: T.t Doubly_linked.t [@printer pp_queue] }
  [@@deriving sexp, show]

  let invariant t : unit =
    Invariant.invariant [%here] t [%sexp_of: t] (fun () ->
        Doubly_linked.iteri t.queue ~f:(fun idx v ->
            let id = T.to_index v in
            if not (Hashtbl.mem t.access id) then
              Fmt.failwith "Unable to find (%d:%a) in accessor" idx T.pp_index
                id ) ;
        Hashtbl.iteri t.access ~f:(fun ~key ~data ->
            if not (Doubly_linked.mem_elt t.queue data) then
              Fmt.failwith "Unable to find elt for %a in queue" T.pp_index key ) )

  let create () =
    {access= Hashtbl.create (module T.Index); queue= Doubly_linked.create ()}

  let add t c =
    let id = T.to_index c in
    if not (Hashtbl.mem t.access id) then
      let elt = Doubly_linked.insert_last t.queue c in
      Hashtbl.add_exn t.access ~key:id ~data:elt

  let remove t id =
    match Hashtbl.find t.access id with
    | None ->
        ()
    | Some elt ->
        Doubly_linked.remove t.queue elt ;
        Hashtbl.remove t.access id

  let take t n =
    if Doubly_linked.is_empty t.queue then Doubly_linked.create ()
    else
      let taken, rest =
        Doubly_linked.partitioni_tf t.queue ~f:(fun i _ -> i < n)
      in
      Doubly_linked.clear t.queue ;
      Doubly_linked.transfer ~src:rest ~dst:t.queue ;
      Doubly_linked.iter taken ~f:(fun v ->
          Hashtbl.remove t.access (T.to_index v) ) ;
      taken
end

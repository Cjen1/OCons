open Types

let debug_flag = false

let set_nodelay ?(should_warn = true) sock =
  match sock |> Eio_unix.FD.peek_opt with
  | None when should_warn ->
      Eio.traceln
        "WARNING: unable to set TCP_NODELAY, higher than required latencies \
         may be experienced"
  | None ->
      ()
  | Some fd ->
      Unix.setsockopt fd Unix.TCP_NODELAY true

let dtraceln fmt =
  let ignore_format = Format.ikfprintf ignore Fmt.stderr in
  let traceln fmt =
    Eio.traceln ("@[<hov 1>%a: " ^^ fmt ^^ "@]") Time_unix.pp (Time_unix.now ())
  in
  if debug_flag then traceln fmt else ignore_format fmt

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

  let reporters : (reporter_pp * reset) list ref = ref []

  let register_reporter pps reset =
    reporters :=
      pps |> List.map (fun pp -> (pp, reset)) |> List.append !reporters

  let run_report period =
    let pp_reporters = !reporters |> List.map fst in
    Eio.traceln "---- Report ----" ;
    Eio.traceln "%a" (Fmt.record pp_reporters) period ;
    List.iter (fun (_, r) -> r ()) !reporters

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

  let rate_reporter init name : unit reporter =
    let state = {v= init; v'= init} in
    let reset () = state.v <- state.v' in
    let open Fmt in
    let pp =
      [ field name
          (fun p -> Core.Float.(Int.(to_float state.v' - to_float state.v) / p))
          float ]
    in
    let update () = state.v' <- state.v' + 1 in
    register_reporter pp reset ; update

  let fold_reporter ~name ~f ~init ~pp : 'a reporter =
    let state = ref [] in
    let reset () = state := [] in
    let open Fmt in
    let pp = [field name (fun p -> (p, List.fold_left f init !state)) pp] in
    let update x = state := x :: !state in
    register_reporter pp reset ; update

  type avg_reporter_state =
    { mutable max: float
    ; mutable tdigest: Tdigest.t
    ; mutable sum: float
    ; mutable count: int }

  let avg_reporter : 'a. ('a -> float) -> string -> 'a reporter =
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
          [ field "avg" (fun s -> s.sum /. Float.of_int s.count) float
          ; field "50%" (fun s -> percentile s 0.5) float
          ; field "99%" (fun s -> percentile s 0.99) float
          ; field "max" (fun s -> s.max) float ]
      in
      pf ppf "%a" pp s
    in
    let pp = [field name (fun _ -> state) pp_stats] in
    let update x =
      let dp = conv x in
      state.max <- max dp state.max ;
      state.tdigest <- Tdigest.add ~data:dp state.tdigest ;
      state.count <- state.count + 1 ;
      state.sum <- state.sum +. dp
    in
    register_reporter pp reset ; update

  let trace_reporter : string -> time reporter =
   fun name ->
    let conv t = (Unix.gettimeofday () -. t) *. 1000. in
    avg_reporter conv name

  let command_trace_reporter : string -> Command.t reporter =
   fun name ->
    let reporter = trace_reporter name in
    fun c ->
      let st = c.Command.trace_start in
      update_command_time c ; reporter st
end

module TRACE = struct
  (* External_infra.accept_handler *)
  let cli_ex = InternalReporter.command_trace_reporter "TRACE:cli->ex"

  let ex_in = InternalReporter.command_trace_reporter "TRACE:ex->in "

  let rep = InternalReporter.command_trace_reporter "TRACE:replicate"

  let rep_reply = InternalReporter.trace_reporter "TRACE:replicate_reply"

  let commit = InternalReporter.command_trace_reporter "TRACE:commit "

  let in_ex = InternalReporter.trace_reporter "TRACE:in->ex "

  let ex_cli = InternalReporter.trace_reporter "TRACE:ex->cli"
end

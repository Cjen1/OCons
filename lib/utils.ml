open Types

let debug_flag = false

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

let dtraceln fmt =
  let ignore_format = Format.ikfprintf ignore Fmt.stderr in
  if debug_flag then Eio.traceln fmt else ignore_format fmt

let is_not_cancel = function Eio.Cancel.Cancelled _ -> false | _ -> true

let maybe_do ~energy ~f =
  let curr = ref energy in
  fun () ->
    if !curr <= 0 then (
      curr := energy ;
      f () ) ;
    curr := !curr - 1

let maybe_yield = maybe_do ~f:Eio.Fiber.yield

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

  let avg_reporter : 'a. ('a -> float) -> string -> 'a reporter =
   fun conv name ->
    let state = ref [] in
    let reset () = state := [] in
    let open Fmt in
    let open Owl.Stats in
    let pp_stats ppf s =
      let pp =
        record
          [ field "avg" (fun s -> mean s) float
          ; field "50%" (fun s -> percentile s 50.) float
          ; field "99%" (fun s -> percentile s 99.) float
          ; field "max" (fun s -> max s) float ]
      in
      pf ppf "%a" pp s
    in
    let pp = [field name (fun _ -> !state |> Array.of_list) pp_stats] in
    let update x = state := conv x :: !state in
    register_reporter pp reset ; update
end

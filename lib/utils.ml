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

let dtraceln fmt = if debug_flag then Eio.traceln fmt else Fmt.kstr ignore fmt

let is_not_cancel = function Eio.Cancel.Cancelled _ -> false | _ -> true

let maybe_yield ~energy =
  let curr = ref energy in
  fun () ->
    if !curr <= 0 then (
      curr := energy ;
      Eio.Fiber.yield () ) ;
    curr := !curr - 1

module InternalReporter = struct
  type reporter_pp = time Fmt.t

  type reset = unit -> unit

  let reporters : (reporter_pp * reset) list ref = ref []

  let register_reporter pp reset = reporters := (pp, reset) :: !reporters

  let run_report period =
    let pp_reporters = !reporters |> List.map fst in
    Eio.traceln "---- Report ----";
    Eio.traceln "%a" (Fmt.record pp_reporters) period;
    List.iter (fun (_,r) -> r ()) (!reporters)

  let run ~sw clock period =
    Eio.Fiber.fork_daemon ~sw (fun () ->
        while true do
          Eio.Fiber.check () ;
          Eio.Time.sleep clock period ;
          run_report period
        done ;
        Eio.Fiber.await_cancel () )

  type rate_counter = {mutable v: int; mutable v': int}

  type 'a reporter = 'a -> unit

  let rate_counter init name : unit reporter =
    let state = {v= init; v'= init} in
    let reset () = state.v <- state.v' in
    let open Fmt in
    let pp =
      field name
        (fun p -> Core.Float.(Int.(to_float state.v' - to_float state.v) / p))
        float
    in
    let update () = state.v' <- state.v' + 1 in
    register_reporter pp reset ; update
end

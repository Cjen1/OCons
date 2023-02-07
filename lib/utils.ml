open! Core

module Quorum = struct
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

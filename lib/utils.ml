open! Core
open! Async

let src = Logs.Src.create "Utils" ~doc:"Utils"

module Log = (val Logs.src_log src : Logs.LOG)

(*
module Batcher = struct
  type 'a t =
    { batch_limit: int
    ; f: 'a list -> unit Lwt.t
    ; mutable dispatched: bool
    ; mutable outstanding: 'a list
    ; label: int }

  let auto_dispatch t = function
    | v when List.length t.outstanding + 1 >= t.batch_limit ->
        Log.debug (fun m -> m "Batcher: batch full, handling") ;
        let vs = v :: t.outstanding |> List.rev in
        t.outstanding <- [] ;
        t.f vs |> handle_failure "batcher"
    | v when not t.dispatched ->
        Log.debug (fun m -> m "Batcher: dispatching async handler") ;
        t.dispatched <- true ;
        t.outstanding <- v :: t.outstanding ;
        Lwt.async (fun () ->
            Lwt.pause ()
            >>= fun () ->
            let vs = t.outstanding |> List.rev in
            t.outstanding <- [] ;
            t.dispatched <- false ;
            t.f vs |> handle_failure "batcher" ;
            Lwt.return_unit)
    | v ->
        Log.debug (fun m -> m "Batcher: adding") ;
        t.outstanding <- v :: t.outstanding

  let create batch_limit ?(label = -1) f =
    {batch_limit; f; dispatched= false; outstanding= []; label}
end
*)

module Quorum = struct
  type 'a t = {elts: 'a list; n: int; threshold: int; eq: 'a -> 'a -> bool} [@@deriving sexp_of]

  let empty threshold eq = {elts= []; n= 0; threshold; eq}

  let add v t =
    if List.mem t.elts v ~equal:t.eq then Error `AlreadyInList
    else Ok {t with elts= v :: t.elts; n= t.n + 1}

  let satisified t = t.n >= t.threshold
end

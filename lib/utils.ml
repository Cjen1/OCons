open Base
open Lwt.Infix

let src = Logs.Src.create "Utils" ~doc:"Utils"

module Log = (val Logs.src_log src : Logs.LOG)

module DEBUG = struct
  let ( >>= ) p f =
    let handler e =
      Fmt.failwith "Got exn while resolving promise: %a" Fmt.exn e
    in
    Lwt.try_bind (fun () -> p) f handler
end

(* Returns f wrapped if either an exception occurrs in the evaluation of f() or in the promise *)
let catch f = try Lwt_result.catch (f ()) with exn -> Lwt.return_error exn

module Lookup = struct
  open Base

  type ('a, 'b) t = ('a, 'b) Base.Hashtbl.t

  let pp ppf _v = Stdlib.Format.fprintf ppf "Hashtbl"

  let get t key =
    Hashtbl.find t key
    |> Result.of_option ~error:(Invalid_argument "No such key")

  let get_exn t key = Result.ok_exn (get t key)

  let remove t key = Hashtbl.remove t key ; t

  let set t ~key ~data = Hashtbl.set t ~key ~data ; t

  let removep t p =
    Hashtbl.iteri t ~f:(fun ~key ~data -> if p data then remove t key |> ignore) ;
    t

  let create key_module = Base.Hashtbl.create key_module

  let fold t ~f ~init =
    Base.Hashtbl.fold t ~f:(fun ~key:_ ~data acc -> f data acc) ~init

  let find_or_add = Base.Hashtbl.find_or_add
end

module Lwt_queue = struct
  type 'a t =
    { q: 'a Stdlib.Queue.t
    ; cond: unit Lwt_condition.t
    ; switch: Lwt_switch.t option }

  let add t v =
    Stdlib.Queue.add v t.q ;
    Lwt_condition.signal t.cond ()

  let take t =
    let rec loop () =
      match (Stdlib.Queue.take_opt t.q, t.switch) with
      | _, Some switch when not (Lwt_switch.is_on switch) ->
          Lwt.return_error `Closed
      | Some v, _ ->
          Lwt.return_ok v
      | None, _ ->
          Lwt_condition.wait t.cond >>= fun () -> loop ()
    in
    loop ()

  let create ?switch () =
    let cond = Lwt_condition.create () in
    Lwt_switch.add_hook switch (fun () ->
        Lwt_condition.broadcast cond () |> Lwt.return) ;
    {q= Stdlib.Queue.create (); cond; switch}
end

let handle_failure str p =
  Lwt.on_failure p (fun exn ->
      Log.err (fun m -> m "Got error %a at %s" Fmt.exn exn str))

let handle_failure_result str p =
  let p =
    p
    >|= function
    | Ok () ->
        ()
    | Error exn ->
        Log.err (fun m -> m "Got error %a at %s" Fmt.exn exn str)
  in
  Lwt.async (fun () -> p)

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

module Quorum = struct
  type 'a t = {elts: 'a list; n: int; threshold: int; eq: 'a -> 'a -> bool}

  let empty threshold eq = {elts= []; n= 0; threshold; eq}

  let add v t =
    if List.mem t.elts v ~equal:t.eq then Error `AlreadyInList
    else Ok {t with elts= v :: t.elts; n= t.n + 1}

  let satisified t = t.n >= t.threshold
end

open Types
open Accessor.O
open Ocons_core.Consensus_intf

module type ActionSig = sig
  type t

  type message

  val send : node_id -> message -> unit

  val broadcast : message -> unit

  val t : ('i -> t -> t, 'i -> unit -> unit, [< A.field]) A.General.t

  val run_side_effects : (unit -> unit) -> t -> t * message action list
end

module type CTypes = sig
  type t

  val t_pp : t Fmt.t

  type message

  val message_pp : message Fmt.t

  val log :
    ( 'a -> log_entry Utils.SegmentLog.t -> log_entry Utils.SegmentLog.t
    , 'a -> t -> t
    , [< A.field] )
    A.General.t

  val commit_index : ('a -> term -> term, 'a -> t -> t, [< A.field]) A.General.t
end

module type ActionFunc = functor (C : CTypes) ->
  ActionSig
    with type t = C.t
     and type message = C.message

module ImperativeActions (C : CTypes) :
  ActionSig with type t = C.t and type message = C.message = struct
  include C

  type s =
    {mutable action_acc: message action list; mutable starting_cid: int; mutable t: t}

  let s = ref None

  let s_init t = {action_acc= []; starting_cid= t.@(commit_index); t}

  let send d m =
    (!s |> Option.get).action_acc <-
      Send (d, m) :: (!s |> Option.get).action_acc

  let broadcast m =
    (!s |> Option.get).action_acc <-
      Broadcast m :: (!s |> Option.get).action_acc

  let t =
    [%accessor
      A.field
        ~get:(fun () -> (!s |> Option.get).t)
        ~set:(fun () t' -> (!s |> Option.get).t <- t')]

  let get_actions init_commit_index =
    let open Iter in
    let commit_upto =
      let ct = (!s |> Option.get).t in
      if ct.@(commit_index) > (!s |> Option.get).starting_cid then
        Some ct.@(commit_index)
      else None
    in
    let make_command_iter upto =
      (* make an iter from lowest un-committed command upwards *)
      Log.iter (!s |> Option.get).t.@(log) ~lo:(init_commit_index + 1) ~hi:upto
      |> Iter.map (fun l -> l.command)
    in
    append_l
      [ of_list (!s |> Option.get).action_acc
      ; commit_upto |> of_opt |> Iter.map make_command_iter
        |> Iter.map (fun i -> CommitCommands i) ]
    |> Iter.to_rev_list

  let run_side_effects f t =
    s := Some (s_init t) ;
    let init_commit_index = t.@(commit_index) in
    f () ;
    ((!s |> Option.get).t, get_actions init_commit_index)
end

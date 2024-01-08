open! Core
open Types
module Time = Time_float_unix

let pp_time_float_unix : Time.t Fmt.t =
 fun ppf v -> Fmt.pf ppf "%0.5f" (Utils.time_to_float v)

module Value = struct
  let pp_command = Command.pp

  type t = command list * (Time.t[@printer pp_time_float_unix])
  [@@deriving compare, equal, hash, bin_io, sexp, show]

  let empty = ([], Time.epoch)
end

(*
  Use delay-reorder-buffer to avoid conflict with leader when backup pre-emptively proposes
 *)

module Counter = struct
  type t = {mutable count: int; limit: int} [@@deriving show]

  let incr_counter t = t.count <- t.count + 1

  let reset t = t.count <- 0

  let over_lim t = t.count >= t.limit
end

module Conspire = Conspire_f.Make (Value)
module FailureDetector = Conspire_leader.FailureDetector

module Types = struct
  type config =
    { conspire: Conspire_f.config
    ; lower_replica_ids: node_id list
    ; other_replica_ids: node_id list
    ; batching_interval: Time.Span.t
    ; delay_interval: Time.Span.t
    ; fd_timeout: int
    ; max_outstanding: int
    ; broadcast_tick_interval: int
    ; clock: float Eio.Time.clock_ty Eio.Time.clock [@opaque] }
  [@@deriving show {with_path= false}]

  let make_config ~node_id ~replica_ids ~delay_interval ~fd_timeout
      ~batching_interval ?(max_outstanding = 8192) ~broadcast_tick_interval
      clock : config =
    let floor f = f |> Int.of_float in
    let replica_count = List.length replica_ids in
    let quorum_size = floor (2. *. Float.of_int replica_count /. 3.) + 1 in
    assert (3 * quorum_size > 2 * replica_count) ;
    let other_replica_ids =
      List.filter replica_ids ~f:(fun i -> not (i = node_id))
    in
    let lower_replica_ids =
      List.filter replica_ids ~f:(fun id -> id < node_id)
    in
    let conspire =
      Conspire_f.
        {node_id; replica_ids; other_replica_ids; replica_count; quorum_size}
    in
    { conspire
    ; other_replica_ids
    ; lower_replica_ids
    ; delay_interval
    ; fd_timeout
    ; batching_interval
    ; max_outstanding
    ; broadcast_tick_interval
    ; clock= (clock :> float Eio.Time.clock_ty Eio.Time.clock) }

  type message =
    | Commands of (Command.t list * (Time.t[@printer pp_time_float_unix]))
    | Conspire of Conspire.message
  [@@deriving show, bin_io]

  type t =
    { config: config [@opaque]
    ; conspire: Conspire.t
    ; command_buffer: Command.t Delay_buffer.t
    ; tick_count: Counter.t
    ; failure_detector: FailureDetector.t
    ; clock: float Eio.Time.clock_ty Eio.Std.r [@opaque] }
  [@@deriving show {with_path= false}]

  let get_command idx t =
    if idx < 0 then Iter.empty
    else Log.get t.conspire.commit_log idx |> fst |> Iter.of_list

  let get_commit_index t = Log.highest t.conspire.commit_log

  module PP = struct
    let message_pp = pp_message

    let t_pp = pp

    let config_pp = pp_config
  end
end

module Make
    (Act : Actions_f.ActionSig
             with type t = Types.t
              and type message = Types.message) =
struct
  include Conspire
  include Types

  let is_leader t =
    List.for_all t.config.lower_replica_ids ~f:(fun nid ->
        not @@ FailureDetector.is_live t.failure_detector nid )

  let available_space_for_commands t = t.config.max_outstanding

  let send ?(force = false) t dst =
    let open Rep in
    let update = get_update_to_send t.conspire.rep dst in
    if force || Option.is_some update.ctree || Option.is_some update.cons then
      Act.send dst (Conspire (Ok update))

  let nack t dst =
    Act.send dst (Conspire (Error {commit= t.conspire.rep.state.commit_index}))

  let broadcast ?(force = false) t =
    List.iter t.config.other_replica_ids ~f:(fun nid -> send ~force t nid)

  let gather_batch_from_buffer t =
    let time = Eio.Time.now t.clock |> Utils.float_to_time in
    let batch_floor =
      CTree.get_value t.conspire.rep.store t.conspire.rep.state.vval
      |> Option.value_map ~default:Time.epoch ~f:snd
    in
    let batches =
      Delay_buffer.get_values t.command_buffer time
      |> Sequence.filter ~f:(fun (_, t) -> Time.(t > batch_floor))
    in
    Conspire.add_commands t.conspire
      (batches |> Sequence.iter |> Iter.from_labelled_iter)

  let handle_event t (event : message Ocons_core.Consensus_intf.event) =
    match event with
    | Tick ->
        (* Increment counters *)
        Counter.incr_counter t.tick_count ;
        FailureDetector.tick t.failure_detector ;
        if is_leader t then (
          (* Do recovery *)
          Conspire.conflict_recovery t.conspire |> ignore ;
          (* If leader add batch *)
          if t.conspire.rep.state.vterm = t.conspire.rep.state.term then
            gather_batch_from_buffer t ;
          broadcast t ) ;
        (* Broadcast occasionally *)
        if Counter.over_lim t.tick_count then (
          Counter.reset t.tick_count ; broadcast t ~force:true )
    | Commands ci ->
        let commands = Iter.to_list ci in
        let now = Eio.Time.now t.clock |> Utils.float_to_time in
        let hedged_target = Time.add now t.config.delay_interval in
        List.iter commands ~f:(fun c ->
            Delay_buffer.add_value t.command_buffer c hedged_target ) ;
        Act.broadcast (Commands (commands, hedged_target))
    | Recv (Commands (cs, tar), _) ->
        List.iter cs ~f:(fun c ->
            Delay_buffer.add_value t.command_buffer c tar )
    | Recv (Conspire m, src) -> (
        FailureDetector.reset t.failure_detector src ;
        let update_result = Conspire.handle_update_message t.conspire src m in
        match update_result with
        | Error `MustAck ->
            Act.traceln "Acking %d" src ;
            send t src
        | Error (`MustNack reason) ->
            Act.traceln "Nack for %d: %s" src
              ( match reason with
              | `Root_of_update_not_found _ ->
                  "Update is not rooted"
              | `Commit_index_not_in_tree ->
                  "Commit index not in tree"
              | `VVal_not_in_tree ->
                  "VVal not int tree" ) ;
            nack t src
        | Ok () ->
            process_acceptor_state t.conspire src ;
            let conflict_recovery_attempt =
              if is_leader t then Conspire.conflict_recovery t.conspire
              else Error `NotLeader
            in
            Result.iter conflict_recovery_attempt ~f:(fun () ->
                Utils.traceln "Recovery complete term: {t:%d,vt:%d}"
                  t.conspire.rep.state.term t.conspire.rep.state.vterm ) ;
            broadcast t ;
            let recovery_started =
              t.conspire.rep.state.term > t.conspire.rep.state.vterm
            in
            let committed =
              Conspire.check_commit t.conspire |> Option.is_some
            in
            let should_broadcast =
              Result.is_ok conflict_recovery_attempt
              || committed || recovery_started
            in
            if should_broadcast then broadcast t else send t src )

  let advance t e =
    let init_leader = is_leader t in
    let res =
      Act.run_side_effects
        (fun () -> Exn.handle_uncaught_and_exit (fun () -> handle_event t e))
        t
    in
    if is_leader t && not init_leader then
      Utils.traceln "Is now leader for %d" t.conspire.rep.state.term ;
    res

  let create (config : config) =
    let conspire = Conspire.create config.conspire in
    let command_buffer =
      Delay_buffer.create ~compare:Command.compare config.batching_interval
        (Eio.Time.now config.clock |> Utils.float_to_time)
    in
    let failure_detector =
      FailureDetector.create config.fd_timeout config.conspire.other_replica_ids
    in
    let tick_count =
      Counter.{count= 0; limit= config.broadcast_tick_interval}
    in
    { config
    ; conspire
    ; command_buffer
    ; tick_count
    ; clock= config.clock
    ; failure_detector }
end

module Impl = Make (Actions_f.ImperativeActions (Types))

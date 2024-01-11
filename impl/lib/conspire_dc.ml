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

(* Idea is that each client sends its message to a single node
   that node is the advocate for that request.
   The advocate hedges the time the message will arrive at the other nodes and sends the request with that time to them
   nodes recv this request and put it into a delay re-order buffer
   On each tick this buffer is examined and and msgs are batched by the millisecond
   If the time of a batch is greater than now then add batch to local log and update local and propose
   (so long as a batch with the relevant timestamp does not exist in the local log)
   This update should be sent to all replicas which are represented in the batch

   All nodes should then have batched the same requests, and advocate can immediately commit to client

   (all the while doing the recovery and commit messages as before)
*)

module Counter = struct
  type t = {mutable count: int; limit: int} [@@deriving show]

  let incr_counter t = t.count <- t.count + 1

  let reset t = t.count <- 0

  let over_lim t = t.count >= t.limit
end

module Conspire = Conspire_f.Make (Value)

module Types = struct
  type config =
    { conspire: Conspire_f.config
    ; other_replica_ids: node_id list
    ; batching_interval: Time.Span.t
    ; delay_interval: Time.Span.t
    ; max_outstanding: int
    ; tick_limit: int
    ; clock: float Eio.Time.clock_ty Eio.Time.clock [@opaque] }
  [@@deriving show {with_path= false}]

  let make_config ~node_id ~replica_ids ~delay_interval ~batching_interval
      ?(max_outstanding = 8192) ~tick_limit clock : config =
    let floor f = f |> Int.of_float in
    let replica_count = List.length replica_ids in
    let quorum_size = floor (2. *. Float.of_int replica_count /. 3.) + 1 in
    assert (3 * quorum_size > 2 * replica_count) ;
    let other_replica_ids =
      List.filter replica_ids ~f:(fun i -> not (i = node_id))
    in
    let conspire =
      Conspire_f.
        {node_id; replica_ids; other_replica_ids; replica_count; quorum_size}
    in
    { conspire
    ; other_replica_ids
    ; delay_interval
    ; batching_interval
    ; max_outstanding
    ; tick_limit
    ; clock= (clock :> float Eio.Time.clock_ty Eio.Time.clock) }

  type message =
    | Commands of (Command.t list * (Time.t[@printer pp_time_float_unix]))
    | Conspire of Conspire.message
  [@@deriving show, bin_io]

  type t =
    { config: config [@opaque]
    ; conspire: Conspire.t
    ; command_buffer:
        Command.t Delay_buffer.t (* TODO only reply to relevnat nodes *)
    ; tick_count: Counter.t
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

  let current_leader t =
    let leader_idx =
      t.conspire.rep.state.term % t.config.conspire.replica_count
    in
    List.nth_exn t.config.conspire.replica_ids leader_idx

  let can_apply_requests _t = true

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
        Counter.incr_counter t.tick_count ;
        if t.conspire.rep.state.vterm = t.conspire.rep.state.term then
          gather_batch_from_buffer t ;
        broadcast t ;
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
        let update_result = Conspire.handle_update_message t.conspire src m in
        match update_result with
        | Error `MustAck ->
            send t src
        | Error (`MustNack reason) ->
            ( match reason with
            | `Root_of_update_not_found _ ->
                Utils.dtraceln "Update is not rooted"
            | `Commit_index_not_in_tree ->
                Utils.dtraceln "Commit index not in tree"
            | `VVal_not_in_tree ->
                Utils.dtraceln "VVal not int tree" ) ;
            nack t src
        | Ok () ->
            process_acceptor_state t.conspire src ;
            let conflict_recovery_attempt =
              Conspire.conflict_recovery t.conspire
            in
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
    Act.run_side_effects
      (fun () -> Exn.handle_uncaught_and_exit (fun () -> handle_event t e))
      t

  let create (config : config) =
    let conspire = Conspire.create config.conspire in
    let command_buffer =
      Delay_buffer.create ~compare:Command.compare config.batching_interval
        (Eio.Time.now config.clock |> Utils.float_to_time)
    in
    let tick_count = Counter.{count= 0; limit= config.tick_limit} in
    {config; conspire; command_buffer; tick_count; clock= config.clock}
end

module Impl = Make (Actions_f.ImperativeActions (Types))

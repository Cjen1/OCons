open Types
open Utils
open C.Types
open Actions_f
open A.O
open Ocons_core.Consensus_intf
module IdMap = Map.Make (Int)

module Types = struct
  type value = command list

  type hash = int64

  type log_entry = {value: value; term: term}

  type segment = {start: log_index; entries: log_entry list}
  [@@deriving accessors]

  type ballot = {segment: segment; term: term} [@@deriving accessors]

  type message =
    | Sync of ballot
    | SyncResp of
        { term: term
        ; conflict_check_result:
            ( log_index
            , [ `Conflict of segment
              | `MissingStartMaxRep of log_index
              | `MissingStartCommit of log_index ] )
            result
              (* If success shows point up to correct replication, otherwise non-matching segment *)
        }
  [@@deriving accessors]

  (* Valid for this term only *)
  type other_nodes_state =
    { matching: log_index
    ; sent: log_index
    ; conflicted: segment option
    ; current_term: term }
  [@@deriving accessors]

  type proposer_state =
    { local_log: log_entry list
    ; local_term: term
    ; other_nodes: other_nodes_state IdMap.t }
  [@@deriving accessors]

  type replica_state =
    {max_vterm: term; max_vlog: log_entry Log.t; max_term: term}
  [@@deriving accessors]

  type t = {rep: replica_state; pro: proposer_state} [@@deriving accessors]
end

module Make
    (Act : ActionSig with type t = Types.t and type message = Types.message) =
struct
  include Types
  open Act

  let ex = ()

  (* Proposer functions *)

  let other_node_state i =
    t @> pro @> other_nodes
    @> [%accessor
         A.field
           ~get:(function s -> IdMap.find i s)
           ~set:(fun s v -> IdMap.add i v s)]

  (*
  let recv_sync_resp src term non_conflict =
    let current = ex.@(other_node_state src) in 
    let current_term = assert false in
    match non_conflict with
    | Ok m when term = current_term ->
        ex.@(other_node_state src @> matching) <- max m (current.matching);
    | Ok m when term 
    | Error fail ->
        ()
        *)

  (* Replica functions *)

  let zip_from ~start i =
    i |> Iter.zip_i |> Iter.map (fun (i, v) -> (i + start, v))

  let fold_short_circuit a f i =
    let open struct
      exception Exit
    end in
    let loc = ref @@ Ok a in
    ( try
        i (fun v ->
            let res = f v in
            loc := res ;
            if Result.is_error res then raise Exit )
      with
    | Exit ->
        ()
    | e ->
        raise e ) ;
    !loc

  let recv_sync src ballot =
    match () with
    | () when ballot.term = ex.@(t @> rep @> max_vterm) ->
        (* Guaranteed that start - 1 matches since reliable inord delivery and
           conflict would increment term, dropping all delayed or otherwise conflicting msgs *)
        let conflict_check_result =
          ballot.segment.entries |> Iter.of_list
          |> zip_from ~start:ballot.segment.start
          |> fold_short_circuit ballot.segment.start (fun (i, v) ->
                 let local = Log.get ex.@(t @> rep @> max_vlog) i in
                 let matches = List.equal Command.equal local.value v.value in
                 if matches then Ok i else Error i )
        in
        let conflict_check_result =
          match conflict_check_result with
          | Ok i ->
              Ok i
          | Error i ->
              Error (`Conflict i)
        in
        send src (SyncResp {term= ballot.term; conflict_check_result})
    | () when ballot.term > ex.@(t @> rep @> max_vterm) ->
        ex.@(t @> rep @> max_vterm) <- ballot.term ;
        Log.cut_after ex.@(t @> rep @> max_vlog) ballot.segment.start ;
        (* TODO check for merge issue *)
        ballot.segment.entries |> Iter.of_list
        |> zip_from ~start:ballot.segment.start
        |> Iter.iter (fun (i, v) -> Log.set ex.@(t @> rep @> max_vlog) i v)
    | _ ->
        assert false
end

module C = Crowbar
module Conspire = Impl_core__Conspire
open! Core
open! Impl_core__Types

module Gen = struct
  open Crowbar
  open Conspire.Types

  let op =
    let open Ocons_core.Types in
    choose
      [ map [bytes] (fun k -> Read k)
      ; map [bytes; bytes] (fun k v -> Write (k, v))
      ; map [bytes; bytes; bytes] (fun key value value' ->
            CAS {key; value; value'} )
      ; const NoOp ]

  let command =
    map [op; int] (fun op id ->
        Ocons_core.Types.Command.{op; id; trace_start= -1.} )

  let value : Conspire.Value.t gen = list command

  let log =
    map
      [list value]
      (fun vs ->
        let log = Log.create [] in
        List.iter vs ~f:(Log.add log) ;
        log )

  let term_vterm = dynamic_bind int (fun term -> pair (const term) (range term))

  let state =
    dynamic_bind log (fun vval ->
        map
          [term_vterm; range (Log.highest vval)]
          (fun (term, vterm) commit_index ->
            Conspire.GlobalTypes.{vval; vterm; term; commit_index} ) )

  let rep_state =
    map [state] (fun state ->
        let local_state = state in
        let expected_remote = Conspire.Replication.clone state in
        Conspire.Replication.
          { local_state
          ; expected_remote
          ; sent_upto= Log.highest local_state.vval
          ; change_flag= false } )

  let operation rep_state =
    let open Conspire.Replication in
    let commit_index =
      map
        [ range ~min:rep_state.local_state.commit_index
            ( Log.highest rep_state.local_state.vval
            - rep_state.local_state.commit_index ) ]
        (fun idx -> CommitIndex idx)
    in
    let term =
      map [range ~min:rep_state.local_state.term 1000] (fun term -> Term term)
    in
    let vterm =
      map
        [range ~min:rep_state.local_state.vterm 1000]
        (fun vterm -> VTerm vterm)
    in
    let vval =
      map
        [range (Log.highest rep_state.local_state.vval); value]
        (fun idx value -> VVal (idx, value))
    in
    choose [commit_index; term; vterm; vval]

  let state_operation_pair = dynamic_bind rep_state operation
end

let () =
  let open Crowbar in
  add_test ~name:"entries_equal"
    [dynamic_bind Gen.rep_state (fun s -> pair (const s) (Gen.operation s))]
    (fun (s, o) ->
      Conspire.Replication.set s o ;
      Conspire.Replication.invariant s )

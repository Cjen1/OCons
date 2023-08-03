module C = Crowbar
module Conspire = Impl_core__Conspire
open! Core
open! Impl_core__Types

let clone state =
  let open Conspire.Types in
  let vval = Log.copy state.vval in
  {state with vval}

module Gen = struct
  open Crowbar
  open Conspire.Types

  let between ~lo ~hi =
    if hi = lo then const lo
    else dynamic_bind (range (hi - lo + 1)) (fun x -> const (x + lo))

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
      [list1 value]
      (fun vs ->
        let log = Log.create [] in
        List.iter vs ~f:(Log.add log) ;
        log )

  let term_vterm =
    dynamic_bind uint16 (fun term ->
        pair (const term) (between ~lo:(-1) ~hi:term) )

  let state =
    dynamic_bind log (fun vval ->
        let ci_gen = between ~lo:0 ~hi:(Log.highest vval) in
        map [term_vterm; ci_gen] (fun (term, vterm) commit_index ->
            Conspire.GlobalTypes.{vval; vterm; term; commit_index} ) )

  let rep_state =
    map [state] (fun state ->
        let local_state = state in
        let expected_remote = clone state in
        Conspire.Replication.
          { local_state
          ; expected_remote
          ; sent_upto= Log.highest local_state.vval
          ; change_flag= false } )

  let operation rep_state =
    let open Conspire.Replication in
    let commit_index =
      map
        [ between ~lo:rep_state.local_state.commit_index
            ~hi:(Log.highest rep_state.local_state.vval) ]
        (fun ci -> CommitIndex ci)
    in
    let term =
      map
        [ between ~lo:rep_state.local_state.term
            ~hi:(rep_state.local_state.term + 1000) ]
        (fun term -> Term term)
    in
    let vterm =
      map
        [ between ~lo:rep_state.local_state.vterm
            ~hi:
              (min rep_state.local_state.term
                 (rep_state.local_state.vterm + 1000) ) ]
        (fun term -> VTerm term)
    in
    let vval =
      map
        [between ~lo:0 ~hi:(Log.highest rep_state.local_state.vval + 1); value]
        (fun idx value -> VVal (idx, value))
    in
    with_printer (fun ppf v ->
        let open Fmt in
        match v with
        | CommitIndex ci ->
            pf ppf "CommitIndex(%d)" ci
        | Term t ->
            pf ppf "Term(%d)" t
        | VTerm t ->
            pf ppf "VTerm(%d)" t
        | VVal (idx, v) ->
            pf ppf "VVal(%d, %a)" idx log_entry_pp v )
    @@ choose [commit_index; term; vterm; vval]

  let rec ind_apply step last =
    let open Crowbar in
    dynamic_bind bool (function
      | false ->
          const last
      | true ->
          dynamic_bind (step last) (fun s -> ind_apply step s) )
end

let invariant t =
  let open Conspire.GlobalTypes in
  let open Conspire.Replication in
  if requires_update t then ignore (get_msg_to_send t) ;
  Crowbar.check_eq t.local_state.commit_index t.expected_remote.commit_index ;
  Crowbar.check_eq t.local_state.term t.expected_remote.term ;
  Crowbar.check_eq t.local_state.vterm t.expected_remote.vterm ;
  match
    find_diverge ~equal:[%equal: log_entry] t.local_state.vval
      t.expected_remote.vval
  with
  | None ->
      ()
  | Some idx ->
      Crowbar.failf "inequal logs at: %d@.%a" idx
        Fmt.(parens @@ pair ~sep:cut (log_pp ~from:idx) (log_pp ~from:idx))
        (t.local_state.vval, t.expected_remote.vval)

let () =
  let open Crowbar in
  let set = Conspire.Replication.set in
  let ( >>= ) = dynamic_bind in
  let ( let* ) = dynamic_bind in
  let edit s =
    let* o = Gen.operation s in
    set s o ; const s
  in
  let single_step = Gen.rep_state >>= edit in
  add_test ~name:"Single edit" [single_step] invariant ;
  let many_step = Gen.rep_state >>= Gen.ind_apply edit in
  add_test ~name:"Many edits" [many_step] invariant

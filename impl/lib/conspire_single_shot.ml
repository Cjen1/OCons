open Core
open Types
open Utils
open C.Types
open Actions_f
open Ocons_core.Consensus_intf
module IdMap = Iter.Map.Make (Int)

module Value = struct
  type t = command list [@@deriving sexp, hash, bin_io]

  let compare = List.compare (fun a b -> Int.compare a.Command.id b.Command.id)
end

let htbl_iter htbl f = Hashtbl.iter htbl ~f

let htbl_iteri htbl f = Hashtbl.iteri htbl ~f:(fun ~key ~data -> f key data)

module Types = struct
  type value = Value.t [@@deriving bin_io]

  let compare_value = List.compare Types.Command.compare

  type config =
    { node_id: node_id
    ; replica_ids: node_id list
    ; replica_count: int
    ; quorum_size: int
    ; fd_timeout: int
    ; max_outstanding: log_index }
  [@@deriving accessors]

  let make_config ~node_id ~replica_ids ~fd_timeout ?(max_outstanding = 8192) ()
      : config =
    let floor f = f |> Int.of_float in
    assert (List.mem replica_ids node_id ~equal:Int.equal) ;
    let replica_count = List.length replica_ids in
    let quorum_size = floor (2. *. Float.of_int replica_count /. 3.) + 1 in
    assert (3 * quorum_size > 2 * replica_count) ;
    { node_id
    ; replica_ids
    ; replica_count
    ; quorum_size
    ; fd_timeout
    ; max_outstanding }

  type sync_state = {value: value; term: term}
  [@@deriving accessors, compare, bin_io]

  type vote = {mutable vvalue: value; mutable vterm: term; mutable term: term}
  [@@deriving accessors, compare, bin_io]

  let set_vote ~vvalue ~vterm ~term v =
    v.vvalue <- vvalue ;
    v.vterm <- vterm ;
    v.term <- term

  let copy_vote {vvalue; vterm; term} ~dst =
    dst.vvalue <- vvalue ;
    dst.vterm <- vterm ;
    dst.term <- term

  type message =
    | Sync of log_index * sync_state
    | SyncResp of log_index * vote
    | Heartbeat
  [@@deriving accessors, compare, bin_io]

  type prop_sm =
    { mutable committed: bool
    ; mutable term: term
    ; mutable value: value
    ; votes: (node_id, vote) Hashtbl.t
    ; mutable max_term: int
    ; mutable max_term_count: int
    ; mutable match_vote_count: int }
  [@@deriving accessors]

  let init_votes config =
    let votes =
      Hashtbl.create ~size:config.replica_count ~growth_allowed:false
        (module Int)
    in
    List.iter config.replica_ids ~f:(fun src ->
        Hashtbl.set votes ~key:src ~data:{vvalue= []; vterm= -1; term= -1} ) ;
    votes

  type fd_sm = {state: (node_id, int) Hashtbl.t} [@@deriving accessors]

  type t =
    { rep_log: vote SegmentLog.t
    ; prop_log: prop_sm SegmentLog.t
    ; commit_index: log_index
    ; config: config
    ; valid_quorum: int -> bool
    ; failure_detector: fd_sm }
  [@@deriving accessors]

  let get_command idx t = (Log.get t.prop_log idx).value |> Iter.of_list

  let get_commit_index t = t.commit_index

  module PP = struct
    open Fmt

    let value_pp : value Fmt.t = brackets @@ list ~sep:comma Command.pp

    let config_pp : config Fmt.t =
      record
        [ field "node_id" (fun c -> c.node_id) int
        ; field "quorum_side" (fun c -> c.quorum_size) int
        ; field "fd_timeout" (fun c -> c.fd_timeout) int
        ; field "replica_ids"
            (fun c -> c.replica_ids)
            (brackets @@ list ~sep:comma int) ]

    let vote_pp =
      record
        [ field "term" (fun (m : vote) -> m.term) int
        ; field "vterm" (fun (m : vote) -> m.vterm) int
        ; field "vvalue" (fun (m : vote) -> m.vvalue) value_pp ]

    let message_pp : message Fmt.t =
     fun ppf v ->
      match v with
      | Sync (idx, m) ->
          pf ppf "Sync(%a)"
            (record
               [ field "idx" (fun _ -> idx) int
               ; field "term" (fun (m : sync_state) -> m.term) int
               ; field "value" (fun (m : sync_state) -> m.value) value_pp ] )
            m
      | SyncResp (idx, m) ->
          pf ppf "SyncResp(%d,%a)" idx vote_pp m
      | Heartbeat ->
          pf ppf "Heartbeat"

    let rep_log_entry_pp =
      braces
      @@ record
           [ field "term" (fun (le : vote) -> le.term) int
           ; field "vterm" (fun (le : vote) -> le.vterm) int
           ; field "vvalue" (fun (le : vote) -> le.vvalue) value_pp ]

    let prop_sm_pp ppf v =
      let { committed
          ; term
          ; value
          ; votes
          ; max_term
          ; max_term_count
          ; match_vote_count } =
        v
      in
      let mt_c =
        max_term
        = (htbl_iter votes |> Iter.fold (fun m (v : vote) -> max m v.term) (-1))
      in
      let mtc_c =
        max_term_count
        = ( htbl_iter votes
          |> Iter.filter (fun (v : vote) -> v.term = max_term)
          |> Iter.length )
      in
      let mvc_c =
        match_vote_count
        = ( htbl_iter votes
          |> Iter.filter (fun (v : vote) ->
                 v.vterm = term && [%compare.equal: Value.t] v.vvalue value )
          |> Iter.length )
      in
      let pp_cache (pp : 'a Fmt.t) : ('a * bool) Fmt.t =
       fun ppf (v, r) ->
        match r with
        | false ->
            Fmt.pf ppf "WRONG(%a)" pp v
        | true ->
            Fmt.pf ppf "correct"
      in
      pf ppf "%a"
        (record
           [ field "state"
               (fun _ -> if committed then "COMMITTED" else "UNDECIDED")
               string
           ; field "term" (fun _ -> term) int
           ; field "value" (fun _ -> value) value_pp
           ; field "votes"
               (fun _ ->
                 Hashtbl.to_alist votes
                 |> List.sort ~compare:(fun (a, _) (b, _) -> Int.compare a b) )
               ( brackets @@ list ~sep:semi @@ parens
               @@ pair ~sep:(Fmt.any ":@ ") int vote_pp )
           ; field "max_term" (fun _ -> (max_term, mt_c)) (pp_cache int)
           ; field "max_term_count"
               (fun _ -> (max_term_count, mtc_c))
               (pp_cache int)
           ; field "match_vote_count"
               (fun _ -> (match_vote_count, mvc_c))
               (pp_cache int) ] )
        ()

    let fd_sm_pp =
      record
        [ field "state"
            (fun v ->
              Hashtbl.to_alist v.state
              |> List.sort ~compare:(fun (a, _) (b, _) -> Int.compare a b) )
            ( brackets @@ list ~sep:semi @@ parens
            @@ pair ~sep:(Fmt.any ":@ ") int int ) ]

    let t_pp =
      record
        [ field "config" (fun t -> t.config) config_pp
        ; field "commit_index" (fun t -> t.commit_index) int
        ; field "rep_log"
            (fun t -> Log.iter t.rep_log |> Iter.to_list)
            (brackets @@ list rep_log_entry_pp)
        ; field "prop_log"
            (fun t -> Log.iter t.prop_log |> Iter.to_list)
            (brackets @@ list prop_sm_pp)
        ; field "fd" (fun t -> t.failure_detector) fd_sm_pp ]
  end
end

module Make
    (Act : ActionSig with type t = Types.t and type message = Types.message) =
struct
  include Types
  module PP = PP
  open Act

  let ex = ()

  let get_msg_term = function
    | Sync (_, {term; _}) | SyncResp (_, {term; _}) ->
        term
    | Heartbeat ->
        assert false

  let get_msg_idx = function
    | Sync (idx, _) | SyncResp (idx, _) ->
        idx
    | Heartbeat ->
        assert false

  let send_sync_resp dst idx conflict =
    let le = Log.get (get_t ()).rep_log idx in
    let msg =
      SyncResp (idx, {term= le.term; vvalue= le.vvalue; vterm= le.vterm})
    in
    if conflict then broadcast msg else send dst msg

  let recv_sync_msg src msg =
    let idx = get_msg_idx msg in
    let ct = get_t () in
    let log = ct.rep_log in
    if not @@ Log.mem log idx then Log.allocate log idx ;
    let le = Log.get log idx in
    match (msg, comp Int.compare (get_msg_term msg) le.term) with
    (* New ballot *)
    | Sync (_, {term; value}), GT ->
        Log.set log idx {term; vvalue= value; vterm= term} ;
        send_sync_resp src idx false
    (* New ballot after conflict *)
    | Sync (_, {term; value; _}), EQ when le.vterm < term ->
        Log.set log idx {term= le.term; vvalue= value; vterm= term} ;
        send_sync_resp src idx false
    (* Revote for existing ballot *)
    | Sync (_, {term; value; _}), EQ
      when le.vterm = term && [%compare.equal: value] value le.vvalue ->
        send_sync_resp src idx false
    (* Conflict for existing ballot (given above) *)
    | Sync _, EQ ->
        Log.set log idx {le with term= le.term + 1} ;
        send_sync_resp src idx true
    (* Nack for old ballot *)
    | Sync _, LT ->
        send_sync_resp src idx false
    | SyncResp _, _ | Heartbeat, _ ->
        assert false

  (* Proposer code *)

  let get_value ct (votes : vote Iter.t) =
    let total_nodes = List.length ct.config.replica_ids in
    let missing_votes = total_nodes - Iter.length votes in
    let max_vterm =
      votes |> IterLabels.fold ~init:(-1) ~f:(fun a (b : vote) -> max a b.vterm)
    in
    let votes_max_vterm =
      Iter.filter (fun (b : vote) -> b.vterm = max_vterm) votes
    in
    let vote_counts =
      let res = Hashtbl.create (module Value) in
      votes_max_vterm
      |> Iter.iter (fun vote ->
             if Hashtbl.mem res vote.vvalue then
               Hashtbl.set res ~key:vote.vvalue
                 ~data:(Hashtbl.find_exn res vote.vvalue + 1)
             else Hashtbl.set res ~key:vote.vvalue ~data:1 ) ;
      res
    in
    let merge vcs : Value.t =
      let res = Hash_set.create (module Command) in
      Hashtbl.iter_keys vcs ~f:(List.iter ~f:(fun c -> Hash_set.add res c)) ;
      res |> Hash_set.to_list |> List.sort ~compare:Command.compare
    in
    let res = ref None in
    Hashtbl.iteri vote_counts ~f:(fun ~key:v ~data:c ->
        if ct.valid_quorum (c + missing_votes) then (
          assert (Option.is_none !res) ;
          res := Some v ) ) ;
    match !res with Some v -> v | None -> merge vote_counts

  let check_conflict idx le ct =
    if (not le.committed) && le.max_term > le.term && ct.valid_quorum le.max_term_count then
      let votes =
        htbl_iter le.votes
        |> Iter.filter (fun (v : vote) -> v.term = le.max_term)
      in
      let all_live_nodes_vote =
        let ( => ) a b = (not a) || b in
        le.max_term_count = ct.config.replica_count
        || ct.config.replica_ids |> Iter.of_list
           |> Iter.for_all (fun src ->
                  let is_live =
                    Hashtbl.find_exn ct.failure_detector.state src > 0
                  in
                  let is_vote =
                    Hashtbl.mem le.votes src
                    && (Hashtbl.find_exn le.votes src).term = le.max_term
                  in
                  is_live => is_vote )
      in
      if all_live_nodes_vote && ct.valid_quorum (Iter.length votes) then (
        le.term <- le.max_term ;
        le.value <- get_value ct votes ;
        le.match_vote_count <- 0 ;
        broadcast @@ Sync (idx, {value= le.value; term= le.term}) )

  let update_commit_index ct =
    let is_commit sm = sm.committed in
    let rec highest_commit ct idx =
      if Log.mem ct.prop_log idx && Log.get ct.prop_log idx |> is_commit then
        highest_commit ct (idx + 1)
      else idx - 1
    in
    set_t {ct with commit_index= highest_commit ct (ct.commit_index + 1)}

  let handle_msg src (msg : vote) idx ct =
    let le = Log.get ct.prop_log idx in
    if not le.committed then
      match () with
      | () when msg.term >= le.term ->
          (* Vote for current value *)
          let stored_vote = Hashtbl.find_exn le.votes src in
          let new_vote = msg.vterm > stored_vote.vterm in
          let should_update = msg.term > stored_vote.term || new_vote in
          if should_update then (
            (* update cached counts *)
            (* max_term *)
            if msg.term > le.max_term then (
              le.max_term <- msg.term ;
              le.max_term_count <- 1 )
            else if msg.term = le.max_term && stored_vote.term < le.max_term
            then le.max_term_count <- le.max_term_count + 1 ;
            (* matching *)
            let is_matching =
              new_vote && msg.vterm = le.term
              && [%compare.equal: Value.t] msg.vvalue le.value
            in
            if is_matching then le.match_vote_count <- le.match_vote_count + 1 ;
            (* update stored vote *)
            copy_vote msg ~dst:stored_vote ;
            (* If sufficient votes then commit *)
            if ct.valid_quorum le.match_vote_count then (
              le.committed <- true ;
              update_commit_index ct ) ;
            (* If sufficient vote but not committed then check conflict *)
            if (not le.committed) && ct.valid_quorum le.max_term_count then
              check_conflict idx le ct )
      | () ->
          assert (msg.term < le.term)

  let failure_detector_update (event : message event) =
    match event with
    | Recv (_, src) ->
        let ct = get_t () in
        Hashtbl.set ct.failure_detector.state ~key:src
          ~data:ct.config.fd_timeout
    | _ ->
        ()

  let handle_event (event : message event) =
    failure_detector_update event ;
    let ct = get_t () in
    match event with
    | Tick ->
        Hashtbl.map_inplace ct.failure_detector.state ~f:(fun v -> v - 1) ;
        Iter.int_range ~start:(ct.commit_index + 1)
          ~stop:(Log.highest ct.prop_log)
        |> Iter.iter (fun idx ->
               let le = Log.get ct.prop_log idx in
               check_conflict idx le ct ) ;
        broadcast Heartbeat
    | Recv (Heartbeat, _) ->
        ()
    | Recv ((Sync _ as m), src) ->
        recv_sync_msg src m
    | Recv (SyncResp (idx, m), src) ->
        let log = ct.prop_log in
        Log.allocate log idx ; handle_msg src m idx ct
    | Commands ci ->
        let start = Log.highest ct.prop_log + 1 in
        IterLabels.iter ci ~f:(fun c ->
            Log.add ct.prop_log
              { committed= false
              ; term= 0
              ; value= [c]
              ; votes= init_votes ct.config
              ; max_term= -1
              ; max_term_count= 0
              ; match_vote_count= 0 } ) ;
        let stop = Log.highest ct.prop_log in
        Log.iteri ct.prop_log ~lo:start ~hi:stop (fun (idx, le) ->
            broadcast @@ Sync (idx, {value= le.value; term= le.term}) )

  let do_until_const t e =
    let eq = Core.Queue.create () in
    let nlq = ref [] in
    let enqueue e = nlq := e :: !nlq in
    let run t e =
      let t, acts = run_side_effects (fun () -> handle_event e) t in
      let acts_iter = Iter.of_list acts in
      let local_msgs =
        acts_iter
        |> Iter.filter_map (function
             | Broadcast m ->
                 Some m
             | Send (dst, m) when dst = t.config.node_id ->
                 Some m
             | _ ->
                 None )
        |> Iter.map (fun m -> Recv (m, t.config.node_id))
      in
      let non_local_msgs =
        acts_iter
        |> Iter.filter (function
             | Send (dst, _) when dst = t.config.node_id ->
                 false
             | _ ->
                 true )
      in
      (t, local_msgs, non_local_msgs)
    in
    let rec aux t () =
      if Core.Queue.is_empty eq then (t, !nlq)
      else
        let e = Core.Queue.dequeue_exn eq in
        let t, l, nl = run t e in
        l |> Iter.iter (fun e -> Queue.enqueue eq e) ;
        nl |> Iter.iter (fun e -> enqueue e) ;
        aux t ()
    in
    Queue.enqueue eq e ; aux t ()

  let advance t e = do_until_const t e

  let create config =
    { config
    ; valid_quorum= (fun size -> size >= config.quorum_size)
    ; commit_index= -1
    ; rep_log= SegmentLog.create {term= 0; vvalue= []; vterm= -1}
    ; prop_log=
        SegmentLog.create_mut (fun () ->
            { committed= false
            ; term= 0
            ; value= []
            ; votes= init_votes config
            ; max_term= -1
            ; max_term_count= 0
            ; match_vote_count= 0 } )
    ; failure_detector=
        { state=
            Hashtbl.of_alist_exn
              (module Int)
              ( config.replica_ids
              |> List.map ~f:(fun id -> (id, config.fd_timeout)) ) } }
end

module Impl = Make (ImperativeActions (Types))

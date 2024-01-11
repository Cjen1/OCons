module C = Crowbar

let entries_equal (ea, la) (eb, lb) =
  la = lb
  && List.equal [%compare.equal: Ocons_core.Types.log_entry] (Iter.to_list ea)
       (Iter.to_list eb)

module Gen = struct
  open Ocons_core.Types
  open Crowbar

  let op =
    let open Ocons_core.Types in
    choose
      [ map [bytes] (fun k -> Read k)
      ; map [bytes; bytes] (fun k v -> Write (k, v))
      ; map [bytes; bytes; bytes] (fun key value value' ->
            CAS {key; value; value'} )
      ; const NoOp ]

  let command =
    with_printer Command.pp
    @@ map [op; int; float; float] (fun op id submitted trace_start ->
           Ocons_core.Types.Command.{op; id; trace_start; submitted} )

  let log_entry =
    with_printer log_entry_pp
    @@ map [command; int] (fun command term -> {command; term})

  let pp_entries ppf (v, _) =
    Fmt.pf ppf "%a"
      Fmt.(brackets @@ list ~sep:comma log_entry_pp_mach)
      (Iter.to_list v)

  let entries =
    with_printer pp_entries
    @@ map [list log_entry] (fun les -> (Iter.of_list les, List.length les))

  let conspire_value : Impl_core.ConspireSS.value gen = list command
end

module LP = Impl_core.Line_prot

let test_entry_equality les =
  let open Crowbar in
  Eio_mock.Backend.run
  @@ fun () ->
  let length = List.length les in
  let w_entries = (Iter.of_list les, length) in
  (*guard (LP.Size.entries w_entries < 1024) ;*)
  let fr, fw = Ocons_core.Utils.mock_flow () in
  let br = Eio.Buf_read.of_flow ~max_size:65536 fr in
  Eio.Buf_write.with_flow fw
  @@ fun bw ->
  LP.SerPrim.entries w_entries bw ;
  let r_entries = LP.DeserPrim.entries br in
  check_eq ~pp:Gen.pp_entries ~eq:entries_equal w_entries r_entries

module Paxos = struct
  open Impl_core.Paxos
  open LP.Paxos

  let msg_gen =
    let open Gen in
    let open Crowbar in
    choose
      [ map [int; int] (fun term leader_commit ->
            RequestVote {term; leader_commit} )
      ; map [int; int; entries] (fun term start_index entries ->
            RequestVoteResponse {term; start_index; entries} )
      ; map [int; int; int; int; entries]
          (fun term leader_commit prev_log_index prev_log_term entries ->
            AppendEntries
              {term; leader_commit; prev_log_index; prev_log_term; entries} )
      ; map [int; bool; int; float] (fun term success index trace ->
            AppendEntriesResponse
              {term; success= (if success then Ok index else Error index); trace} )
      ]

  let msg_equal a b =
    match (a, b) with
    | RequestVote a, RequestVote b ->
        a.term = b.term && a.leader_commit = b.leader_commit
    | RequestVoteResponse a, RequestVoteResponse b ->
        a.term = b.term
        && a.start_index = b.start_index
        && entries_equal a.entries b.entries
    | AppendEntries a, AppendEntries b ->
        a.term = b.term
        && a.leader_commit = b.leader_commit
        && a.prev_log_index = b.prev_log_index
        && a.prev_log_term = b.prev_log_term
        && entries_equal a.entries b.entries
    | AppendEntriesResponse a, AppendEntriesResponse b ->
        a.term = b.term && a.success = b.success
    | _ ->
        false

  let test_msg_equality msg =
    let open Crowbar in
    Eio_mock.Backend.run
    @@ fun () ->
    let fr, fw = Ocons_core.Utils.mock_flow () in
    let br = Eio.Buf_read.of_flow ~max_size:65536 fr in
    Eio.Buf_write.with_flow fw
    @@ fun bw ->
    serialise msg bw ;
    let msg' = parse br in
    check_eq ~pp:PP.message_pp ~eq:msg_equal msg msg'

  let test_msg_series_equality msgs =
    let open Crowbar in
    Eio_mock.Backend.run
    @@ fun () ->
    let fr, fw = Ocons_core.Utils.mock_flow () in
    let br = Eio.Buf_read.of_flow ~max_size:65536 fr in
    Eio.Buf_write.with_flow fw
    @@ fun bw ->
    let rec aux = function
      | [] ->
          ()
      | msg :: ms ->
          serialise msg bw ;
          let msg' = parse br in
          check_eq ~pp:PP.message_pp ~eq:msg_equal msg msg' ;
          aux ms
    in
    aux msgs
end

module Raft = struct
  open Impl_core.Raft
  open LP.Raft

  let msg_gen =
    let open Gen in
    let open Crowbar in
    choose
      [ map [int; int; int] (fun term lastIndex lastTerm ->
            RequestVote {term; lastIndex; lastTerm} )
      ; map [int; bool] (fun term success ->
            RequestVoteResponse {term; success} )
      ; map [int; int; int; int; entries]
          (fun term leader_commit prev_log_index prev_log_term entries ->
            AppendEntries
              {term; leader_commit; prev_log_index; prev_log_term; entries} )
      ; map [int; bool; int] (fun term success index ->
            AppendEntriesResponse
              {term; success= (if success then Ok index else Error index)} ) ]

  let msg_equal a b =
    match (a, b) with
    | RequestVote a, RequestVote b ->
        a.term = b.term && a.lastIndex = b.lastIndex && a.lastTerm = b.lastTerm
    | RequestVoteResponse a, RequestVoteResponse b ->
        a.term = b.term && a.success = b.success
    | AppendEntries a, AppendEntries b ->
        a.term = b.term
        && a.leader_commit = b.leader_commit
        && a.prev_log_index = b.prev_log_index
        && a.prev_log_term = b.prev_log_term
        && entries_equal a.entries b.entries
    | AppendEntriesResponse a, AppendEntriesResponse b ->
        a.term = b.term && a.success = b.success
    | _ ->
        false

  let test_msg_equality msg =
    let open Crowbar in
    Eio_mock.Backend.run
    @@ fun () ->
    let fr, fw = Ocons_core.Utils.mock_flow () in
    let br = Eio.Buf_read.of_flow ~max_size:65536 fr in
    Eio.Buf_write.with_flow fw
    @@ fun bw ->
    serialise msg bw ;
    let msg' = parse br in
    check_eq ~pp:PP.message_pp ~eq:msg_equal msg msg'

  let test_msg_series_equality msgs =
    let open Crowbar in
    Eio_mock.Backend.run
    @@ fun () ->
    let fr, fw = Ocons_core.Utils.mock_flow () in
    let br = Eio.Buf_read.of_flow ~max_size:65536 fr in
    Eio.Buf_write.with_flow fw
    @@ fun bw ->
    let rec aux = function
      | [] ->
          ()
      | msg :: ms ->
          serialise msg bw ;
          let msg' = parse br in
          check_eq ~pp:PP.message_pp ~eq:msg_equal msg msg' ;
          aux ms
    in
    aux msgs
end

module ConspireSS = struct
  open Impl_core.ConspireSS
  open LP.ConspireSS

  let msg_gen =
    let open Gen in
    let open Crowbar in
    choose
      [ const Heartbeat
      ; map [int; conspire_value; int] (fun idx value term ->
            Sync (idx, {value; term}) )
      ; map [int; conspire_value; int; int] (fun idx vvalue vterm term ->
            SyncResp (idx, {vvalue; vterm; term}) ) ]

  let msg_equal = [%compare.equal: message]

  let test_msg_equality msg =
    let open Crowbar in
    Eio_mock.Backend.run
    @@ fun () ->
    let fr, fw = Ocons_core.Utils.mock_flow () in
    let br = Eio.Buf_read.of_flow ~max_size:65536 fr in
    Eio.Buf_write.with_flow fw
    @@ fun bw ->
    serialise msg bw ;
    let msg' = parse br in
    check_eq ~pp:PP.message_pp ~eq:msg_equal msg msg'

  let test_msg_series_equality msgs =
    let open Crowbar in
    Eio_mock.Backend.run
    @@ fun () ->
    let fr, fw = Ocons_core.Utils.mock_flow () in
    let br = Eio.Buf_read.of_flow ~max_size:65536 fr in
    Eio.Buf_write.with_flow fw
    @@ fun bw ->
    let rec aux = function
      | [] ->
          ()
      | msg :: ms ->
          serialise msg bw ;
          let msg' = parse br in
          check_eq ~pp:PP.message_pp ~eq:msg_equal msg msg' ;
          aux ms
    in
    aux msgs
end

let () =
  let open Crowbar in
  (* Common*)
  add_test ~name:"entries_equal"
    [list Gen.log_entry]
    (fun l ->
      let e = (Iter.of_list l, List.length l) in
      check_eq ~eq:entries_equal e e ) ;
  (* Paxos*)
  add_test ~name:"paxos_entries_ser_deser"
    [list Gen.log_entry]
    test_entry_equality ;
  add_test ~name:"paxos_msg_passing" [Paxos.msg_gen] Paxos.test_msg_equality ;
  add_test ~name:"paxos_msg_series_passing"
    [list Raft.msg_gen]
    Raft.test_msg_series_equality ;
  (* Raft *)
  add_test ~name:"raft_entries_ser_deser"
    [list Gen.log_entry]
    test_entry_equality ;
  add_test ~name:"raft_msg_passing" [Raft.msg_gen] Raft.test_msg_equality ;
  add_test ~name:"raft_msg_series_passing"
    [list Raft.msg_gen]
    Raft.test_msg_series_equality ;
  (* Conspire *)
  add_test ~name:"conspire_ss_msg_passing" [ConspireSS.msg_gen]
    ConspireSS.test_msg_equality ;
  add_test ~name:"conspire_ss_msg_series_passing"
    [list ConspireSS.msg_gen]
    ConspireSS.test_msg_series_equality

(* acceptor.ml *)

open Lwt.Infix
open Log.Logger
open Core

(* Types of acceptors *)
type t =
  { id: Types.unique_id
  ; mutable ballot_num: Ballot.t
  ; accepted: (Types.slot_number, Pval.t) Base.Hashtbl.t
  ; mutable gc_threshold: Types.slot_number
  ; replica_slot_outs: (Types.replica_id, Types.slot_number) Base.Hashtbl.t
  ; f: int }

(* Useful helper functions *)
let ( < ) = Ballot.less_than

let ( = ) = Ballot.equal

(* Initialize a new acceptor *)
let initialize replica_uris leader_uris acceptor_uris =
  { id= Uuid_unix.create ()
  ; ballot_num= Ballot.bottom ()
  ; accepted= Base.Hashtbl.create (module Int)
  ; gc_threshold= 1
  ; replica_slot_outs= Base.Hashtbl.create (module Uuid)
  ; f=
      min
        ((List.length acceptor_uris - 1) / 2)
        (min (List.length replica_uris - 1) (List.length leader_uris)) }

let callback1_mutex = Core.Mutex.create ()

let callback2_mutex = Core.Mutex.create ()

let callback3_mutex = Core.Mutex.create ()

(* This callback occurs when an acceptor receives a phase1 message.

   The message will contain a ballot number that a leader is attempting
   to obtain a majority quorum over. The acceptor will adopt the ballot
   number if it is greater than the one it is currently storing.

   In either case, the acceptor replies with its current state (this
   may be the new ballot number it has adopted) *)
let phase1_callback (a : t) (b : Ballot.t) =
  Core.Mutex.critical_section callback2_mutex ~f:(fun () ->
      Lwt_main.run
        (let%lwt () = write_to_log INFO "\n" in
         let%lwt () =
           write_with_timestamp INFO
             ("Receive phase1 message with ballot " ^ Ballot.to_string b)
         in
         let%lwt () =
           if a.ballot_num < b then
             let%lwt () =
               write_with_timestamp INFO
                 ("Adopt ballot number " ^ Ballot.to_string b)
             in
             Lwt.return (a.ballot_num <- b)
           else Lwt.return ()
         in
         let%lwt () =
           write_with_timestamp INFO
             ( "Reply to phase1 message with: "
             ^ Ballot.to_string a.ballot_num
             ^ ", pvalues: " )
         in
         let%lwt () =
           Lwt_list.iter_s
             (fun pval -> write_to_log INFO ("\t\t\t\t" ^ Pval.to_string pval))
             (Base.Hashtbl.data a.accepted)
         in
         Lwt.return
           (a.id, a.ballot_num, Base.Hashtbl.data a.accepted, a.gc_threshold)))

(* This callback occurs when an acceptor receives a phase2 message
   ... *)
let phase2_callback (a : t) (p : Pval.t) =
  Core.Mutex.critical_section callback2_mutex ~f:(fun () ->
      Lwt_main.run
        (let%lwt () = write_to_log INFO "\n" in
         let%lwt () =
           write_with_timestamp INFO
             ("Receive Phase2 callback with pvalue " ^ Pval.to_string p)
         in
         let b, s, _ = p in
         let%lwt () =
           if b = a.ballot_num then
             let%lwt () =
               write_with_timestamp INFO ("Accept pvalue " ^ Pval.to_string p)
             in
             Lwt.return
               ((*Conditional Update*)
                Base.Hashtbl.change a.accepted s (fun opt ->
                    match opt with
                    | None ->
                        Some p
                    | Some (b', _, _) ->
                        if Ballot.less_than b' b then Some p else opt))
           else Lwt.return ()
         in
         let%lwt () =
           write_with_timestamp INFO
             ( "Reply to phase2 message with: (" ^ Types.string_of_id a.id ^ ","
             ^ Ballot.to_string a.ballot_num
             ^ ")" )
         in
         Lwt.return (a.id, a.ballot_num)))

let recv_so_update (a : t) ((slot, rep) : Types.slot_number * Types.replica_id)
    =
  Base.Hashtbl.set a.replica_slot_outs rep slot ;
  let sl_os = Base.Hashtbl.data a.replica_slot_outs in
  let unique_cons xs x = if List.mem ~equal:( = ) xs x then xs else x :: xs in
  let vals = Base.Hashtbl.keys a.accepted in
  let tb_gc =
    List.filter ~f:(fun x -> List.count ~f:(fun y -> y >= x) sl_os > a.f) vals
  in
  List.iter ~f:(fun k -> Base.Hashtbl.remove a.accepted k) tb_gc

(* Initialize a server for a given acceptor  on a given host and port *)
let start_server (acceptor : t) (host : string) (port : int) =
  Message.start_new_server host port
    ~phase1_callback:(phase1_callback acceptor)
    ~phase2_callback:(phase2_callback acceptor)
    ~slot_out_update_callback:(recv_so_update acceptor)

(* Creating a new acceptor consists of initializing a record for it and
   starting a server for accepting incoming messages *)
let new_acceptor host port replica_uris leader_uris acceptor_uris =
  let acceptor = initialize replica_uris leader_uris acceptor_uris in
  start_server acceptor host port
  >>= fun uri ->
  write_to_log INFO ("Initializing new acceptor: \n\tURI " ^ Uri.to_string uri)
  >>= fun () -> Lwt.wait () |> fst

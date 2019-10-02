(* acceptor.ml *)

open Server
open Utils

(* Types of acceptors *)
type t =
  { id: Types.unique_id
  ; mutable ballot_num: Ballot.t
  ; accepted: (int, Pval.t) Base.Hashtbl.t
  ; mutable gc_threshold: Types.slot_number
  ; replica_slot_outs: (Types.replica_id, Types.slot_number) Base.Hashtbl.t
        (* Uppermost slot known by replica *)
  ; fault_tolerance: int
  ; resp_mutex: Lwt_mutex.t
  ; wal: Lwt_io.output_channel * Unix.file_descr }

(* TODO exploit cooperative threading to remove mutexes *)

module Phase1Server = Make_Server (struct
  type nonrec t = t

  let connected_callback :
      Lwt_io.input_channel * Lwt_io.output_channel -> t -> unit Lwt.t =
   fun (ic, oc) (acceptor : t) ->
    let* msg = Lwt_io.read_line ic in
    let ballot = msg |> assert false in
    let* () =
      critical_section acceptor.resp_mutex ~f:(fun () ->
          Lwt.return
          @@ if acceptor.ballot_num < ballot then acceptor.ballot_num <- ballot)
    in
    let resp =
      ( acceptor.id
      , acceptor.ballot_num
      , Base.Hashtbl.data acceptor.accepted (* TODO reduce this? *)
      , acceptor.gc_threshold )
    in
    let marshalled_resp = resp |> assert false in
    (* TODO write to wal *)
    Lwt_io.write_line oc marshalled_resp
end)

module Phase2Server = Make_Server (struct
  type nonrec t = t

  exception Preempted of Ballot.t

  let connected_callback :
      Lwt_io.input_channel * Lwt_io.output_channel -> t -> unit Lwt.t =
   fun (ic, oc) (acceptor : t) ->
    let* msg = Lwt_io.read_line ic in
    let ((ib, is, _) as ipval) = msg |> assert false in
    try
      let* () =
        critical_section acceptor.resp_mutex ~f:(fun () ->
            if ib = acceptor.ballot_num then
              match Base.Hashtbl.find acceptor.accepted is with
              | Some (b', _, _) ->
                  if
                    Ballot.less_than b' ib
                    (* If more up to date ballot number was received *)
                  then Base.Hashtbl.set acceptor.accepted ~key:is ~data:ipval
              | None ->
                  Base.Hashtbl.set acceptor.accepted ~key:is ~data:ipval
            else raise (Preempted ib) ;
            Lwt.return_unit)
      in
      let resp = (acceptor.id, acceptor.ballot_num) in
      let marshalled_resp = resp |> assert false in
      (* TODO write to wal *)
      Lwt_io.write_line oc marshalled_resp
    with Preempted b ->
      let nack = b |> assert false in
      Lwt_io.write_line oc nack
end)

(* Initialize a new acceptor *)
let create fault_tolerance wal_location =
  { id= Uuid_unix.create ()
  ; ballot_num= Ballot.bottom ()
  ; accepted= Base.Hashtbl.create (module Base.Int)
  ; gc_threshold= 1
  ; replica_slot_outs= Base.Hashtbl.create (module Uuid)
  ; fault_tolerance
  ; resp_mutex= Lwt_mutex.create ()
  ; wal=
      (let fd =
         Unix.openfile wal_location [Unix.O_RDWR; Unix.O_CREAT]
         @@ int_of_string "0x660"
       in
       let chan =
         Lwt_io.of_fd ~mode:Lwt_io.Output (Lwt_unix.of_unix_file_descr fd)
       in
       (chan, fd)) }

let start acceptor host p1_port p2_port =
  Lwt.join
    [ Phase1Server.start host p1_port acceptor
    ; Phase2Server.start host p2_port acceptor ]

(* Creating a new acceptor consists of initializing a record for it and
   starting a server for accepting incoming messages *)
let create_and_start_acceptor host p1_port p2_port failure_tolerance
    wal_location =
  let acceptor = create failure_tolerance wal_location in
  start acceptor host p1_port p2_port

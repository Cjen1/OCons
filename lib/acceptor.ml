(* acceptor.ml *)

open Utils
open Messaging

(* Types of acceptors *)
type t =
  { id: Types.unique_id
  ; mutable ballot_num: Ballot.t
  ; accepted: (Types.slot_number, Pval.t) Base.Hashtbl.t
  ; resp_mutex: Lwt_mutex.t
  ; wal: Lwt_io.output_channel * Unix.file_descr }

(* TODO exploit cooperative threading to remove mutexes *)

module Phase1Server = Server.Make_Server (struct
  type nonrec t = t

  let connected_callback :
      Lwt_io.input_channel * Lwt_io.output_channel -> t -> unit Lwt.t =
   fun (ic, oc) (acceptor : t) ->
    Logs.debug (fun m -> m "Receiving p1a");
    let* msg = Lwt_io.read_value ic in
    let p1a : p1a =
      Bytes.of_string msg |> Protobuf.Decoder.decode_exn p1a_from_protobuf
    in
    let* () =
      Logs_lwt.debug (fun m ->
          m "p1: received request for ballot: %s" (Ballot.to_string p1a.ballot))
    in
    let* p1b =
      critical_section acceptor.resp_mutex ~f:(fun () ->
          let* () =
            if Ballot.less_than acceptor.ballot_num p1a.ballot then
              (*Ordering of wal vs update is important for durability *)
              let* () =
                Logs_lwt.debug (fun m ->
                    m "p1: updating ballot to %s" (Ballot.to_string p1a.ballot))
              in
              let* () =
                acceptor.ballot_num |> Ballot.to_string
                |> write_to_wal acceptor.wal
              in
              Lwt.return @@ (acceptor.ballot_num <- p1a.ballot)
            else Lwt.return ()
          in
          let p1b : p1b =
            { ballot= acceptor.ballot_num
            ; accepted=
                Base.Hashtbl.data acceptor.accepted
                (* TODO reduce this? Can be done by including a high water mark in p1a message decisions *)
            }
          in
          Lwt.return p1b)
    in
    Logs.debug (fun m -> m "Sending p1b");
    let p1b_string =
      p1b |> Protobuf.Encoder.encode_exn p1b_to_protobuf |> Bytes.to_string
    in
    let* () = Lwt_io.write_value oc p1b_string in
    Logs_lwt.debug (fun m -> m "Sent p1b")
end)

module Phase2Server = Server.Make_Server (struct
  type nonrec t = t

  exception Preempted of Ballot.t

  let connected_callback :
      Lwt_io.input_channel * Lwt_io.output_channel -> t -> unit Lwt.t =
   fun (ic, oc) (acceptor : t) ->
    let* msg = Lwt_io.read_value ic in
    let p2a : p2a =
      msg |> Bytes.of_string |> Protobuf.Decoder.decode_exn p2a_from_protobuf
    in
    let ((ib, is, _) as ipval) = p2a.pval in
    let* () =
      Logs_lwt.debug (fun m ->
          m "p2_callback: got pval from leader: %s" (Pval.to_string ipval))
    in
    try%lwt
      let* () =
        critical_section acceptor.resp_mutex ~f:(fun () ->
            if not (Ballot.less_than ib acceptor.ballot_num) then
              let should_update =
                (*match Base.Hashtbl.find acceptor.accepted is with
                | Some (b', _, _) ->
                    if
                      not (Ballot.less_than ib b') (* ~(ib < b') = ib >= b' *)
                      (* If more up to date ballot number was received *)
                    then true
                    else (
                      Logs.debug (fun m ->
                          m
                            "phase2server: assert incomming ballot = current \
                             ballot: (%s = %s)"
                            (Ballot.to_string ib)
                            (Ballot.to_string acceptor.ballot_num)) ;
                      assert false )
                (* if else branch then there exists a ballot with a ballot > ballot_num which has been accepted already *)
                | None ->
                    true
                  *)
                true
              in
              if should_update then
                let* () = Logs_lwt.debug (fun m -> m "p2: writing to wal") in
                let* () = write_to_wal acceptor.wal @@ Pval.to_string ipval in
                let* () =
                  Logs_lwt.debug (fun m ->
                      m "p2: Committed %s" (Pval.to_string ipval))
                in
                Lwt.return
                @@ Base.Hashtbl.set acceptor.accepted ~key:is ~data:ipval
              else Lwt.return_unit
            else raise (Preempted acceptor.ballot_num))
      in
      let p2b : p2b =
        {acceptor_id= acceptor.id; ballot= acceptor.ballot_num}
      in
      let marshalled_resp =
        p2b |> Protobuf.Encoder.encode_exn p2b_to_protobuf |> Bytes.to_string
      in
      Lwt_io.write_value oc marshalled_resp
    with Preempted b ->
      let* () =
        Logs_lwt.debug (fun m ->
            m "p2: preempted by ballot: %s" (Ballot.to_string b))
      in
      let nack : nack = {ballot= b} in
      let nack_string =
        nack |> Protobuf.Encoder.encode_exn nack_to_protobuf |> Bytes.to_string
      in
      Lwt_io.write_value oc nack_string
end)

(* Initialize a new acceptor *)
let create wal_location =
  { id= Types.create_id ()
  ; ballot_num= Ballot.bottom ()
  ; accepted= Base.Hashtbl.create (module Base.Int)
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
let create_and_start_acceptor host p1_port p2_port wal_location =
  let* () = Logs_lwt.info (fun m -> m "Spinning up acceptor") in
  let acceptor = create wal_location in
  start acceptor host p1_port p2_port

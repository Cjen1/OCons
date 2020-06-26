open Base
open Ocamlpaxos
open Types
open Utils
open Lwt.Infix

module P_t_ref = struct
  type t = (log_index, log_entry) Lookup.t

  let init () = Lookup.create (module Int)

  type op =
    | Set of log_index * log_entry [@key 1]
    | Remove of log_index [@key 2]
  [@@deriving protobuf]

  let encode_blit op =
    let res = Protobuf.Encoder.encode_exn op_to_protobuf op in
    let p_len = Bytes.length res in
    let blit buf ~offset =
      Bytes.blit ~src:res ~src_pos:0 ~dst:buf ~dst_pos:offset ~len:p_len
    in
    (p_len, blit)

  let decode buf ~offset =
    let decode_buf =
      Bytes.sub buf ~pos:offset ~len:(Bytes.length buf - offset)
    in
    Protobuf.Decoder.decode_exn op_from_protobuf decode_buf

  let apply t op =
    match op with
    | Set (index, value) ->
        Lookup.set t ~key:index ~data:value
    | Remove index ->
        Lookup.remove t index
end

module P_ref = Utils.Persistant (P_t_ref)

let test_ref os log () =
  let log =
    List.fold_right ~f:(fun op log -> P_ref.change log op) ~init:log os
  in
  P_ref.sync log >>= fun _ -> Lwt.return_unit

let bench f =
  let st = Unix.gettimeofday () in
  f () >>= fun _ -> Unix.gettimeofday () -. st |> Lwt.return

let test () =
  let op =
    P_t_ref.Set
      (10, {command= {op= StateMachine.Read "asdf"; id= 1}; term= 1; index= 10})
  in
  let os = List.init 100000 ~f:(fun _ -> op) in
  let log_path = "log1.tmp" in
  P_ref.of_file log_path
  >>= fun log ->
  bench (test_ref os log)
  >>= fun time ->
  Fmt.pr "Reference: %.3f\n" time ;
  Lwt.return_unit

let () = Lwt_main.run (test ())

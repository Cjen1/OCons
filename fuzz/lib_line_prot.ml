open! Ocons_core
open! Types

module Gen = struct
  open Ocons_core.Types
  open Crowbar

  let op =
    choose
      [ map [bytes] (fun k -> Read k)
      ; map [bytes; bytes] (fun k v -> Write (k, v))
      ; map [bytes; bytes; bytes] (fun key value value' ->
            CAS {key; value; value'} )
      ; const NoOp ]

  let command =
    map [op; int; float; float] (fun op id submitted trace_start ->
        Ocons_core.Types.Command.{op; id; submitted; trace_start} )

  let log_entry = map [command; int] (fun command term -> {command; term})

  let entries =
    map [list log_entry] (fun les -> (Iter.of_list les, List.length les))

  let op_response =
    choose
      [ const Success
      ; map [bytes] (fun k -> Failure k)
      ; map [bytes] (fun k -> ReadSuccess k) ]
end

let test_client_request r =
  let open Crowbar in
  Eio_mock.Backend.run
  @@ fun () ->
  let fr, fw = Ocons_core.Utils.mock_flow () in
  let br = Eio.Buf_read.of_flow ~max_size:65536 fr in
  Eio.Buf_write.with_flow fw
  @@ fun bw ->
  Line_prot.External_infra.serialise_request r bw ;
  let r' = Line_prot.External_infra.parse_request br in
  (* compare *)
  check_eq ~cmp:Command.compare ~pp:Command.pp_mach r r' ;
  (* hash *)
  check_eq ~pp:Command.pp_mach r r' ~cmp:(fun a b ->
      let ha, hb = (Command.hash a, Command.hash b) in
      Int.compare ha hb )

let test_client_response r =
  let open Crowbar in
  Eio_mock.Backend.run
  @@ fun () ->
  let fr, fw = Ocons_core.Utils.mock_flow () in
  let br = Eio.Buf_read.of_flow ~max_size:65536 fr in
  Eio.Buf_write.with_flow fw
  @@ fun bw ->
  Line_prot.External_infra.serialise_response r bw ;
  let r' = Line_prot.External_infra.parse_response br in
  check_eq ~pp:Line_prot.External_infra.response_pp r r'

let () =
  let open Crowbar in
  add_test ~name:"client_request" [Gen.command] (fun command ->
      test_client_request command ) ;
  add_test ~name:"client_response" [int; Gen.op_response; float]
    (fun id response ts -> test_client_response (id, response, ts))

open! Ocons_core
open! Types

module Gen = struct
  open Crowbar

  let op =
    choose
      [ map [bytes] (fun k -> Read k)
      ; map [bytes; bytes] (fun k v -> Write (k, v))
      ; map [bytes; bytes; bytes] (fun key value value' ->
            CAS {key; value; value'} )
      ; const NoOp ]

  let command = map [op; int] (fun op id -> Ocons_core.Types.Command.{op; id})

  let log_entry =
    map [command; int] (fun command term -> Ocons_core.Types.{command; term})

  let entries =
    map [list log_entry] (fun les -> (Iter.of_list les, List.length les))

  let op_response =
    choose
      [ const Success
      ; map [bytes] (fun k -> Failure k)
      ; map [bytes] (fun k -> ReadSuccess k) ]
end

let make_source q =
  object
    inherit Eio.Flow.source

    val q = q

    val mutable left_over : Cstruct.t option = None

    method read_into buf =
      let copy_and_assign_rem data buf =
        match (Cstruct.length data, Cstruct.length buf) with
        | ld, lb when ld <= lb ->
            Cstruct.blit data 0 buf 0 ld ;
            ld
        | ld, lb ->
            Cstruct.blit data 0 buf 0 lb ;
            let rem = Cstruct.take ~min:(ld - lb) data in
            left_over <- Some rem ;
            lb
      in
      match left_over with
      | Some data ->
          copy_and_assign_rem data buf
      | None ->
          copy_and_assign_rem (Eio.Stream.take q) buf
  end

let make_sink q =
  object
    inherit Eio.Flow.sink

    method copy src =
      try
        while true do
          let buf = Cstruct.create 4096 in
          let got = src#read_into buf in
          Eio.Stream.add q (Cstruct.split buf got |> fst)
        done
      with End_of_file -> ()

    method! write bufs = List.iter (fun buf -> Eio.Stream.add q buf) bufs
  end

let mock_flow () =
  let q = Eio.Stream.create 8 in
  (make_source q, make_sink q)

let test_client_request r =
  let open Crowbar in
  Eio_mock.Backend.run
  @@ fun () ->
  let fr, fw = mock_flow () in
  let br = Eio.Buf_read.of_flow ~max_size:65536 fr in
  Eio.Buf_write.with_flow fw
  @@ fun bw ->
  Line_prot.External_infra.serialise_request r bw ;
  let r' = Line_prot.External_infra.parse_request br in
  check_eq ~cmp:Command.compare ~pp:Command.pp r r'

let test_client_response r =
  let open Crowbar in
  Eio_mock.Backend.run
  @@ fun () ->
  let fr, fw = mock_flow () in
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
  add_test ~name:"client_response" [int; Gen.op_response] (fun id response ->
      test_client_response (id, response) )

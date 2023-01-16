module C = Crowbar

let entries_equal (ea, la) (eb, lb) =
  la = lb && List.equal ( = ) (Iter.to_list ea) (Iter.to_list eb)

module Gen = struct
  open Crowbar

  let op =
    let open Ocons_core.Types in
    choose
      [ map [bytes] (fun k -> Read k)
      ; map [bytes; bytes] (fun k v -> Write (k, v))
      ; map [bytes; bytes; bytes] (fun key value value' ->
            CAS {key; value; value'} )
      ; const NoOp ]

  let command = map [op; int] (fun op id -> Ocons_core.Types.Command.{op; id})

  let log_entry =
    map [command; int] (fun command term -> Ocons_core.Types.{command; term})
end

module LP = Paxos_core.Line_prot

let make_source q =
  object (self)
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
  object (self)
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

let test_entry_equality les =
  let open Crowbar in
  Eio_mock.Backend.run
  @@ fun () ->
  let length = List.length les in
  let w_entries = (Iter.of_list les, length) in
  (*guard (LP.Size.entries w_entries < 1024) ;*)
  let fr, fw = mock_flow () in
  let br = Eio.Buf_read.of_flow ~max_size:65536 fr in
  Eio.Buf_write.with_flow fw
  @@ fun bw ->
  LP.SerPrim.entries w_entries bw ;
  let r_entries = LP.DeserPrim.entries br in
  check_eq ~eq:entries_equal w_entries r_entries

let () =
  let open Crowbar in
  add_test ~name:"entries_equal"
    [list Gen.log_entry]
    (fun l ->
      let e = (Iter.of_list l, List.length l) in
      check_eq ~eq:entries_equal e e ) ;
  add_test ~name:"entries_ser_deser" [list Gen.log_entry] test_entry_equality

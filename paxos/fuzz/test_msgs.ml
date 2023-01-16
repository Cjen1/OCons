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

module LP = struct
  open Paxos_core.Line_prot
end

let () =
  let open Crowbar in
  add_test ~name:"entries_equal"
    [list Gen.log_entry]
    (fun l ->
      let e = (Iter.of_list l, List.length l) in
      check_eq ~eq:entries_equal e e );
  add_test ~name:"entries_ser_deser"
    [list Gen.log_entry]
    (fun les ->
      let length 

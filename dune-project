(lang dune 1.11)
(name ocaml-paxos)
(source (github chrislomaxjones/part-ii-project))
(authors "Chris Jones" "Chris Jensen")

(package
  (name ocaml-paxos)
  (synopsis "An ocaml implementation of the Paxos protocol")
  (depends 
    oUnit 
    core 
    core.uuid lwt 
    yojson 
    uri 
    capnp-rpc-lwt 
    capnp-rpc-unix))

(package
  (name ocaml-paxos-bin)
  (synopsis "A server using ocaml-paxos")
  (depends
    core
    lwt
    uri
    lwt_ppx))

(lang dune 1.11)
(using fmt 1.2)
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

(lang dune 1.11)
(using fmt 1.2)
(name ocamlpaxos)
(source (github cjen1/OcamlPaxos))
(authors "Chris Jones" "Chris Jensen")

(allow_approximate_merlin)

(generate_opam_files true)

(package
  (name ocamlpaxos)
  (synopsis "An ocaml implementation of the Paxos protocol")
  (depends 
    (ocaml (>= 4.08.0))
    base
    (core (<= v0.13.0))
    ppx_jane
    dune
    lwt
    ppx_deriving_protobuf
    conf-libev
    logs
    fmt
    capnp
    owl
    odbutils
    ))

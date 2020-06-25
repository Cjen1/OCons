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
    core
    dune
    ppx_jane
    lwt
    lwt_ppx
    ppx_deriving_protobuf
    conf-libev
    logs
    fmt
    capnp-rpc-net
    capnp-rpc-unix
    capnp-rpc-lwt
    capnp
    ))

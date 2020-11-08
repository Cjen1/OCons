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
    core
    async
    ppx_jane
    ppx_log
    ppx_deriving_yojson
    dune
    logs
    fmt
    owl
    core_bench
    ))

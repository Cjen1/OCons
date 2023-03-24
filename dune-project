(lang dune 2.7)
(name ocons)
(source (github cjen1/OcamlPaxos))
(authors "Chris Jones" "Chris Jensen")

(allow_approximate_merlin)

(generate_opam_files true)

(package
  (name ocons)
  (synopsis "The infrastructure for pluggable consensus implementations")
  (depends 
    (ocaml (= 5.0.0))
    cmdliner
    iter
    accessor
    crowbar
    bisect_ppx
    ppx_jane
    ppx_log
    ppx_accessor
    accessor
    base
    core
    core_unix
    owl
    eio
    eio_main
    eio_linux
    ))

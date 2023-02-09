(lang dune 2.7)
(name ocons-core)
(source (github cjen1/OcamlPaxos))
(authors "Chris Jones" "Chris Jensen")

(allow_approximate_merlin)

(generate_opam_files true)

(package
  (name ocons-core)
  (synopsis "The infrastructure for pluggable consensus implementations")
  (depends 
    (ocaml (>= 4.08.0))
    crowbar
    cmdliner
    accessor iter
    bisect_ppx
    ppx_jane
    ppx_log
    ppx_accessor
    eio
    accessor
    eio.mock
    base
    core
    core_unix
    accessor
    owl
    ))

(package
  (name ocons-variants)
  (synopsis "An ocaml implementation of the Paxos protocol")
  (depends 
    (ocaml (>= 4.08.0))
    core
    async
    accessor_async
    rpc_parallel
    ppx_jane
    ppx_log
    ppx_deriving_yojson
    ppx_accessor
    dune
    logs
    fmt
    owl
    core_bench
    core_profiler
    bisect_ppx
    ocons-core
    ))

(package
  (name ocons-conn-mgr)
  (synopsis "A unreliable messaging library")
  (depends ))

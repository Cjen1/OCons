# Overview

A multi-degree paxos replicated log implemented in OCaml.

### Features

* Highly moddable core state machine
* Persistance to disk (fsync'd)
* Relatively high performance

### Library structure
This is presented as a library which exposes the core state machine in `lib/paxos.ml`, an implementation with all scaffolding in place in `lib/infra.ml` and a client library for that scaffolding in `lib/client.ml`.

The log, state machine and RPC protocol is given in `lib/types.ml`.

These library components are put together in `bin/client.ml`, `bin/main.ml` and `bin/bench.ml`.


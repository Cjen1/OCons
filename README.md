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

### Exemplar (Template) usage

```
dune exec paxos/main.exe -- ID 0:127.0.0.1:5000,1:127.0.0.1:5010,2:127.0.0.1:5020 -p 50ID0 -q 50ID1 -t 1
```

```
dune exec paxos/main.exe -- 0 0:127.0.0.1:5000,1:127.0.0.1:5010,2:127.0.0.1:5020 -p 5000 -q 5001 -t 1
dune exec paxos/main.exe -- 1 0:127.0.0.1:5000,1:127.0.0.1:5010,2:127.0.0.1:5020 -p 5010 -q 5011 -t 1
dune exec paxos/main.exe -- 2 0:127.0.0.1:5000,1:127.0.0.1:5010,2:127.0.0.1:5020 -p 5020 -q 5021 -t 1
```

```
dune exec bin/cli.exe -- write 1 1 0 127.0.0.1:5001,127.0.0.1:5011,127.0.0.1:5021 -r 10
```

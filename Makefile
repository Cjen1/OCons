.PHONY: build
build:
	eval `opam env` 
	opam exec -- dune build @install 

.PHONY: test
test: clean
	opam exec -- dune runtest -f
	opam exec -- dune exec scripts/micro_bench.exe -- paxos -rate 100

.PHONY: clean
clean:
	opam exec -- dune clean
	rm -rf *.log *.datadir *.dat *.dat.old *.stderr *.stdout data.json

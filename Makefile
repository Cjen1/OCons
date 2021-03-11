.PHONY: build clean clean_benchmark

build:
	eval `opam env` 
	dune build @install 

clean: clean_benchmark
	dune clean

clean_benchmark:
	rm -rf *.log *.datadir *.dat *.dat.old *.stderr *.stdout

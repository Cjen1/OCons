build:
	eval `opam env` && \
	dune build @install && \
	dune install 
	rm -v ../bin/*
	cp _build/install/default/bin/* ../bin/


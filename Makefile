build:
	eval `opam env` && \
	dune build @install && \
	dune install 
	rm ../bin/*
	cp _build/install/default/bin/* ../bin/


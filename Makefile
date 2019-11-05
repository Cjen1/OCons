build:
	eval `opam env` && \
	dune build @install && \
	dune install 


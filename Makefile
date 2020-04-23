build:
	eval `opam env` 
	opam install --deps-only . -y
	dune build @install 
	dune install 


PACKAGES="ocamlfind ounit core lwt base-unix capnp-rpc-unix"

sudo apt-get update -qq
sudo apt-get install -qq ocaml ocaml-native-compilers camlp4-extra opam

yes | opam init https://opam.ocaml.org/1.1
opam install ${PACKAGES}
opam update

eval `opam config env`

# Build the source
sudo make
# Build and run the tests
sudo make test

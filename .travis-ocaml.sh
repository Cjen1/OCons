PACKAGES="ocamlfind ounit core lwt base-unix"

sudo apt-get update -qq
sudo apt-get install -qq ocaml ocaml-native-compilers camlp4-extra opam

# Initialize OPAM
yes | opam init https://opam.ocaml.org/1.1

# Install pre-requisite packages
yes | opam install ${PACKAGES}

# DON'T WORRY ABOUT THIS FOR NOW
#-------------------------------
# Cap'n Proto installation (+ package)
# sudo apt-get install capnproto
#opam depext -i capnp-rpc-unix

# Update all packages 
opam update

# Config environment ???
eval `opam config env`

# Build the source from the Makefile
sudo make

# Build and run the tests from the Makefile
sudo make test

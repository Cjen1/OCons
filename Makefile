
.PHONY: all clean byte native profile debug test

# Flags passed to corebuild.
#
# 	-	-I flag used to search sub-directories for code.
#
# 	-	-use-ocamlfind required for Opam packages
#
#	-	Separate _tags file is used for package dependencies and
#		bin_annot.
#
FLAGS = -use-ocamlfind -I src -I lib -I tests -I src/rpc -I src/app -I src/replica

# Corebuild is Ocamlbuild wrapper that makes it play better with Core
OCB = corebuild $(FLAGS)

#-----------------------------------------------------------------------------

# Build native and byte code targets as dependencies
all: native byte

#-----------------------------------------------------------------------------

# Clean the build directory
clean:
	$(OCB) -clean

# Check that packages can be found
sanity:
	ocamlfind query ounit lwt unix capnp-rpc-lwt capnp-rpc-unix core

#-----------------------------------------------------------------------------

# Build a native code target
native:
	$(OCB) main.native

# Build a bytecode target
byte:
	$(OCB) main.byte

#-----------------------------------------------------------------------------

# Build and run an **EXAMPLE** test suite
# 	-	Since no specified directory path implied by importing file,
# 		should probably rename tests to be test_foo, test_bar, etc...
#
# 	-	OR gather all tests into a test.ml file and always pass suites
# 		to that
test:
	$(OCB) example.native
	./example.native

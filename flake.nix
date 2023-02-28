{
  inputs = {
    opam-nix.url = "github:tweag/opam-nix";
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.follows = "opam-nix/nixpkgs";
  };
  outputs = { self, flake-utils, opam-nix, nixpkgs }@inputs:
    flake-utils.lib.eachDefaultSystem (system:
      let
        inherit (builtins) map listToAttrs hasAttr typeOf trace;
        pkgs = nixpkgs.legacyPackages.${system};
        on = opam-nix.lib.${system};
        opam2query = opamFile:
          let 
            f = e :
              if typeOf e == "set" then {name = e.val; value = "*";} else {name = e; value = "*";};
          in listToAttrs (map f (on.importOpam opamFile).depends);
        localPackageQuery = opam2query ./ocons.opam; 
        devPackagesQuery = {
          ocaml-lsp-server = "*";
          ocamlformat = "*";
          utop = "*";

          bigstringaf = "*";
          cstruct = "*";
          lwt-dllist = "*";
          optint = "*";
          psq = "*";
          fmt = "*";
          hmap = "*";
          mtime = "*";
          uring = "*";
          logs = "*";
          luv_unix = "*";
        };
        query = devPackagesQuery // (trace localPackageQuery localPackageQuery) // {
          ocaml-base-compiler = "5.0.0";
        };
        scope = on.buildOpamProject' {recursive=true; } ./. query;
        devPackages = builtins.attrValues
          (pkgs.lib.getAttrs (builtins.attrNames query) scope);
        magic-trace = pkgs.stdenv.mkDerivation {
          name = "magic-trace";
          src = pkgs.fetchurl {
            url = "https://github.com/janestreet/magic-trace/releases/download/v1.1.0/magic-trace";
            sha256 = "1arskf8zdr2bq5zzq4q8mkyzhj7c2f5xy5brlqgkbxb4bb417isd";
          };
          phases = ["installPhase" "patchPhase"];
          installPhase = ''
            mkdir -p $out/bin
            cp $src $out/bin/magic-trace
            chmod +x $out/bin/magic-trace
            '';
        };
      in
      {
        defaultPackage = pkgs.hello;
        devShell = pkgs.mkShell {
          buildInputs = devPackages ++ [
          magic-trace
          pkgs.linuxPackages_latest.perf
          pkgs.fzf
          ];
        };
      });
}

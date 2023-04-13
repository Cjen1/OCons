{
  inputs = {
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.follows = "opam-nix/nixpkgs";
    opam-repository = {
      url = "github:ocaml/opam-repository";#562afb69b736d0b96a88588228dd55ae9cfefe60";
      flake = false;
    };
    opam-nix = {
      url = "github:tweag/opam-nix";
      inputs.opam-repository.follows = "opam-repository";
    };
  };
  outputs = { self, flake-utils, opam-nix, nixpkgs, opam-repository}:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        on = opam-nix.lib.${system};
        devPackagesQuery = {
          ocaml-lsp-server = "*";
          ocamlformat = "*";
          utop = "*";
        };
        repos = [
          opam-repository
        ];
        query = devPackagesQuery // {
          ocaml-base-compiler = "5.0.0";
          ocamlfind = "1.9.5";
        };
        scope = on.buildDuneProject { inherit repos; } "ocons" ./. query;
        devPackages = builtins.attrValues
          (pkgs.lib.getAttrs (builtins.attrNames query) scope);
      in
      {
        defaultPackage = scope.ocons;
        devShell = pkgs.mkShell {
          inputsFrom = [scope.ocons];
          buildInputs = devPackages ++ [
            pkgs.linuxPackages_latest.perf
            pkgs.fzf
          ];
        };
      });
}


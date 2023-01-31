{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, utils }:
    utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in
      {
        defaultPackage = pkgs.hello;
        devShell = with pkgs; mkShell {
          buildInputs = [
            opam
            zlib
            openblas
            pkgconfig
          ];
          shellHook = ''
          eval "$(opam env)"
          '';
        };
      });
}


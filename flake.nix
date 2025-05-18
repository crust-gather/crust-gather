{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    crane.url = "github:ipetkov/crane";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, crane, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        craneLib = crane.mkLib pkgs;
        htmlFilter = path: _type: builtins.match ".*html$" path != null;
        htmlOrCargo = path: type:
          (htmlFilter path type) || (craneLib.filterCargoSources path type);
      in
    {
      packages.default = craneLib.buildPackage {
        src = nixpkgs.lib.cleanSourceWith {
          src = ./.;
          filter = htmlOrCargo; # Include html event-filter templates
          name = "source";
        };
        doCheck = false; # Skip running tests, due to kwok dependency, for simplicity
      };
    });
}


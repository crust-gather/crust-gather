let
  rustOverlay = import (builtins.fetchTarball {
    url = "https://github.com/oxalica/rust-overlay/archive/master.tar.gz";
  });
  pkgs = import <nixpkgs> {
    overlays = [ rustOverlay ];
  };
  manifest = (pkgs.lib.importTOML ./Cargo.toml).package;
  rust = pkgs.rust-bin.stable.latest.default;
in
pkgs.rustPlatform.buildRustPackage rec {
  pname = manifest.name;
  version = manifest.version;
  doCheck = false;
  cargoLock.lockFile = ./Cargo.lock;
  src = pkgs.lib.cleanSource ./.;
  cargo = rust;
  rustc = rust;
}

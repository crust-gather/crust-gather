#! /usr/bin/env nix-shell
#! nix-shell -i bash -p 'callPackage (fetchGit https://github.com/crust-gather/crust-gather) {}'
kubectl-crust-gather ${@}

## How to release crust-gather

This can be done in one simple step:
1. pushing a `vX.Y.Z` tag pointing to the latest state of the `main` branch.

This step will trigger the release CI [workflow](../.github/workflows/release.yaml), which will:

- publish the release binaries and GitHub release for that tag
- push the container image to `ghcr.io/crust-gather/crust-gather`
- rewrite the Helm chart `version` to `X.Y.Z` and `appVersion` to `vX.Y.Z`
- package and push the Helm chart to `oci://ghcr.io/crust-gather`

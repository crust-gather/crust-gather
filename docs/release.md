## How to release crust-gather

This can be done in a few simple steps:
1. Running `make update-crate version=X.Y.Z`, where `X`, `Y` and `Z` are all integers, to push a new commit updating both
   `Cargo.{lock,toml}` to the specified version.
    * This includes validation of the version number, ensuring that non-alphanumerical version numbers cannot be set.
2. Submitting a pull request with those changes
3. Once that pull request is merged, pushing a `vX.Y.Z` tag pointing to the latest state of the `main` branch.

Step 3 will trigger the release CI [workflow][../.github/workflows/release.yaml].
That workflow validates that Cargo files refer to a version number which is consistent with the tag. If that is not the
case, the workflow will fail with a help message.

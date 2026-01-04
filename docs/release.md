## How to release crust-gather

This can be done in one simple step:
1. pushing a `vX.Y.Z` tag pointing to the latest state of the `main` branch.

This step will trigger the release CI [workflow][../.github/workflows/release.yaml], which will publish release build against this tag.

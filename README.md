# crust-gather

## Description

`crust-gather` is a `kubectl` plugin that provides functionality similar to the `oc adm inspect` command.

## Some key features:

- Collect all available resources across the cluster.
- Collect container logs.
- Collect node kubelet logs.
- Resource collection is parallel, with exponential backoff retry and configurable timeout.
- Filter collected resources based on regexes, include/excludes, namespaces, groups or kinds.
- Advanced filters can be specified by CLI flags, configuration files or fetched from `ConfigMap` via `--config-map`.
- Collect snapshot from kubeconfig stored in the cluster via `--kubeconfig-secret-name` or `--kubeconfig-secret-label`.
- Display events in an HTML table with filtering capabilities.
- Store data in a zip/tar.gz archive.
- Hide out secret data, by providing environment keys with values to exclude during processing, or a `secrets` file.
- Browse cluster snapshot with kubectl/k9s, via a local web server.
- Collect cluster snapshot or multiple cluster snapshots in github actions workflow artifact and serve it via `crust-gather serve` ([Demo](#demo-artifact-serving)).

## Demo

[![asciicast](https://asciinema.org/a/632848.svg)](https://asciinema.org/a/632848)

### Demo artifact serving

One of the QoL features `crust-gather` provides is an ability to collect cluster snapshots during CI workflow run and serve the content like a k8s cluster after the originating cluster is removed. It can serve any number of clusters simulaniously, each cluster stored under separate context.

Easy to use version of this provided via `nix` shell script, which includes artifact download and crust-gather installation:

```bash
# Requires GITHUB_TOKEN to be set
./serve-artifact.sh --owner rancher-sandbox --repo cluster-api-provider-rke2 --artifact_id 1461387168 &
# alternatively, if you have an artifact link
./serve-artifact.sh -u https://github.com/rancher-sandbox/cluster-api-provider-rke2/actions/runs/8923331571/artifacts/1467008322 &
kubectl get ns
NAME
capd-system
capi-system
...
```

## Prerequisites

Depending on the installation type, there might be needed:

- kubectl + [krew](https://krew.sigs.k8s.io/docs/user-guide/setup/install/)
- [nix](https://nixos.org/download/)

## Usage

The plugin can be installed with `krew` and used as follows:

```bash
kubectl krew index add crust-gather https://github.com/crust-gather/crust-gather.git
kubectl krew install crust-gather/crust-gather

kubectl crust-gather --help
```

Alternatively, it can be installed standalone via `install.sh` script:
```bash
curl -sSfL https://github.com/crust-gather/crust-gather/raw/main/install.sh | sh

crust-gather --help
```

Or used with `nix`:
```bash
nix-shell -p 'callPackage (fetchGit https://github.com/crust-gather/crust-gather) {}'

kubectl-crust-gather --help
```

## Testing

To run tests locally you need to have `golang` and `kwok` pre-installed. On linux and mac this will be done automatically with the test command:
```bash
make test
```

Alternatively you can pass `GOOS` or `GOARCH` directly to the make task:
```bash
GOOS=linux GOARCH=amd64 make test
```

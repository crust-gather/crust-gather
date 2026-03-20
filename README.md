# crust-gather

## Description

`crust-gather` is a `kubectl` plugin that provides functionality, which allows to collect full or partial cluster state and serve it via an api server.

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
- Store archive in an OCI image.
- Hide out secret data, by providing environment keys with values to exclude during processing, or a `secrets` file.
- Browse cluster snapshot with kubectl/k9s, via a local web server.
- Serve OCI snapshot directly as kubernetes-like API server, without downloading the archive locally.
- Collect cluster snapshot or multiple cluster snapshots in github actions workflow artifact and serve it via `crust-gather serve` ([Demo](#demo-artifact-serving)).

## Demo

[![asciicast](https://asciinema.org/a/632848.svg)](https://asciinema.org/a/632848)

## Helm charts demo

[![asciicast](https://asciinema.org/a/AfbGJSUUtAItmQVp2EEC6j2vo.svg)](https://asciinema.org/a/AfbGJSUUtAItmQVp2EEC6j2vo)

### Artifact serving

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
kubectl krew install crust-gather

kubectl crust-gather --help
```

Alternatively, it can be installed standalone via `install.sh` script:
```bash
curl -sSfL https://github.com/crust-gather/crust-gather/raw/main/install.sh | sh

crust-gather --help
```

Or used with `nix`:
```bash
nix shell github:crust-gather/crust-gather

kubectl-crust-gather --help
```

## MCP

`crust-gather` can also run as an MCP server over stdio:

```bash
crust-gather mcp
```

This exposes the cluster collection and archive serving flows as MCP tools, so an MCP client can:

- collect an archive from a kubeconfig and optional context
- collect and push an archive to an OCI registry
- start serving an existing archive in the background
- start serving directly from an OCI registry in the background
- check serving status
- stop the current background serving task

The purpose of mcp server is to simply give LLM knowledge about the tool's existence and the availability to use it when it seems fit. Written by machine, meant for machine.

[![asciicast](https://asciinema.org/a/857581.svg)](https://asciinema.org/a/857581)

### MCP tools

`collect_archive`

- Purpose: collect a cluster snapshot into a local archive path.
- Input: kubeconfig path, optional kubeconfig context, optional archive path, selectors, redaction settings, insecure TLS flag.
- Result: runs the real collection flow and returns the target archive path, normalized selectors, redaction summary, and elapsed time.

`collect_oci`

- Purpose: collect a cluster snapshot and push it to an OCI registry.
- Input: kubeconfig path, optional context, OCI image reference, optional staging archive path, selectors, redaction settings, registry auth, insecure TLS flag.
- Result: runs the real collection flow and returns the target image reference, normalized selectors, redaction summary, and elapsed time.

`serve_archive`

- Purpose: start the existing `crust-gather serve` API in the background from a local archive.
- Input: archive path, optional kubeconfig path, optional context, optional socket.
- Result: starts the HTTP server in the background and returns the bound socket and serve source descriptor.

`serve_oci`

- Purpose: start the existing `crust-gather serve` API in the background from an OCI image, without downloading the archive locally.
- Input: OCI image reference, optional kubeconfig path, optional context, optional socket, optional registry auth.
- Result: starts the HTTP server in the background and returns the bound socket and serve source descriptor.

`serving_status`

- Purpose: report whether a background serve task is active.
- Result: returns `running` or `idle`, the active serve source if present, and the last serve exit state if available.

`stop_serving`

- Purpose: stop the current background serve task.
- Result: requests shutdown, waits for the server task to exit, and returns the final shutdown status.

### Selectors and redaction

MCP collection tools accept the same selector model as the CLI, using regex-capable include and exclude lists for:

- namespaces
- kinds
- API groups or `group/kind`
- resource names

Redaction can be provided in two ways:

- `secret_values`: literal secret strings to scrub from collected data
- `secrets_file`: a file containing one secret per line

Both are applied before data is stored in the resulting archive.

### MCP setup example

Example stdio MCP configuration for a desktop client that supports command-based servers:

```json
{
  "mcpServers": {
    "crust-gather": {
      "command": "crust-gather",
      "args": ["mcp"],
      "env": {
        "RUST_LOG": "info"
      }
    }
  }
}
```

If `crust-gather` is not in `PATH`, use the absolute path to the binary instead.

If installed via `krew`, the same MCP server can be launched through `kubectl`:

```json
{
  "mcpServers": {
    "crust-gather": {
      "command": "kubectl",
      "args": ["crust-gather", "mcp"],
      "env": {
        "RUST_LOG": "info"
      }
    }
  }
}
```

### MCP usage example

Typical flow from an MCP client:

1. Call `collect_archive` or `collect_oci` to produce snapshot data.
2. Call `serve_archive` or `serve_oci` to expose the snapshot through the HTTP API.
3. Use `serving_status` to discover the active socket and current state.
4. Call `stop_serving` when the API is no longer needed.

## Served API

The `serve` command, and the MCP `serve_archive` and `serve_oci` tools, expose a read-only Kubernetes-like API backed by collected archive data.

The `{server}` path segment is the archive or OCI-backed context name exposed by `crust-gather`.

### Endpoint purpose summary

- Discovery endpoints make `kubectl`, `k9s`, and other Kubernetes clients understand what resources exist in the served snapshot.
- List endpoints provide resource collections, including watch-style replay from recorded archive changes.
- Get endpoints provide exact stored objects for inspection and client navigation.
- The log endpoint exposes collected pod logs without needing access to the original cluster.

## Testing

To run tests locally:
```bash
make test
```

Alternatively you can pass `GOOS` or `GOARCH` directly to the make task:
```bash
GOOS=linux GOARCH=amd64 make test
```

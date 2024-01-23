# crust-gather

## Description

`crust-gather` is a `kubectl` plugin that provides functionality similar to the `oc adm inspect` command.

## Some key features:

- Collect all available resources across the cluster.
- Collect container logs.
- Collect node kubelet logs.
- Resource collection is parallel, with exponential backoff retry and configurable timeout.
- Filter collected resources based on regexes, include/excludes, namespaces, groups or kinds.
- Display events in an HTML table with filtering capabilities.
- Store data in a zip/tar.gz archive.
- Hide out secret data, by providing environment keys with values to exclude during processing.
- Browse cluster snapshot with kubectl/k9s, via a local web server.

## Demo

[![asciicast](https://asciinema.org/a/632848.svg)](https://asciinema.org/a/632848)

## Prerequisites
- kubectl
- [krew](https://krew.sigs.k8s.io/docs/user-guide/setup/install/)

## Usage

The plugin can be installed and used as follows:

```bash
kubectl krew index add crust-gather https://github.com/crust-gather/crust-gather.git
kubectl krew install crust-gather/crust-gather

kubectl crust-gather --help
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

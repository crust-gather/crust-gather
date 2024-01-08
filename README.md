# crust-gather

## Description

`crust-gather` is a `kubectl` plugin that provides functionality similar to the `oc adm inspect` command.

## Some key features:

- Collect all available resources across the cluster.
- Collect container logs.
- Filter collected resources based on regexes, include/excludes, namespaces, groups or kinds.
- Display events in an HTML table with filtering capabilities.
- Store data in a zip/tar.gz archive.
- Hide out secret data, by providing environment keys with values to exclude during processing.

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

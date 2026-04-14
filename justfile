image := "ghcr.io/crust-gather/crust-gather:1.0.0"

create-kind:
    kind create cluster

load-kind-image:
    docker build -t {{image}} .
    kind load docker-image {{image}}

clippy: fmt
    cargo clippy --fix --allow-dirty --all-targets --all-features

fmt:
    cargo fmt --all
FROM --platform=${BUILDPLATFORM} debian:bookworm-slim AS build
ARG TARGETPLATFORM
ARG BUILDPLATFORM

RUN apt-get update && apt-get install -y curl clang

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable
RUN . "$HOME/.cargo/env"

ENV PATH="/root/.cargo/bin:/usr/local/bin:$PATH"

RUN mkdir /usr/src/crust-gather
WORKDIR /usr/src/crust-gather
COPY . .

ARG features=""
RUN cargo install --locked --features=${features} --path .

FROM --platform=${BUILDPLATFORM} gcr.io/distroless/cc-debian12:nonroot
WORKDIR /apps
COPY --from=build /usr/src/crust-gather/target/release/kubectl-crust-gather /apps
EXPOSE 9095
ENTRYPOINT ["/apps/kubectl-crust-gather"]

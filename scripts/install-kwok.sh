#!/bin/bash

if [ -z "${1}" ]; then
  echo "must provide installation dir as a first parameter"
  exit 1
fi

get_var() {
    local KEY=$1
    local ENV_VAR=$2

    if [ -z "$ENV_VAR" ]; then
        echo $(go env $KEY)
    else
        echo "$ENV_VAR"
    fi
}

# Variables preparation
DEST_DIR="${1}"
KWOK_REPO=kubernetes-sigs/kwok
KWOK_RELEASE="${2:-v0.4.0}"
OS=$(get_var GOOS $GOOS)
ARCH=$(get_var GOARCH $GOARCH)

# Install kwokctl
wget -O ${DEST_DIR}/kwokctl -c "https://github.com/${KWOK_REPO}/releases/download/${KWOK_RELEASE}/kwokctl-${OS}-${ARCH}"
chmod +x ${DEST_DIR}/kwokctl

# Install kwok
wget -O ${DEST_DIR}/kwok -c "https://github.com/${KWOK_REPO}/releases/download/${KWOK_RELEASE}/kwok-${OS}-${ARCH}"
chmod +x ${DEST_DIR}/kwok

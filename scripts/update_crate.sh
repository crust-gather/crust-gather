#!/bin/bash

if [ $# -ne 1 ]; then
	echo "Usage: $0 <new-version>"
	exit 1
fi

version=$1

cargo install cargo-edit
version=${version#v}

cargo set-version $version
if [ $? -ne 0 ]; then
	echo "cargo set-version failed. Exiting."
	exit 1
fi

git add Cargo.*
git commit -m "Update Cargo files for version $version"
git push

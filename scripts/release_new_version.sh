#!/bin/bash

if [ $# -ne 1 ]; then
	echo "Usage: $0 <new-tag>"
	exit 1
fi

tag=$1

cargo install cargo-edit
version=${tag#v}

if [ $version = $tag ]; then
	echo "Setting tag as v$version" for consistency with existing tags."
	tag=v$version
fi

cargo set-version $version
git add Cargo.*
git commit -m "Update Cargo files for version $version"
git tag $tag
git push
git push --tags

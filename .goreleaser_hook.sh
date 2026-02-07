#!/usr/bin/env bash

go_arch=$1
go_os=$2
project_name=$3

# Make Go -> Rust arch/os mapping
case $go_arch in
    amd64) rust_arch='x86_64' ;;
    arm64) rust_arch='aarch64' ;;
    *) echo "unknown arch: $go_arch" && exit 1 ;;
esac
case $go_os in
    linux) rust_os='linux' ;;
    darwin) rust_os='apple-darwin' ;;
    windows) rust_os='windows' ;;
    *) echo "unknown os: $go_os" && exit 1 ;;
esac

# Find artifacts matching rust_arch and rust_os
found=$(find artifacts -name "*${rust_arch}*${rust_os}*")
echo "Looking for artifacts in $(pwd)/artifacts: $found"

# Find distribution folders matching project_name, go_os, and go_arch
dist_folders=$(find dist/${project_name}_${go_os}_${go_arch}*)
echo "Copying artifacts from corresponding folder to each of: $dist_folders"

# Check if artifacts were found
if [ -z "$found" ]; then
    echo "No artifacts found matching ${rust_arch} and ${rust_os}. Exiting."
    exit 1
fi

# Iterate over distribution folders and copy artifacts
if [ -z "$dist_folders" ]; then
    echo "No distribution folders found for ${project_name}_${go_os}_${go_arch}. Exiting."
    exit 1
fi

for folder in $dist_folders; do
    echo "Copying artifacts to $folder"
    cp ./artifacts/*${rust_arch}*${rust_os}*/* $folder
done

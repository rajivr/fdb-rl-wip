#!/usr/bin/env bash

set -e
set -u
set -o pipefail

echo "+---------------------+"
echo "| Build documentation |"
echo "+---------------------+"

cd ../../fdb-rl || { echo "cd failure"; exit 1; }

RUSTDOCFLAGS='-Z unstable-options --document-hidden-items' cargo doc --document-private-items

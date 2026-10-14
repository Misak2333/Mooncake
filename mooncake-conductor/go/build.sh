#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 CONDUCTOR_BUILD_DIR OUTPUT_BINARY" >&2
    exit 1
fi
conductor_build=$(cd "$1" && pwd)
output_binary=$2
export PKG_CONFIG_PATH="$conductor_build/pkgconfig-build${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
export CGO_ENABLED=1
cd "$(dirname "$0")"
"${GO:-go}" build -o "$output_binary" ./examples/basic

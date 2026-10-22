#!/usr/bin/env bash
# Shallow-clone the core reference repositories into refs/ at the versions Forge compiles
# against (see Cargo.lock), so the code you read matches the code you build.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p refs
clone() { # repo tag dir
  if [ ! -d "refs/$3" ]; then
    git clone --depth 1 --branch "$2" "https://github.com/$1" "refs/$3" || git clone --depth 1 "https://github.com/$1" "refs/$3"
  fi
}
clone apache/datafusion 55.2.0 datafusion
clone vortex-data/vortex 0.89.0 vortex
clone apache/arrow-rs 59.3.1 arrow-rs
clone apache/arrow-rs-object-store v0.13.2 arrow-rs-object-store
clone open-telemetry/opentelemetry-proto v1.9.0 opentelemetry-proto
clone open-telemetry/semantic-conventions main semantic-conventions
echo "refs/ ready"

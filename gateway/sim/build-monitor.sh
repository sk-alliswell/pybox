#!/bin/sh
# Build the self-contained monitor page: the engine compiled to WebAssembly,
# the scenarios, and the validation table (from the reference files, or from
# the measured logs when their folder is given).
#   ./build-monitor.sh [OUT.html] [DIR_WITH_MEASURED_LOGS]
set -eu
cd "$(dirname "$0")"
out="${1:-target/monitor.html}"
data="${2:-}"
cargo build --release --locked --bin gwsim
cargo build --profile wasm --locked --target wasm32-unknown-unknown --lib
if [ -n "$data" ]; then
    target/release/gwsim validate-all scenarios/validation.toml --data "$data" --json target/validation.json >/dev/null
else
    target/release/gwsim validate-all scenarios/validation.toml --reference reference --json target/validation.json >/dev/null
fi
target/release/gwsim monitor --wasm target/wasm32-unknown-unknown/wasm/gwsim.wasm --out "$out" --validation target/validation.json \
    scenarios/example-fleet.toml scenarios/example-fleet-xl.toml scenarios/sat-smart.toml scenarios/sat-direct-patient.toml scenarios/sat-bounded-patient.toml \
    scenarios/sat-direct.toml scenarios/sat-plain.toml scenarios/sat-bounded.toml scenarios/sat-tight.toml \
    scenarios/example-pinned-backends.toml scenarios/example-shared-backends.toml \
    scenarios/example-litellm-peak.toml scenarios/example-agents-tools.toml
   

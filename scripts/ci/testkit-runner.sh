#!/usr/bin/env bash
set -euo pipefail

# Shared owns locked package selection and Cargo executable admission;
# Testkit owns server setup and admission.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
if [[ "$#" != 0 && ( "$#" != 1 || "$1" != --check ) ]]; then
  echo 'usage: testkit-runner.sh [--check]' >&2
  exit 2
fi
export PATH="$ROOT/.tools/host/bin:$PATH"
export CARGO_HOME="$ROOT/.cache/cargo/icydb"
if bash "$ROOT/scripts/dev/install-rust-tools.sh" --consumer "$ROOT" \
  --package ic-testkit --lockfile "$ROOT/Cargo.lock" --bin ic-testkit-server --profile release "$@"; then
  exit 0
else
  status=$?
  printf 'Prepare the locked Testkit selection with: make install-testkit\n' >&2
  exit "$status"
fi

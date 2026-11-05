#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
cd "$ROOT"

# Exclude procedural-macro execution from the target library graph: declarations
# still need derive macros, but runtime libraries must not acquire a generator.
graph() {
  cargo tree --locked --offline --no-default-features \
    --edges normal,no-proc-macro --prefix none --format '{p}|{f}' "$@"
}

require_runtime_graph() {
  local selected="$1"
  if rg '^(ctor|proc-macro-crate|proc-macro2|quote|syn) v|^ic-wire v[^|]+\|.*typescript' <<< "$selected"; then
    echo 'Runtime libraries must not select code generation or Wire metadata.' >&2
    exit 1
  fi
}

runtime="$(graph -p icydb)"
require_runtime_graph "$runtime"
if rg '^ic-testkit v' <<< "$runtime"; then
  echo 'Runtime consumers must not select native test infrastructure.' >&2
  exit 1
fi
wasm_testing="$(graph -p icydb --features testing --target wasm32-unknown-unknown)"
if rg '^ic-testkit v' <<< "$wasm_testing"; then
  echo 'The testing feature must not pull Testkit into Wasm.' >&2
  exit 1
fi
model="$(graph -p icydb-model)"
require_runtime_graph "$model"
if rg '^icydb(-core)? v' <<< "$model"; then
  echo 'Model-only consumers must remain independent of the database runtime.' >&2
  exit 1
fi

build="$(graph -p icydb --features build)"
if ! rg -q '^ctor v' <<< "$build"; then
  echo 'Explicit host generation must select declaration registration.' >&2
  exit 1
fi
if rg '^ic-wire v[^|]+\|.*typescript' <<< "$build"; then
  echo 'Actor generation must not implicitly select Wire metadata.' >&2
  exit 1
fi

metadata="$(graph -p icydb --features wire-schema)"
if ! rg -q '^ic-wire v[^|]+\|.*typescript' <<< "$metadata"; then
  echo 'Wire metadata must explicitly select its upstream generator.' >&2
  exit 1
fi
if rg '^ctor v' <<< "$metadata"; then
  echo 'Wire metadata must not implicitly select model registration.' >&2
  exit 1
fi

echo 'Native runtime, model, build and Wire metadata graphs are independent.'

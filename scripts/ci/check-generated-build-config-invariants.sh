#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if ! command -v rg >/dev/null 2>&1; then
  echo "[ERROR] ripgrep (rg) is required for generated build config invariant checks." >&2
  exit 1
fi

violations="$(
  rg -n --no-heading --color=never \
    'icydb::build::|icydb::build_with_options!|icydb_build::(BuildOptions|BuildSqlUpdatePolicy|generate_with_options|build_with_options!)|build_with_options!' \
    crates canisters testing schema scripts \
    --glob '*.rs' \
    --glob '!crates/icydb-build/src/**' \
    --glob '!crates/icydb-config/src/**' \
    || true
)"

if [[ -n "$violations" ]]; then
  echo "[ERROR] Generated canister build scripts must use icydb_config::build_configured_canister!()." >&2
  echo "[ERROR] Raw BuildOptions/build_with_options use is restricted to the build/config owner crates." >&2
  echo "$violations" >&2
  exit 1
fi

configured_name_violations="$(
  find canisters -name build.rs -print0 \
    | xargs -0 awk '
        function is_snake(name) {
          return name ~ /^[a-z][a-z0-9_]*$/
        }

        /build_configured_canister!\(/ {
          in_macro = 1
          arg = 0
          next
        }

        in_macro && /^[[:space:]]*\);/ {
          in_macro = 0
          next
        }

        in_macro && NF {
          arg += 1
          if (arg == 3) {
            line = $0
            sub(/^[[:space:]]*"/, "", line)
            sub(/".*$/, "", line)
            if (!is_snake(line)) {
              printf "%s:%d:%s\n", FILENAME, FNR, $0
            }
            in_macro = 0
          }
        }
      ' \
    || true
)"

if [[ -n "$configured_name_violations" ]]; then
  echo "[ERROR] Configured canister build names must be lower snake_case." >&2
  echo "$configured_name_violations" >&2
  exit 1
fi

echo "Generated build config invariants passed."

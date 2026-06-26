#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

# shellcheck source=scripts/ci/invariant-common.sh
source "$ROOT/scripts/ci/invariant-common.sh"

require_rg "executor no-production-panics invariant"

status=0
while IFS= read -r -d '' file; do
  hits="$(
    awk '
      function brace_delta(line,    i, ch, delta) {
        delta = 0
        for (i = 1; i <= length(line); i += 1) {
          ch = substr(line, i, 1)
          if (ch == "{") {
            delta += 1
          } else if (ch == "}") {
            delta -= 1
          }
        }
        return delta
      }

      function cfg_test_or_benchmark(line) {
        if (line !~ /^[[:space:]]*#\[cfg\(/) {
          return 0
        }
        if (line ~ /not[[:space:]]*\([[:space:]]*test/) {
          return 0
        }
        return line ~ /(^|[^[:alnum:]_])test([^[:alnum:]_]|$)/ ||
          line ~ /feature[[:space:]]*=[[:space:]]*"executor-benchmarks"/
      }

      function reset_skip() {
        skip_cfg = 0
        skip_started = 0
        skip_depth = 0
      }

      skip_cfg {
        if (!skip_started) {
          if ($0 ~ /^[[:space:]]*#/) {
            next
          }
          delta = brace_delta($0)
          if ($0 ~ /;/ && delta == 0) {
            reset_skip()
            next
          }
          if (delta != 0) {
            skip_started = 1
            skip_depth = delta
            if (skip_depth <= 0) {
              reset_skip()
            }
          }
          next
        }

        skip_depth += brace_delta($0)
        if (skip_depth <= 0) {
          reset_skip()
        }
        next
      }

      cfg_test_or_benchmark($0) {
        skip_cfg = 1
        skip_started = 0
        skip_depth = 0
        next
      }

      $0 ~ /[.]expect[(]|[.]unwrap[(]|panic![(]|(^|[^[:alnum:]_])assert![(]/ {
        print FILENAME ":" FNR ":" $0
      }
    ' "$file"
  )"

  if [[ -n "$hits" ]]; then
    if (( status == 0 )); then
      echo "[ERROR] Production executor code must return typed errors instead of panicking." >&2
      echo "[ERROR] Offending patterns: .unwrap(), .expect(), panic!, assert!." >&2
    fi
    printf '%s\n' "$hits" >&2
    status=1
  fi
done < <(
  find crates/icydb-core/src/db/executor \
    -type f \
    -name '*.rs' \
    ! -path '*/tests/*' \
    ! -name 'tests.rs' \
    ! -name '*_tests.rs' \
    ! -name 'test_*.rs' \
    -print0
)

exit "$status"

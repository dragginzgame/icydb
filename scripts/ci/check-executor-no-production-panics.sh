#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"
cd "$ROOT"

# shellcheck source=scripts/ci/invariant-common.sh
source "$ROOT/scripts/ci/invariant-common.sh"

require_rg "executor no-production-panics invariant"

status=0
# Capture discovery first; process-substitution failure would be invisible to
# the loop's exit status. An empty or missing production inventory also fails.
files="$(rg --files --hidden --no-ignore crates/icydb-core/src/db/executor --glob '*.rs' "${COMMON_GLOBS[@]}")"
while IFS= read -r file; do
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

      function cfg_test_item(line) {
        # Only skip attributes that require test. Merely mentioning test in
        # cfg(any(test, feature = "sql")) does not exclude production builds.
        return line ~ /^[[:space:]]*#\[cfg\([[:space:]]*test[[:space:]]*\)\]/ || \
          line ~ /^[[:space:]]*#\[cfg\([[:space:]]*all\([[:space:]]*test[[:space:]]*[,)]/
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

      cfg_test_item($0) {
        skip_cfg = 1
        skip_started = 0
        skip_depth = 0
        next
      }

      $0 ~ /^[[:space:]]*\/\// { next }

      $0 ~ /[.](expect|unwrap)[[:space:]]*[(]|(^|[^[:alnum:]_])(panic|assert(_eq|_ne)?|unreachable|todo|unimplemented)[[:space:]]*!/ {
        print FILENAME ":" FNR ":" $0
      }
    ' "$file"
  )"

  if [[ -n "$hits" ]]; then
    if (( status == 0 )); then
      echo "[ERROR] Production executor code must return typed errors instead of panicking." >&2
      echo "[ERROR] Offending patterns: .unwrap(), .expect(), panic!, assert!, assert_eq!, assert_ne!, unreachable!, todo!, unimplemented!." >&2
    fi
    printf '%s\n' "$hits" >&2
    status=1
  fi
done <<< "$files"

exit "$status"

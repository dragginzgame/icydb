#!/usr/bin/env bash
set -euo pipefail

# Qualify actual execution and failure propagation, without parsing version-
# dependent MAKEFLAGS syntax. The isolated recipe only prints a marker and fails;
# no consumer Makefile is loaded and no caller settings or artifacts are changed.
[[ $# -le 1 ]] || { echo 'usage: check-make-execution.sh [GNU_MAKE]' >&2; exit 2; }
# Child Make reads MAKEFLAGS, not MFLAGS. Qualify each independently so replacing
# MAKEFLAGS cannot conceal the parent's retained invocation modes. Let GNU Make
# parse both encodings, including the different long-option form in Make 3.81.
for flags in "${MAKEFLAGS-}" "${MFLAGS-}"; do
    status=0
    output="$(MAKEFLAGS="$flags" "${1:-make}" --no-print-directory -f - MAKEFILES= 2>&1 <<'MAKE'
.PHONY: shared-tooling-execution-check
shared-tooling-execution-check:
	@printf '%s\n' shared-tooling-make-executed
	@exit 23
MAKE
    )" || status=$?
    if [[ "$status" != 2 ]] || ! printf '%s\n' "$output" | grep -Fx shared-tooling-make-executed >/dev/null; then
        echo 'shared-tooling requires recipe execution and failure propagation; remove Make ignore-errors, dry-run, question, touch and version-only modes' >&2
        exit 1
    fi
done

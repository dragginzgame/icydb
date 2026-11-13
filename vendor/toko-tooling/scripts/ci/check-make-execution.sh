#!/usr/bin/env bash
set -euo pipefail

# Qualify actual execution and failure propagation, without parsing version-
# dependent MAKEFLAGS syntax. The isolated recipe only prints a marker and fails;
# no consumer Makefile is loaded and no caller settings or artifacts are changed.
[[ $# -le 2 && ( $# -lt 2 || "$2" == --dry-run ) ]] || {
    echo 'usage: check-make-execution.sh [GNU_MAKE [--dry-run]]' >&2; exit 2;
}
# Child Make reads MAKEFLAGS, not MFLAGS. Qualify each independently so replacing
# MAKEFLAGS cannot conceal the parent's retained invocation modes. Let GNU Make
# parse both encodings, including the different long-option form in Make 3.81.
for flags in "${MAKEFLAGS-}" "${MFLAGS-}"; do
    status=0
    output="$(MAKEFLAGS="$flags" "${1:-make}" --no-print-directory -f - MAKEFILES= 2>&1 <<'MAKE'
.PHONY: toko-tooling-execution-check
toko-tooling-execution-check:
	@printf '%s\n' toko-tooling-make-executed
	@exit 23
MAKE
    )" || status=$?
    if [[ "${2:-}" == --dry-run ]]; then
        # GNU Make must print both recipes without executing either. Allow
        # jobserver diagnostics from this isolated probe, as execution mode does.
        if [[ "$status" != 0 ]] ||
            ! printf '%s\n' "$output" | grep -Fx "printf '%s\n' toko-tooling-make-executed" >/dev/null ||
            ! printf '%s\n' "$output" | grep -Fx 'exit 23' >/dev/null ||
            printf '%s\n' "$output" | grep -Fx toko-tooling-make-executed >/dev/null; then
            echo 'TOKO_TOOLING_DRY_RUN=1 requires make -n' >&2
            exit 1
        fi
    elif [[ "$status" != 2 ]] || ! printf '%s\n' "$output" | grep -Fx toko-tooling-make-executed >/dev/null; then
        echo 'toko-tooling requires recipe execution and failure propagation; remove Make ignore-errors, dry-run, question, touch and version-only modes' >&2
        exit 1
    fi
done

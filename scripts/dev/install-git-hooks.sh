#!/usr/bin/env bash
set -euo pipefail

# Opt-in, repository-local setup. Never replace another hook installation.
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root"
[[ "$(git rev-parse --show-toplevel)" == "$root" ]] || { echo 'hook setup requires the repository root.' >&2; exit 1; }
[[ -f Cargo.toml && -f Makefile ]] || { echo 'hook setup requires a Rust repository and its Makefile fmt target.' >&2; exit 1; }
[[ ! -L .githooks && -f .githooks/pre-commit && ! -L .githooks/pre-commit && -x .githooks/pre-commit ]] || { echo 'tracked .githooks/pre-commit must be a regular executable file in a regular hook directory.' >&2; exit 1; }

if current="$(git config --get core.hooksPath)"; then
    if [[ "$current" != .githooks ]]; then
        printf 'hook setup refused to replace core.hooksPath=%s; reconcile the existing hooks first.\n' "$current" >&2
        exit 1
    fi
else
    status=$?
    [[ "$status" == 1 ]] || exit "$status"
    hooks="$(git rev-parse --git-path hooks)"
    for existing in "$hooks"/*; do
        case "$existing" in *.sample) continue ;; esac
        if [[ -f "$existing" && -x "$existing" ]]; then
            printf 'hook setup refused to disable existing hook: %s; reconcile it first.\n' "$existing" >&2
            exit 1
        fi
    done
fi
git config --local core.hooksPath .githooks
echo 'Installed repository-local formatting hook: .githooks/pre-commit'

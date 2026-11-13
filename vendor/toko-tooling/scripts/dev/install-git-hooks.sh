#!/usr/bin/env bash
set -euo pipefail

# Opt-in, repository-local setup. Never replace another hook installation.
[[ $# == 2 && "$1" == --consumer && -n "$2" ]] || {
    echo 'usage: install-git-hooks.sh --consumer <repository>' >&2; exit 2;
}
tooling_root="$(cd -P "${BASH_SOURCE[0]%/*}/../.." && printf '%s/.' "$PWD")"
tooling_root="${tooling_root%/.}"
root="$2"
[[ "$root" == /* ]] || root="$PWD/$root"
root="$(cd -P "$root" && printf '%s/.' "$PWD")"
root="${root%/.}"
cd "$root"
hook_dir=.githooks
if [[ "$tooling_root" == "$root/vendor/toko-tooling" ]]; then hook_dir=vendor/toko-tooling/.githooks; fi
# Preserve literal trailing newlines; remove only Git's record terminator.
repository_root="$(git rev-parse --show-toplevel && printf '.')"
repository_root="${repository_root%$'\n.'}"
repository_root="$(cd -P "$repository_root" && printf '%s/.' "$PWD")"
repository_root="${repository_root%/.}"
[[ "$repository_root" == "$root" ]] || { echo 'hook setup requires the repository root.' >&2; exit 1; }
[[ -f Cargo.toml && -f Makefile ]] || { echo 'hook setup requires a Rust repository and its Makefile fmt target.' >&2; exit 1; }
[[ ! -L "$hook_dir" && -f "$hook_dir/pre-commit" && ! -L "$hook_dir/pre-commit" && -x "$hook_dir/pre-commit" ]] || { echo 'selected pre-commit must be a regular executable file in a regular hook directory.' >&2; exit 1; }
[[ -f "$tooling_root/scripts/ci/check-make-execution.sh" && ! -L "$tooling_root/scripts/ci/check-make-execution.sh" ]] || { echo 'hook setup requires the shared Make execution check.' >&2; exit 1; }

if current="$(git config --get core.hooksPath && printf '.')"; then
    current="${current%$'\n.'}"
    if [[ "$current" != "$hook_dir" ]]; then
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
git config --local core.hooksPath "$hook_dir"
echo "Installed repository-local formatting hook: $hook_dir/pre-commit"

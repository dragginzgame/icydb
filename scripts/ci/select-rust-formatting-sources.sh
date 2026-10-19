#!/usr/bin/env bash
# Shared companions: scripts/ci/fixture-lifecycle.sh
set -euo pipefail
[[ $# == 1 ]] || { echo 'usage: select-rust-formatting-sources.sh ROOT' >&2; exit 2; }
root="$1"
[[ "$root" == /* ]] || root="$PWD/$root"
root="$(cd -P "$root" && printf '%s/.' "$PWD")"
root="${root%/.}"
helper="${BASH_SOURCE[0]}"
[[ "$helper" == /* ]] || helper="$PWD/$helper"
# shellcheck source=scripts/ci/fixture-lifecycle.sh
source "${helper%/*}/fixture-lifecycle.sh"
fixture="$(fixture_create "${TMPDIR:-/tmp}/rust-formatting-sources.XXXXXX")"
fixture_complete=false
trap 'fixture_finish "$?" "$fixture" "Rust source selection retained"' EXIT
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_COMMON_DIR GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_NAMESPACE
# Include tracked deletions: the disposable caller must remove those paths too.
# No partial list is published if Git or any path admission fails.
git -C "$root" ls-files -z --cached --others --exclude-standard -- \
    '*.rs' Cargo.toml '**/Cargo.toml' > "$fixture/paths"
while IFS= read -r -d '' path; do
    case "$path" in
        ''|/*|..|../*|*/../*|*/..|./*|*/./*|*/.|.git|.git/*)
            echo "invalid source path: $path" >&2; exit 2 ;;
    esac
    if [[ -e "$root/$path" || -L "$root/$path" ]]; then
        [[ -f "$root/$path" && ! -L "$root/$path" ]] || {
            echo "source is not a regular file: $path" >&2; exit 2;
        }
        parent="$root/."
        if [[ "$path" == */* ]]; then
            parent="$(cd -P "${root}/${path%/*}" && printf '%s/.' "$PWD")"
        fi
        parent="${parent%/.}"
        case "$parent/" in "$root/"*) ;; *) echo "source escapes root: $path" >&2; exit 2 ;; esac
    fi
done < "$fixture/paths"
cat "$fixture/paths"
fixture_complete=true

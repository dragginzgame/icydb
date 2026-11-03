#!/usr/bin/env bash
# Source-only receipt mechanics. Caller owns policy, trusted state directory,
# selected files and exclusive source/state custody throughout release delivery.

release_receipt_identity() {
    printf '%s\n' "${RELEASE_SOURCE:?}" "${RELEASE_PREVIOUS:?}" "${RELEASE_VERSION:?}" \
        "${RELEASE_DATE:?}" "${RELEASE_REMOTE:?}" "${RELEASE_BRANCH:?}" || return
    if [[ -n "${RELEASE_GROUP:-}" ]]; then
        [[ "$RELEASE_GROUP" =~ ^[A-Za-z0-9][A-Za-z0-9_-]*$ ]] || return 1
        printf 'release-group:%s\n' "$RELEASE_GROUP" || return
        if [[ -n "${RELEASE_COMMIT:-}" ]]; then
            git rev-parse --verify "$RELEASE_COMMIT:ci/cargo-release-groups.json" || return
        else
            git hash-object -- ci/cargo-release-groups.json || return
        fi
    fi
    git remote get-url --push --all "$RELEASE_REMOTE"
}

release_receipt_payload() {
    local commit="$1" path
    shift
    [[ $# -gt 0 ]] || return 1
    for path in "$@"; do
        if [[ -n "$commit" ]]; then
            git rev-parse --verify "$commit:$path" || return
        else
            git hash-object -- "$path" || return
        fi
    done
}

# Generate into a private adjacent temporary file; never clobber a receipt if
# generation fails, and never follow an existing receipt symlink.
release_receipt_write() {
    local receipt="$1" temporary
    shift
    [[ ! -L "$receipt" && ( ! -e "$receipt" || -f "$receipt" ) ]] || return 1
    temporary="$(mktemp "$receipt.XXXXXX")" || return
    if "$@" > "$temporary" && mv -f -- "$temporary" "$receipt"; then
        return 0
    fi
    rm -f -- "$temporary"
    return 1
}

# Exact byte comparison also rejects changed trailing newlines and symlinks.
release_receipt_check() {
    local receipt="$1" temporary result=1
    shift
    [[ -f "$receipt" && ! -L "$receipt" ]] || return 1
    temporary="$(mktemp "$receipt.XXXXXX")" || return
    if "$@" > "$temporary" && cmp -s -- "$temporary" "$receipt"; then
        result=0
    fi
    rm -f -- "$temporary" || return
    return "$result"
}

# Preserve a previous attempt before any append/replacement. Admit the complete
# set first; absent files are allowed, but links, directories and basename
# collisions refuse. A failed copy leaves active evidence and its archive intact.
# Caller owns exclusive custody of the trusted containing directories.
release_receipt_archive() {
    local template="$1" file other archive='' name
    shift
    [[ $# -gt 0 ]] || return 1
    local names=()
    for file in "$@"; do
        [[ ! -L "$file" && ( ! -e "$file" || -f "$file" ) ]] || return 1
        name="${file##*/}"
        [[ -n "$name" ]] || return 1
        for other in ${names[@]+"${names[@]}"}; do [[ "$other" != "$name" ]] || return 1; done
        names+=("$name")
    done
    for file in "$@"; do
        [[ -e "$file" ]] || continue
        if [[ -z "$archive" ]]; then
            archive="$(mktemp -d "$template")" || return
            printf 'Release evidence archive: %s\n' "$archive" >&2
        fi
        cp -p -- "$file" "$archive/${file##*/}" || return
        cmp -- "$file" "$archive/${file##*/}" || return
    done
}

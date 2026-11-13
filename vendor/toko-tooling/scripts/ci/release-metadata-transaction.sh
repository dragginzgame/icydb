#!/usr/bin/env bash
# Source-only preparation mechanics. Callers own rendering, validation, the
# selected relative file inventory and exclusive source/state namespace custody.

# Refuse aliasing through any selected path component, not only its final name.
release_metadata_regular() {
    local root="$1" path="$2" parent
    [[ -n "$path" && "$path" != /* ]] || return 1
    case "/$path/" in *'/../'*|*'/./'*|*'//'*) return 1 ;; esac
    case "$path" in *$'\n'*|*$'\r'*) return 1 ;; esac
    parent="$path"
    while :; do
        [[ ! -L "$root/$parent" ]] || return 1
        [[ "$parent" == */* ]] || break
        parent="${parent%/*}"
    done
    [[ -f "$root/$path" ]]
}

# Run in a subshell so traps never replace the adapter's traps. RENDER receives
# original and candidate roots; CHECK receives no arguments and reads the working
# payload. Both callbacks must propagate failures explicitly (or be executables).
# No Git, Cargo, staging, version selection or release-recovery dispatch occurs.
# Caught failures restore bytes/modes and retain originals/candidates. SIGKILL
# leaves the printed evidence for the caller's existing reconciliation workflow.
release_metadata_prepare() (
    set -euo pipefail
    [[ $# -ge 4 ]] || exit 2
    local template="$1" render="$2" check="$3" file other scratch
    shift 3
    local selected=() installed=false complete=false
    for file in "$@"; do
        release_metadata_regular . "$file" || exit 1
        for other in ${selected[@]+"${selected[@]}"}; do
            [[ "$file" != "$other" ]] || exit 1
        done
        selected+=("$file")
    done
    scratch="$(mktemp -d "$template")" || exit $?
    printf 'Release preparation originals retained: %s\n' "$scratch" >&2
    # Traps retain even partial backups. Installation begins only after all
    # selected originals and all candidates have been admitted successfully.
    # shellcheck disable=SC2329 # Invoked by the EXIT trap in this subshell.
    release_metadata_finish() {
        local status=$? path
        trap - EXIT INT TERM
        if [[ "$complete" == true ]]; then
            rm -rf -- "$scratch" || status=$?
        else
            [[ "$status" != 0 ]] || status=1
            if [[ "$installed" == true ]]; then
                for path in "${selected[@]}"; do
                    if release_metadata_regular . "$path" &&
                        release_metadata_regular "$scratch/original" "$path" &&
                        cp -p -- "$scratch/original/$path" "./$path"; then
                        :
                    else
                        printf 'Metadata restoration failed: %s\n' "$path" >&2
                        status=1
                    fi
                done
            fi
            printf 'Failed preparation; originals retained: %s\n' "$scratch" >&2
        fi
        exit "$status"
    }
    trap release_metadata_finish EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    mkdir "$scratch/original" "$scratch/candidate" || exit $?
    for file in "${selected[@]}"; do
        if [[ "$file" == */* ]]; then
            mkdir -p -- "$scratch/original/${file%/*}" "$scratch/candidate/${file%/*}" || exit $?
        fi
        cp -p -- "./$file" "$scratch/original/$file" || exit $?
        cmp -- "./$file" "$scratch/original/$file" || exit $?
    done
    "$render" "$scratch/original" "$scratch/candidate" || exit $?
    for file in "${selected[@]}"; do
        release_metadata_regular "$scratch/candidate" "$file" || exit 1
        release_metadata_regular . "$file" || exit 1
        cmp -- "./$file" "$scratch/original/$file" || exit $?
    done
    installed=true
    for file in "${selected[@]}"; do
        cp -- "$scratch/candidate/$file" "./$file" || exit $?
    done
    "$check" || exit $?
    # Validation is read-only: a successful callback cannot silently alter the
    # rendered payload that it has just qualified.
    for file in "${selected[@]}"; do
        release_metadata_regular . "$file" || exit 1
        cmp -- "$scratch/candidate/$file" "./$file" || exit $?
    done
    complete=true
)

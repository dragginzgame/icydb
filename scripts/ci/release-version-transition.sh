#!/usr/bin/env bash

release_version_is_valid() {
    [[ "$1" =~ ^(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})$ ]]
}

release_next_version() {
    local bump="$1"
    local version="$2"
    local major minor patch

    if ! release_version_is_valid "$version"; then
        echo "Invalid workspace version: $version" >&2
        return 1
    fi
    IFS=. read -r major minor patch <<< "$version"

    case "$bump" in
        patch)
            printf '%s.%s.%s\n' "$major" "$minor" "$((10#$patch + 1))"
            ;;
        minor)
            printf '%s.%s.0\n' "$major" "$((10#$minor + 1))"
            ;;
        major)
            printf '%s.0.0\n' "$((10#$major + 1))"
            ;;
        *)
            echo "Unsupported release bump: $bump" >&2
            return 1
            ;;
    esac
}

release_transition_is_valid() {
    local bump="$1"
    local candidate_version="$2"
    local release_version="$3"
    local expected_version

    if ! release_version_is_valid "$release_version"; then
        echo "Invalid workspace version: $release_version" >&2
        return 1
    fi
    expected_version="$(release_next_version "$bump" "$candidate_version")" || return 1
    if [[ "$release_version" != "$expected_version" ]]; then
        echo "Expected $bump transition $candidate_version -> $expected_version; found $release_version" >&2
        return 1
    fi
}

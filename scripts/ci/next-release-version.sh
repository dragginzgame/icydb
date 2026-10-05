#!/usr/bin/env bash
set -euo pipefail

[[ $# -eq 2 ]] || { echo 'usage: next-release-version.sh X.Y.Z patch|minor|major' >&2; exit 2; }
version="$1"
kind="$2"
[[ "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]] || exit 2
IFS=. read -r major minor patch <<<"$version"
maximum_component=9223372036854775807
for component in "$major" "$minor" "$patch"; do
    # Bound arithmetic before Bash converts a decimal to a signed integer.
    # shellcheck disable=SC2071 # Decimal strings must be bounded before numeric conversion.
    [[ ${#component} -lt 19 || ( ${#component} -eq 19 && "$component" < "$maximum_component" ) ]] || {
        echo 'release version component exceeds supported arithmetic' >&2
        exit 2
    }
done
case "$kind" in
    patch) patch=$((patch + 1)) ;;
    minor) minor=$((minor + 1)); patch=0 ;;
    major) major=$((major + 1)); minor=0; patch=0 ;;
    *) echo 'release kind must be patch, minor or major' >&2; exit 2 ;;
esac
printf '%s.%s.%s\n' "$major" "$minor" "$patch"

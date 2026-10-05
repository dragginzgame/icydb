#!/usr/bin/env bash
set -euo pipefail
# Explicit metadata set; Git records preserve spaces and terminate every path.
printf '%s\0' Cargo.toml Cargo.lock README.md CHANGELOG.md
previous_line="${RELEASE_PREVIOUS%.*}"
candidate_line="${RELEASE_VERSION%.*}"
printf '%s\0' "docs/changelog/$candidate_line.md"
if [[ "$previous_line" != "$candidate_line" && -f "docs/changelog/$previous_line.md" ]]; then
    printf '%s\0' "docs/changelog/$previous_line.md"
fi
git ls-files -z -- ':(glob)crates/**/Cargo.toml' ':(glob)testing/**/Cargo.toml'

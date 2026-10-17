#!/usr/bin/env bash
# Shared companions: scripts/dev/cargo-tool-build.sh scripts/dev/tool-store.sh scripts/dev/install-rust-tools.sh scripts/ci/verify-file-checksum.sh scripts/ci/tool-catalog-records.pl
set -euo pipefail

# Repository CI only: prepared host tools and compiler are required. This reads
# the installer's identity; it neither installs tools nor admits cached bytes.
[[ $# == 1 ]] || { echo 'usage: cargo-tool-cache-key.sh TOOL-VERSIONS' >&2; exit 2; }
ROOT="${BASH_SOURCE[0]}"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
# shellcheck source=scripts/dev/tool-store.sh
source "$ROOT/scripts/dev/tool-store.sh"
# shellcheck source=scripts/dev/cargo-tool-build.sh
source "$ROOT/scripts/dev/cargo-tool-build.sh"
cargo_build_init
key="$(
    {
        printf '%s\n' "$build_identity" "${ImageOS:-}" "${ImageVersion:-}"
        # Cache the same admitted records used by installation and inspection.
        perl "$ROOT/scripts/ci/tool-catalog-records.pl" rust "$1" || exit $?
        for file in scripts/ci/cargo-tool-cache-key.sh scripts/ci/tool-catalog-records.pl \
            scripts/dev/install-rust-tools.sh scripts/dev/tool-store.sh \
            scripts/ci/verify-file-checksum.sh; do
            bash "$ROOT/scripts/ci/verify-file-checksum.sh" --print sha256 "$ROOT/$file" || exit $?
        done
    } | perl -MDigest::SHA=sha256_hex -0777 -ne 'print sha256_hex($_)'
)"
# tool_store_root already rejects CR/LF before these GitHub output records.
printf 'key=%s\nroot=%s\n' "$key" "$store_root"

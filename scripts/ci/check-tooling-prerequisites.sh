#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in selection|archive) ;; *) echo 'usage: check-tooling-prerequisites.sh selection|archive' >&2; exit 2 ;; esac
missing=false
for tool in perl jq yq; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "Toko Tooling requires $tool on PATH (yq must be Mike Farah yq v4). Prepare host tools before retrying." >&2
        missing=true
    fi
done
[[ "$missing" == false ]] || exit 1
if ! printf 'name = "toko"\n' | yq -p toml -o json '.name' - 2>/dev/null | jq -e '. == "toko"' >/dev/null 2>&1; then
    echo 'Toko Tooling requires Mike Farah yq v4 with TOML input and JSON output support.' >&2
    exit 1
fi
modules=(JSON::PP Digest::SHA)
if [[ "$1" == archive ]]; then modules+=(Archive::Tar IO::Uncompress::Gunzip); fi
for module in "${modules[@]}"; do
    if ! perl "-M$module" -e 1 >/dev/null 2>&1; then
        echo "Toko Tooling requires Perl module $module. Install it for the selected Perl before retrying." >&2
        missing=true
    fi
done
if [[ "$1" == selection ]] && ! command -v cargo >/dev/null 2>&1; then
    echo 'Toko Tooling requires Cargo on PATH; prepare the selected Rust compiler first.' >&2
    missing=true
fi
[[ "$missing" == false ]]

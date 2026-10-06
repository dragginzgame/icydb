#!/usr/bin/env bash
set -euo pipefail

# Exercise IcyDB's real Make gate in a disposable current-manifest inventory.
# Git exports/staging are fixture-only; no commits or dependency resolution occur.
unset MAKEFLAGS MAKEOVERRIDES MFLAGS GNUMAKEFLAGS
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
export PATH="$ROOT/.tools/host/bin:$PATH"
YQ="${YQ:-$ROOT/.tools/host/bin/yq}"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-cargo-adoption.XXXXXX")"
finish() {
  local status=$?
  if [[ "$status" == 0 ]]; then rm -rf "$fixture"
  else echo "Cargo adoption fixture retained: $fixture" >&2; fi
}
trap finish EXIT
mkdir "$fixture/repo"
git -C "$ROOT" archive --format=tar HEAD > "$fixture/source.tar"
tar -xf "$fixture/source.tar" -C "$fixture/repo"
git -C "$ROOT" ls-files --cached --others --exclude-standard -z -- '*Cargo.toml' > "$fixture/manifests"
while IFS= read -r -d '' path; do
  [[ ! -f "$ROOT/$path" ]] || cp "$ROOT/$path" "$fixture/repo/$path"
done < "$fixture/manifests"
for path in Cargo.lock Makefile rust-toolchain.toml ci/dependency-pinning-exceptions.json \
  scripts/ci/check-dependency-pins.sh scripts/ci/dependency-pins.jq \
  scripts/ci/read-cargo-workspace-version.sh scripts/ci/check-dependency-graph-invariants.sh; do
  cp "$ROOT/$path" "$fixture/repo/$path"
done
cd "$fixture/repo"
git init --quiet
git add -- .
check() {
  make --no-print-directory check-dependency-pins "YQ=$YQ" > "$fixture/check.log" 2>&1
}
check || { cat "$fixture/check.log" >&2; exit 1; }
bash scripts/ci/check-dependency-graph-invariants.sh > "$fixture/graph.log" 2>&1
version="$(make --no-print-directory -s version "YQ=$YQ")"
cp crates/icydb-diagnostic-code/Cargo.toml "$fixture/member.toml"
# Renamed dependencies inherit their identity from the root catalog.
requirement="$("$YQ" -p toml -o json '.workspace.dependencies.remain' Cargo.toml |
  jq -r 'if type == "string" then . else .version end')"
printf '\n[workspace.dependencies.adoption_alias]\npackage = "remain"\nversion = "%s"\n' "$requirement" >> Cargo.toml
for section in dependencies dev-dependencies build-dependencies 'target."cfg(unix)".build-dependencies'; do
  cp "$fixture/member.toml" crates/icydb-diagnostic-code/Cargo.toml
  printf '\n[%s.adoption_alias]\nworkspace = true\n' "$section" \
    >> crates/icydb-diagnostic-code/Cargo.toml
  check || { cat "$fixture/check.log" >&2; exit 1; }
  printf 'version = "1"\n' >> crates/icydb-diagnostic-code/Cargo.toml
  if check; then echo 'member dependency source override admitted' >&2; exit 1; fi
done
cp "$fixture/member.toml" crates/icydb-diagnostic-code/Cargo.toml
printf '\n[build-dependencies.missing_alias]\nworkspace = true\n' >> crates/icydb-diagnostic-code/Cargo.toml
if check; then echo 'missing root dependency alias admitted' >&2; exit 1; fi
sed "s/version = { workspace = true }/version = \"$version\"/" "$fixture/member.toml" \
  > crates/icydb-diagnostic-code/Cargo.toml
if check; then echo 'independent member version admitted' >&2; exit 1; fi
cp "$fixture/member.toml" crates/icydb-diagnostic-code/Cargo.toml
check || { cat "$fixture/check.log" >&2; exit 1; }
echo 'IcyDB Cargo inheritance gate and retained graph policies passed'

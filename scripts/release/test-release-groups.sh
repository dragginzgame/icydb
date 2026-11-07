#!/usr/bin/env bash
# Qualify real consumer transactions in disposable Cargo/Git workspaces.
set -euo pipefail
unset MAKEFLAGS MAKEOVERRIDES MFLAGS RELEASE_COMMIT ICYDB_RELEASE_ROOT
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export PATH="$root/.tools/host/bin:$PATH"
export YQ="${YQ:-$root/.tools/host/bin/yq}"
export CARGO_RELEASE_METADATA_MAX_BYTES=8388608
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-release-groups.XXXXXX")"
fixture_complete=false
# shellcheck source=scripts/ci/fixture-lifecycle.sh
source "$root/scripts/ci/fixture-lifecycle.sh"
trap 'fixture_finish "$?" "$fixture" "Release group fixture retained"' EXIT
: "${CARGO_RELEASE_METADATA_EDITOR:?prepare the locked IC Build editor}"
# Synthetic commits/tags and the local-only remote must not inherit developer
# signing/hooks configuration or a caller-selected repository/index.
mkdir -p "$fixture/templates"
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TEMPLATE_DIR="$fixture/templates"
git_context="$(git rev-parse --local-env-vars)"
while IFS= read -r variable; do
    unset "$variable"
done <<< "$git_context"
expect_failure() {
    if "$@" > "$fixture/refused.log" 2>&1; then echo "Unexpected success: $*" >&2; exit 1; fi
}
# Real Make and direct publication entrypoints refuse before any tool setup.
for selection in missing empty unknown; do
    for target in release-patch release-minor release-major release-resume release-preflight publish; do
        case "$selection" in
            missing) expect_failure env -u RELEASE_GROUP make -s -C "$root" "$target" ;;
            empty) expect_failure make -s -C "$root" "$target" RELEASE_GROUP= ;;
            unknown) expect_failure make -s -C "$root" "$target" RELEASE_GROUP=unknown ;;
        esac
    done
    case "$selection" in
        missing) expect_failure env -u RELEASE_GROUP bash "$root/scripts/ci/publish-workspace.sh" ;;
        empty) expect_failure env RELEASE_GROUP= bash "$root/scripts/ci/publish-workspace.sh" ;;
        unknown) expect_failure env RELEASE_GROUP=unknown bash "$root/scripts/ci/publish-workspace.sh" ;;
    esac
done
mkdir -p "$fixture/base/scripts" "$fixture/base/ci" "$fixture/base/releases"
cp -R "$root/scripts/ci" "$root/scripts/release" "$fixture/base/scripts/"
cd "$fixture/base"
printf '[workspace]\nresolver = "3"\nmembers = ["crates/*"]\n\n[workspace.package]\nedition = "2024"\nrust-version = "1.99.1"\n\n[workspace.dependencies]\n' > Cargo.toml
packages=(icydb-diagnostic-code icydb-schema icydb-model-macros icydb-model icydb-core icydb icydb-cli)
for package in "${packages[@]}"; do
    mkdir -p "crates/$package/src"
    printf '%s = { path = "crates/%s", version = "=0.294.0" }\n' "$package" "$package" >> Cargo.toml
    printf '[package]\nname = "%s"\nversion = "0.294.0"\nedition.workspace = true\n' "$package" > "crates/$package/Cargo.toml"
    printf 'pub fn fixture() {}\n' > "crates/$package/src/lib.rs"
done
printf 'database_alias = { package = "icydb", path = "crates/icydb", version = "=0.294.0" }\n' >> Cargo.toml
printf '\n[dependencies]\nicydb.workspace = true\n' >> crates/icydb-cli/Cargo.toml
for group in database cli; do
    mkdir -p "releases/$group"
    for name in CHANGELOG 0.295; do
        printf '# Notes\n\n## [0.295.0]\n\nSelected group change.\n' > "releases/$group/$name.md"
    done
done
jq -n --args '$ARGS.positional as $names | {schema:1, shared_metadata_files:["ci/dependency-pinning-exceptions.json"], groups:
    ["database","cli"] | map(. as $g | ($names | map(select(if $g == "cli" then . == "icydb-cli" else . != "icydb-cli" end))) as $members |
    {name:$g,primary:(if $g == "cli" then "icydb-cli" else "icydb" end),version:"package",members:$members,dependencies:($members + (if $g == "database" then ["database_alias"] else [] end)),
     changelog:("releases/"+$g+"/CHANGELOG.md"),metadata_files:["releases/"+$g+"/0.295.md"]})}' \
    "${packages[@]}" > ci/cargo-release-groups.json
jq -n --args '$ARGS.positional | map({rule:"cargo-exact",file:"Cargo.toml",subject:.,value:"=0.294.0",reason:"Coordinated fixture contract.",evidence:"CHANGELOG.md"})' \
    "${packages[@]}" database_alias > ci/dependency-pinning-exceptions.json
printf '# Fixture index\n' > CHANGELOG.md
cargo generate-lockfile --offline > "$fixture/lock.log" 2>&1
git init -q
git config user.email fixture@example.invalid
git config user.name 'Release fixture'
git add .
git commit -qm 'Fixture source'
for group in database cli; do
    cp -R "$fixture/base" "$fixture/$group"
    cd "$fixture/$group"
    export RELEASE_GROUP="$group" RELEASE_PREVIOUS=0.294.0 RELEASE_VERSION=0.295.0 RELEASE_DATE=2026-11-02 RELEASE_KIND=minor
    RELEASE_SOURCE="$(git rev-parse HEAD)"
    export RELEASE_SOURCE
    unset RELEASE_RECEIPT_DIR
    receipts="$(git rev-parse --absolute-git-dir)/release-state/receipts"
    other=cli; [[ "$group" != cli ]] || other=database
    bash scripts/ci/cargo-release-group.sh plan "$group" > "$fixture/$group-plan.json"
    jq -e --arg other "$other" '.files | all(.[]; startswith("releases/"+$other+"/") | not)' "$fixture/$group-plan.json" >/dev/null
    # Fail after emitting valid candidate bytes: neither partial output nor a
    # failed editor may mutate any file in the encompassing transaction.
    cat > "$fixture/failing-editor" <<EDITOR
#!/usr/bin/env bash
"$CARGO_RELEASE_METADATA_EDITOR" "\$@"
exit 7
EDITOR
    chmod +x "$fixture/failing-editor"
    expect_failure env CARGO_RELEASE_METADATA_EDITOR="$fixture/failing-editor" bash scripts/release/prepare.sh
    git diff --exit-code > "$fixture/$group-rollback.log"
    bash scripts/release/prepare.sh > "$fixture/$group-prepare.log" 2>&1
    [[ -f "$receipts/$group-v0.295.0.candidate" ]]
    cmp "$fixture/base/releases/$other/CHANGELOG.md" "releases/$other/CHANGELOG.md"
    cmp "$fixture/base/releases/$other/0.295.md" "releases/$other/0.295.md"
    [[ "$(RELEASE_GROUP="$other" bash scripts/release/version.sh)" == 0.294.0 ]]
    [[ "$(bash scripts/release/version.sh)" == 0.295.0 ]]
    # Even an allowed-family path outside this group's exact plan is refused.
    other_manifest=crates/icydb-cli/Cargo.toml
    [[ "$group" != cli ]] || other_manifest=crates/icydb/Cargo.toml
    cmp "$fixture/base/$other_manifest" "$other_manifest"
    printf '\n# unrelated edit\n' >> "$other_manifest"
    expect_failure bash scripts/ci/release-candidate-receipt.sh record minor "$RELEASE_SOURCE"
    cp "$fixture/base/$other_manifest" "$other_manifest"
    git add .
    bash scripts/ci/release-candidate-receipt.sh verify-staged
    git commit -qm "Release $group 0.295.0"
    RELEASE_COMMIT="$(git rev-parse HEAD)"
    export RELEASE_COMMIT
    bash scripts/ci/release-candidate-receipt.sh verify-commit
    git tag -a "$group-v0.295.0" -m 'fixture release'
    bash scripts/ci/record-release-gate-receipt.sh
    [[ -f "$receipts/$group-v0.295.0.commit" ]]
    # Cache cleanup in this disposable fixture must not erase qualification.
    mkdir -p .cache/release-receipts
    rm -rf .cache
    bash scripts/ci/verify-release-gate-receipt.sh "$RELEASE_COMMIT"
    # A missing canonical receipt cannot be replaced by a cache lookalike.
    mv "$receipts/$group-v0.295.0.commit" "$fixture/$group-saved.commit"
    mkdir -p .cache/release-receipts
    cp "$fixture/$group-saved.commit" ".cache/release-receipts/$group-v0.295.0.commit"
    expect_failure bash scripts/ci/verify-release-gate-receipt.sh "$RELEASE_COMMIT"
    mv "$fixture/$group-saved.commit" "$receipts/$group-v0.295.0.commit"
    rm -rf .cache
    expect_failure env RELEASE_GROUP="$other" bash scripts/ci/verify-release-gate-receipt.sh "$RELEASE_COMMIT"
    # Recovery reads the selected commit even when HEAD has newer source.
    printf '// later source\n' >> crates/icydb/src/lib.rs
    git add .; git commit -qm 'Later source'
    bash scripts/ci/release-candidate-receipt.sh verify-commit
    # A later source edit cannot be represented as a release-only projection.
    expect_failure env RELEASE_COMMIT="$(git rev-parse HEAD)" bash scripts/ci/release-candidate-receipt.sh verify-commit
    unset RELEASE_COMMIT
    # Publication is exercised with real admission and registry/Cargo substitutes.
    mkdir "$fixture/$group-bin"
    real_cargo="$(command -v cargo)"
    cat > "$fixture/$group-bin/cargo" <<CARGO
#!/usr/bin/env bash
if [[ "\${1:-}" == publish ]]; then printf '%s\n' "\$*" >> "$fixture/$group-published"; exit 0; fi
exec "$real_cargo" "\$@"
CARGO
    chmod +x "$fixture/$group-bin/cargo"
    printf '#!/usr/bin/env bash\nexit 1\n' > scripts/ci/check-crates-io-version.sh
    git add .; git commit -qm 'Registry substitute'
    PATH="$fixture/$group-bin:$PATH" PUBLISH_DRY_RUN=1 PUBLISH_VERIFY=always bash scripts/ci/publish-workspace.sh > "$fixture/$group-publication.log" 2>&1
    if [[ "$group" == cli ]]; then
        [[ "$(wc -l < "$fixture/$group-published")" == 1 ]]
        grep -q 'publish -p icydb-cli ' "$fixture/$group-published"
        expect_failure env PACKAGE=icydb PUBLISH_VALIDATE_ONLY=1 bash scripts/ci/publish-workspace.sh
    else
        [[ "$(wc -l < "$fixture/$group-published")" == 6 ]]
        if grep -q 'icydb-cli' "$fixture/$group-published"; then exit 1; fi
        expect_failure env PUBLISH_FROM=icydb-cli PUBLISH_VALIDATE_ONLY=1 bash scripts/ci/publish-workspace.sh
    fi
done
# Exercise an actual saved runner attempt against a local-only remote. Gate
# execution is substituted; metadata, receipts, tags and recovery are real.
cp -R "$fixture/base" "$fixture/recovery"
cd "$fixture/recovery"
mkdir make
cp "$root/make/"{release,execution}.mk make/
cat > Makefile <<'MAKE'
.DEFAULT_GOAL := release-version
# Exercise the inner saved runner explicitly; ordinary CI has no parent batch.
# Automatic capture is qualified separately by the release-batch fixtures.
RELEASE_BATCH_ACTIVE := 1
include make/release.mk
release-version:
	@bash scripts/release/version.sh
release-preflight release-verify:
	@:
release-prepare-version:
	@bash scripts/release/prepare.sh
release-prepared-check:
	@bash scripts/release/check-metadata.sh
release-files:
	@bash scripts/release/files.sh
release-commit-check:
	@bash scripts/ci/release-candidate-receipt.sh verify-staged
release-committed-check:
	@bash scripts/ci/release-candidate-receipt.sh verify-commit
release-tagged-check:
	@bash scripts/ci/record-release-gate-receipt.sh
	@test -f "$(RECOVERY_ALLOW)"
release-push-check:
	@bash scripts/ci/verify-release-gate-receipt.sh "$(RELEASE_COMMIT)"
MAKE
git branch -M main
git add .; git commit -qm 'Recovery fixture callbacks'
git init -q --bare "$fixture/remote.git"
git remote add origin "$fixture/remote.git"
git push -qu origin main
export RELEASE_GROUP=cli RELEASE_RECEIPT_DIR="$fixture/recovery-receipts" RECOVERY_ALLOW="$fixture/allow-push"
expect_failure make -s release-minor
[[ -f .git/release-state/cli-0.295.0.plan ]]
[[ "$(sed -n '11p' .git/release-state/cli-0.295.0.plan)" == tag ]]
[[ "$(git log -1 --format=%s)" == 'Release cli 0.295.0' ]]
expect_failure env RELEASE_GROUP=database make -s release-resume VERSION=0.295.0
touch "$RECOVERY_ALLOW"
make -s release-resume VERSION=0.295.0 > "$fixture/recovery.log" 2>&1
[[ "$(sed -n '11p' .git/release-state/cli-0.295.0.plan)" == complete ]]
[[ "$(git --git-dir="$fixture/remote.git" rev-parse refs/heads/main)" == "$(git rev-parse HEAD)" ]]
cmp "$fixture/base/releases/database/CHANGELOG.md" releases/database/CHANGELOG.md
printf '%s\n' 'Group transactions, rollback, exact receipts, recovery and publication selection passed.'
fixture_complete=true

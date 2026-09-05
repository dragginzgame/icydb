#!/usr/bin/env bash
set -euo pipefail

# Exercise capture/provenance decisions without Cargo, a replica, or Twiggy.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEST_ROOT="$(mktemp -d)"
trap 'find "$TEST_ROOT" -depth -delete' EXIT
FIXTURE="$TEST_ROOT/repository"
ARTIFACTS="$FIXTURE/artifacts/wasm-size"
mkdir -p "$FIXTURE/scripts/ci" "$FIXTURE/docs/reports/recurring" "$ARTIFACTS" "$TEST_ROOT/bin"
cp "$ROOT/scripts/ci/wasm-audit-report.sh" "$ROOT/scripts/ci/wasm-report-common.sh" "$FIXTURE/scripts/ci/"
ln -s "$(command -v true)" "$TEST_ROOT/bin/twiggy"
export PATH="$TEST_ROOT/bin:$PATH"

revision="1111111111111111111111111111111111111111"
tree="2222222222222222222222222222222222222222"
digest="3333333333333333333333333333333333333333333333333333333333333333"
run=0

write_fixture() {
    local canister="$1" stem hash gz_hash
    stem="$ARTIFACTS/$canister.wasm-release"
    printf '\000asm\001\000\000\000' > "$stem.final-deployable.wasm"
    gzip -n -9 -c "$stem.final-deployable.wasm" > "$stem.final-deployable.wasm.gz"
    hash="$(sha256sum "$stem.final-deployable.wasm")"
    gz_hash="$(sha256sum "$stem.final-deployable.wasm.gz")"
    printf 'Fixture summary\n' > "$stem.summary.md"
    jq -n --arg canister "$canister" --arg revision "$revision" --arg tree "$tree" \
        --arg digest "$digest" --arg hash "${hash%% *}" --arg gz_hash "${gz_hash%% *}" \
        --argjson gz_bytes "$(wc -c < "$stem.final-deployable.wasm.gz")" '{
        format_version: 1,
        measurement_profile: {identity: "icydb-wasm-footprint/0.251/v1"},
        canister: $canister, profile: "wasm-release", sql_variant: "sql-on",
        provenance: {
            source_revision: $revision, source_tree: $tree, source_dirty: false,
            lockfile_sha256: $digest, workspace_root: "/fixture",
            cargo_target_dir: "/fixture/target", rust_toolchain: "fixture-rust"
        },
        tools: {ic_wasm_sha256: $digest, wasm_opt_sha256: $digest},
        pipeline: {
            build_profile: "production", candid_metadata: "enabled",
            post_link_transform: "binaryen-132-oz+bulk-memory+sign-ext+nontrapping-float-to-int+one-caller-inline-max-0/v1",
            final_deployable_stage: "binaryen_oz_wasm",
            path_remapping: "workspace=/w;cargo-registry=/c;rust-library=/r"
        },
        artifacts: {
            final_deployable_wasm: {bytes: 8, sha256: $hash},
            final_deployable_wasm_gz: {bytes: $gz_bytes, sha256: $gz_hash}
        }
    }' > "$stem.report.json"
}

run_subject() {
    run=$((run + 1))
    report_dir="$TEST_ROOT/run-$run"
    bash "$FIXTURE/scripts/ci/wasm-audit-report.sh" --skip-build \
        --date 2026-09-05 --report-dir "$report_dir" --canister default_empty "$@" > "$TEST_ROOT/output" 2>&1
}

expect_failure() {
    if run_subject "$@"; then
        printf 'Expected capture rejection in run %s\n' "$run" >&2
        exit 1
    fi
    test ! -e "$report_dir/report.md"
}

change_report() {
    local canister="$1" expression="$2"
    local report="$ARTIFACTS/$canister.wasm-release.report.json"
    jq "$expression" "$report" > "$report.updated"
    mv "$report.updated" "$report"
}

# No Git repository exists here: reused artifacts must supply the identity.
write_fixture default_empty
write_fixture default_empty_metrics
run_subject --canister default_empty_metrics
grep -Fq "code snapshot identifier: \`$revision\`" "$report_dir/report.md"
grep -Fq "source tree: \`$tree\`" "$report_dir/report.md"
grep -Fq "lockfile SHA-256: \`$digest\`" "$report_dir/report.md"

# Preserve a comparable byte-count baseline for subsequent capture decisions.
baseline="$FIXTURE/docs/reports/recurring/2026/01/01/wasm-footprint/01"
mkdir -p "${baseline%/*}"
cp -R "$report_dir" "$baseline"
run_subject --canister default_empty_metrics
grep -Fq 'comparability status: `comparable`' "$report_dir/report.md"

# Completed reports and their evidence are immutable, even with --report-dir.
existing_report="$report_dir"
report_hash="$(sha256sum "$existing_report/report.md")"
evidence="$existing_report/artifacts/wasm-footprint.default_empty.wasm-release.sql-on.size-report.json"
evidence_hash="$(sha256sum "$evidence")"
change_report default_empty '.provenance.source_dirty = true'
if run_subject --report-dir "$existing_report"; then
    echo 'Expected existing report directory to be rejected' >&2
    exit 1
fi
test "$(sha256sum "$existing_report/report.md")" = "$report_hash"
test "$(sha256sum "$evidence")" = "$evidence_hash"

# A reserved or incomplete directory must also be left to its original run.
reserved="$TEST_ROOT/reserved"
mkdir "$reserved"
if run_subject --report-dir "$reserved"; then
    echo 'Expected reserved report directory to be rejected' >&2
    exit 1
fi
test -z "$(find "$reserved" -mindepth 1 -print -quit)"
write_fixture default_empty

# Hashes protect even equal-length corruption; byte counts are checked too.
printf '\000asm\002\000\000\000' > "$ARTIFACTS/default_empty.wasm-release.final-deployable.wasm"
expect_failure
write_fixture default_empty
printf 'corrupt gzip' > "$ARTIFACTS/default_empty.wasm-release.final-deployable.wasm.gz"
expect_failure
write_fixture default_empty
change_report default_empty '.artifacts.final_deployable_wasm.bytes += 1'
expect_failure
write_fixture default_empty
change_report default_empty '.artifacts.final_deployable_wasm_gz.bytes += 1'
expect_failure

# Required identity fields and the requested measurement subject must agree.
for expression in \
    'del(.provenance.source_revision)' \
    '.provenance.source_dirty = "false"' \
    '.canister = "sql"' \
    '.profile = "release"' \
    '.sql_variant = "sql-off"'; do
    write_fixture default_empty
    change_report default_empty "$expression"
    expect_failure
done

# Every actor must come from one recorded source/build identity.
for expression in \
    '.provenance.source_revision = "4444444444444444444444444444444444444444"' \
    '.provenance.source_tree = "4444444444444444444444444444444444444444"' \
    '.provenance.lockfile_sha256 = "4444444444444444444444444444444444444444444444444444444444444444"' \
    '.provenance.rust_toolchain = "other-rust"' \
    '.provenance.source_dirty = true' \
    '.tools.ic_wasm_sha256 = "4444444444444444444444444444444444444444444444444444444444444444"'; do
    write_fixture default_empty
    write_fixture default_empty_metrics
    change_report default_empty_metrics "$expression"
    expect_failure --canister default_empty_metrics
done

# Consistently dirty evidence can be captured, but cannot claim comparability.
write_fixture default_empty
change_report default_empty '.provenance.source_dirty = true'
run_subject
grep -Fq 'non-comparable (current artifacts were built from a dirty source tree)' "$report_dir/report.md"
grep -Fq 'source dirty: `true`' "$report_dir/report.md"

assert_baseline() {
    local expected="$1" observed
    observed="$(sed -n 's/^- compared baseline report path: `\(.*\)`$/\1/p' "$report_dir/report.md")"
    test "$observed" = "$expected"
}

run_canonical() {
    local day="$1" sequence="$2" destination
    shift 2
    destination="$FIXTURE/docs/reports/recurring/${day//-//}/wasm-footprint/$sequence"
    run_subject --date "$day" --report-dir "$destination" "$@"
    report_dir="$destination"
}

# Automatic runs establish the day baseline and keep subsequent runs pinned.
write_fixture default_empty
for sequence in 01 02; do
    run=$((run + 1))
    bash "$FIXTURE/scripts/ci/wasm-audit-report.sh" --skip-build \
        --canister default_empty --date 2026-02-02 > "$TEST_ROOT/output" 2>&1
    report_dir="$FIXTURE/docs/reports/recurring/2026/02/02/wasm-footprint/$sequence"
    if [[ "$sequence" == "01" ]]; then
        assert_baseline 'docs/reports/recurring/2026/01/01/wasm-footprint/01/report.md'
    else
        assert_baseline 'docs/reports/recurring/2026/02/02/wasm-footprint/01/report.md'
    fi
done

# An explicit canonical path carries its day even when --date differs.
run_canonical 2026-02-02 03 --date 2026-12-01
assert_baseline 'docs/reports/recurring/2026/02/02/wasm-footprint/01/report.md'
grep -Fq '# Recurring Audit - Wasm Footprint (2026-02-02)' "$report_dir/report.md"

# Backdated first runs cannot select later dates or later runs from their day.
run_canonical 2026-02-01 01
assert_baseline 'docs/reports/recurring/2026/01/01/wasm-footprint/01/report.md'
run_canonical 2025-12-01 01
assert_baseline 'N/A'

# Noncanonical output locations use the requested day and its run-01 baseline.
run_subject --date 2026-02-02
assert_baseline 'docs/reports/recurring/2026/02/02/wasm-footprint/01/report.md'

# A first run seeks the latest compatible earlier report, across the full batch.
change_report default_empty '.tools.ic_wasm_sha256 = "4444444444444444444444444444444444444444444444444444444444444444"'
run_canonical 2026-02-03 01
assert_baseline 'N/A'
write_fixture default_empty
run_canonical 2026-02-04 01
assert_baseline 'docs/reports/recurring/2026/02/02/wasm-footprint/03/report.md'
write_fixture default_empty_metrics
run_canonical 2026-02-05 01 --canister default_empty_metrics
assert_baseline 'docs/reports/recurring/2026/01/01/wasm-footprint/01/report.md'

# A non-comparable same-day baseline stays pinned; it is never replaced by an
# older compatible report to manufacture a passing comparison.
run_canonical 2026-02-03 02
assert_baseline 'docs/reports/recurring/2026/02/03/wasm-footprint/01/report.md'
grep -Fq 'comparability status: `non-comparable' "$report_dir/report.md"

# A missing daily baseline cannot be replaced by another day's report. When
# run 01 is later captured, it must ignore the already-present same-day run 02.
run_canonical 2026-02-06 02
assert_baseline 'N/A'
run_canonical 2026-02-06 01
assert_baseline 'docs/reports/recurring/2026/02/05/wasm-footprint/01/report.md'
printf 'Wasm audit capture checks passed: %s cases\n' "$run"

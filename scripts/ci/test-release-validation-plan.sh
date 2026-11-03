#!/usr/bin/env bash
# Actual IcyDB policy/selector/receipt and Shared dispatch, with substitute gates.
# These fixtures qualify admission/recovery, never product or native-host gates.
set -euo pipefail
unset MAKEFLAGS MFLAGS MAKEOVERRIDES GNUMAKEFLAGS MAKEFILES GITHUB_STEP_SUMMARY
root="$(cd "$(dirname "$0")/../.." && pwd -P)"
mkdir -p "$root/target"
fixture="$(mktemp -d "$root/target/release-plan-test.XXXXXX")"
trap 'printf "IcyDB validation-plan fixture evidence: %s\n" "$fixture"' EXIT
mkdir -p "$fixture/repo/scripts/"{ci,release} "$fixture/repo/ci" "$fixture/bin"
cp "$root/ci/"{release-validation.json,release-validation-contract.json,cargo-release-groups.json} "$fixture/repo/ci/"
cp "$root/Makefile" "$fixture/repo/"
cp "$root/scripts/release/"{validation-plan.pl,validation-receipt.pl,qualify-validation-plan.sh} "$fixture/repo/scripts/release/"
cp "$root/scripts/ci/"{run-validation-targets.sh,run-icydb-validation-targets.sh,check-make-execution.sh,verify-file-checksum.sh,validation-input-checkpoint.pl,release-validation-plan.pl} "$fixture/repo/scripts/ci/"
# Keep the real Makefile byte-identical, including aggregate recipes. Only
# fixture stage execution is substituted; the execution-mode probe uses Make.
export ICYDB_FIXTURE_MAKE
ICYDB_FIXTURE_MAKE="$(command -v make)"
cat > "$fixture/bin/make" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *' -f - '* ]]; then exec "$ICYDB_FIXTURE_MAKE" "$@"; fi
[[ $# == 5 && "$1" == --no-print-directory && "$2" == -C && "$4" == -- ]]
cd "$3"
exec bash fixture-stage.sh "$5"
STUB
chmod +x "$fixture/bin/make"
mkdir -p "$fixture/cargo-probe"
cat > "$fixture/cargo-probe/cargo" <<'CARGO'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$ICYDB_CARGO_TRACE"
[[ "$1" != test || "${RUST_TEST_THREADS:-}" == 2 ]]
CARGO
chmod +x "$fixture/cargo-probe/cargo"
export ICYDB_CARGO_TRACE="$fixture/cargo-calls"
# Execute the actual four Make recipes, substituting only Cargo. This qualifies
# package/feature/target dispatch without claiming product or package execution.
"$ICYDB_FIXTURE_MAKE" --no-print-directory -C "$root" \
    "CARGO_WORK_ENV=PATH=\"$fixture/cargo-probe:$PATH\"" \
    clippy-cli check-cli test-cli package-cli > "$fixture/cargo-projection.log"
cat > "$fixture/cargo-expected" <<'EXPECTED'
clippy --locked -p icydb-cli --all-targets -- -D warnings
check --locked -p icydb-cli --all-targets
test --locked --no-fail-fast -p icydb-cli --all-targets
package --locked -p icydb-cli
EXPECTED
cmp "$fixture/cargo-expected" "$ICYDB_CARGO_TRACE"
export PATH="$fixture/bin:$PATH"
cd "$fixture/repo"
cat > fixture-stage.sh <<'STAGE'
set -eu
printf '%s\n' "$1" >> target/executed
[[ "$1" != "${FAIL_STAGE:-}" ]] || exit 27
if [[ "$1" == "${INTERRUPT_STAGE:-}" ]]; then kill -TERM "$PPID"; exit 143; fi
if [[ "$1" == "${DRIFT_STAGE:-}" ]]; then printf '// drift\n' >> crates/icydb-cli/src/main.rs; fi
STAGE
printf '/target/\n' > .gitignore
mkdir -p crates/{icydb-cli,icydb-core}/src
printf 'fn main() {}\n' > crates/icydb-cli/src/main.rs
printf '// engine\n' > crates/icydb-core/src/lib.rs
printf 'fixture lock\n' > Cargo.lock
git init -q
git config user.name 'IcyDB validation fixture'
git config user.email fixture@example.invalid
git add .
git commit -qm baseline
base="$(git rev-parse HEAD)"
for group in database cli; do git tag -am baseline "$group-v1.0.0"; done
mkdir -p target
cases=0
refuse() {
    cases=$((cases + 1))
    if "$@" > "$fixture/refusal-$cases.log" 2>&1; then
        printf 'Unexpected admission in case %s\n' "$cases" >&2; exit 1
    fi
}
for group in database cli; do
    git checkout -q -B "test-$group" "$base"
    if [[ "$group" == cli ]]; then source_file=crates/icydb-cli/src/main.rs
    else source_file=crates/icydb-core/src/lib.rs; fi
    printf '// owner change\n' >> "$source_file"
    git commit -qam 'owner source change'
    head="$(git rev-parse HEAD)"
    mkdir -p "target/$group"
    perl scripts/release/validation-plan.pl "$group" "$head" "$group-v1.0.0" "target/$group/auto.json" auto
    jq -e --arg group "$group" '.scope == (if $group=="cli" then "group" else "workspace" end) and
      ([.stages[] | select(.target=="clippy-cli" or .target=="check-cli" or .target=="test-cli" or .target=="package-cli") | .required] | all)' "target/$group/auto.json" > /dev/null
    : > target/executed
    bash scripts/release/qualify-validation-plan.sh "$group" "$group-v1.0.0" workspace > "$fixture/$group-full.log"
    jq -r '.stages[]' ci/release-validation.json > target/expected
    cmp target/expected target/executed
    if [[ "$group" == cli ]]; then
        : > target/executed
        FAIL_STAGE='test' bash scripts/release/qualify-validation-plan.sh cli cli-v1.0.0 auto > "$fixture/cli-selected.log"
        jq -r '.stages[] | select(.required) | .target' target/cli/auto.json > target/expected
        cmp target/expected target/executed
        receipt="$(sed -n '/\/receipt.json$/p' "$fixture/cli-selected.log")"
        jq -e '.contract=="icydb-release-validation-qualification/1" and .counts.executed==11 and .counts.reused==0 and .release_omission_admitted==false' "$receipt" > /dev/null
    fi
    FAIL_STAGE=clippy-cli refuse bash scripts/release/qualify-validation-plan.sh "$group" "$group-v1.0.0" auto
    refuse perl scripts/release/validation-plan.pl unknown "$head" "$group-v1.0.0" "target/$group/unknown.json" auto
    refuse perl scripts/release/validation-plan.pl "$group" "$base" "$group-v1.0.0" "target/$group/stale.json" auto
    refuse perl scripts/release/validation-plan.pl "$group" "$head" other-v1.0.0 "target/$group/wrong-baseline.json" auto
    printf 'changed lock\n' >> Cargo.lock
    git commit -qam 'shared lock change'
    perl scripts/release/validation-plan.pl "$group" "$(git rev-parse HEAD)" "$group-v1.0.0" "target/$group/shared.json" auto
    jq -e '.scope=="workspace" and ([.stages[].required] | all)' "target/$group/shared.json" > /dev/null
done
# Unknown/build/public-library/shared paths cannot be called CLI presentation.
for path in unknown.rs crates/icydb-cli/src/lib.rs crates/icydb-cli/build.rs crates/icydb-core/src/lib.rs; do
    git checkout -q -B expansion "$base"
    printf '// uncertain input\n' >> "$path"
    git add "$path"
    git commit -qm 'unclassified change'
    mkdir -p target/expansion
    plan="target/expansion/plan-${cases}.json"
    perl scripts/release/validation-plan.pl cli "$(git rev-parse HEAD)" cli-v1.0.0 "$plan" auto
    jq -e '.scope=="workspace" and ([.stages[].required] | all)' "$plan" > /dev/null
    cases=$((cases + 1))
done
for change in added removed recipe group; do
    git checkout -q -B mapping "$base"
    case "$change" in
      added) jq -cS '.stages += ["new-stage"]' ci/release-validation.json > target/changed.json; cp target/changed.json ci/release-validation.json ;;
      removed) jq -cS '.stages |= map(select(.!="test"))' ci/release-validation.json > target/changed.json; cp target/changed.json ci/release-validation.json ;;
      recipe) printf '\nnew-stage:\n\t@false\n' >> Makefile ;;
      group) jq '.groups += [{name:"unknown"}]' ci/cargo-release-groups.json > target/changed.json; cp target/changed.json ci/cargo-release-groups.json ;;
    esac
    git commit -qam 'mapping drift'
    refuse perl scripts/release/validation-plan.pl cli "$(git rev-parse HEAD)" cli-v1.0.0 "target/drift-$change.json" auto
done
git checkout -q -B recovery "$base"
printf '// owner change\n' >> crates/icydb-cli/src/main.rs
git commit -qam 'CLI change'
printf '// dirty\n' >> crates/icydb-cli/src/main.rs
refuse perl scripts/release/validation-plan.pl cli "$(git rev-parse HEAD)" cli-v1.0.0 target/dirty.json auto
git restore crates/icydb-cli/src/main.rs
DRIFT_STAGE=clippy-cli refuse bash scripts/release/qualify-validation-plan.sh cli cli-v1.0.0 auto
git restore crates/icydb-cli/src/main.rs
INTERRUPT_STAGE=clippy-cli refuse bash scripts/release/qualify-validation-plan.sh cli cli-v1.0.0 auto
# No failed/interrupted attempt may leave a successful receipt.
[[ "$(find target/release-validation -name receipt.json | wc -l | tr -d ' ')" == 3 ]]
bash scripts/release/qualify-validation-plan.sh cli cli-v1.0.0 auto > "$fixture/retry.log"
receipt="$(sed -n '/\/receipt.json$/p' "$fixture/retry.log")"
attempt="${receipt%/receipt.json}"
run=("$attempt"/logs/validation.*)
export RELEASE_GROUP=cli RELEASE_SOURCE
RELEASE_SOURCE="$(git rev-parse HEAD)"
cp "${run[0]}/plan-events.tsv" target/events.original
sed $'/clippy-cli\tPASS/d' target/events.original > "${run[0]}/plan-events.tsv"
refuse perl scripts/release/validation-receipt.pl "$attempt/plan.json" "${run[0]}" target/incomplete-receipt.json
cp target/events.original "${run[0]}/plan-events.tsv"
perl scripts/release/validation-receipt.pl "$attempt/plan.json" "${run[0]}" target/readmitted.json
refuse perl scripts/release/validation-receipt.pl "$attempt/plan.json" "${run[0]}" target/readmitted.json
cp "${run[0]}/timings.tsv" target/timings.original
sed $'s/clippy-cli\tPASS/clippy-cli\tFAIL/' target/timings.original > "${run[0]}/timings.tsv"
refuse perl scripts/release/validation-receipt.pl "$attempt/plan.json" "${run[0]}" target/failed-receipt.json
cp target/timings.original "${run[0]}/timings.tsv"
printf 'unknown\tPASS\tunknown.log\n' >> "${run[0]}/plan-events.tsv"
refuse perl scripts/release/validation-receipt.pl "$attempt/plan.json" "${run[0]}" target/unknown-receipt.json
cp target/events.original "${run[0]}/plan-events.tsv"
RUSTFLAGS='--cfg changed_receipt_environment' refuse perl scripts/release/validation-receipt.pl "$attempt/plan.json" "${run[0]}" target/config-drift.json
# A new tag object on the same commit also changes the admitted baseline.
baseline_object="$(jq -r .baseline_tag_object "$attempt/plan.json.context.json")"
git tag -fam replaced cli-v1.0.0 "$base" > /dev/null
refuse perl scripts/release/validation-receipt.pl "$attempt/plan.json" "${run[0]}" target/baseline-object-drift.json
git update-ref refs/tags/cli-v1.0.0 "$baseline_object"
# Moving the baseline invalidates receipt admission.
git tag -fam moved cli-v1.0.0 HEAD > /dev/null
refuse perl scripts/release/validation-receipt.pl "$attempt/plan.json" "${run[0]}" target/baseline-drift.json
printf '// moved source\n' >> crates/icydb-cli/src/main.rs
git commit -qam 'moved source'
refuse perl scripts/release/validation-receipt.pl "$attempt/plan.json" "${run[0]}" target/stale-receipt.json
printf 'IcyDB selection, exact dispatch, full expansion, required/excluded failures, drift, interruption and fresh recovery passed (%s rejection/expansion cases). Stage bodies were substitutes.\n' "$cases"

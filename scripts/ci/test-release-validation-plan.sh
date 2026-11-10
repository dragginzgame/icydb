#!/usr/bin/env bash
# Actual IcyDB policy/selector/receipt and Shared dispatch, with substitute gates.
# These fixtures qualify admission/recovery, never product or native-host gates.
set -euo pipefail
unset MAKEFLAGS MFLAGS MAKEOVERRIDES GNUMAKEFLAGS MAKEFILES GITHUB_STEP_SUMMARY
root="$(cd "$(dirname "$0")/../.." && pwd -P)"
export PATH="$root/.tools/host/bin:$PATH"
export CARGO_RELEASE_METADATA_EDITOR
export CARGO_RELEASE_METADATA_MAX_BYTES=8388608
CARGO_RELEASE_METADATA_EDITOR="$(CARGO_HOME="$root/.cache/cargo/icydb" bash "$root/scripts/release/select-editor.sh" HEAD)"
mkdir -p "$root/target"
fixture="$(mktemp -d "$root/target/release-plan-test.XXXXXX")"
trap 'printf "IcyDB validation-plan fixture evidence: %s\n" "$fixture"' EXIT
# Exercise actual consumer receipts from a UTF-8 checkout path as well as
# spaces; Shared owns reason/text decoding while IcyDB retains byte custody.
fixture_repo="$fixture/repo with spaces"$'\316\273'
mkdir -p "$fixture_repo/scripts/"{ci,release} "$fixture_repo/ci" "$fixture/bin" "$fixture/templates"
# The synthetic history belongs to this fixture, not the invoking developer's
# signing/hooks policy or repository context. Keep all overrides in this child.
export GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null GIT_TEMPLATE_DIR="$fixture/templates"
git_context="$(git rev-parse --local-env-vars)"
while IFS= read -r variable; do
    unset "$variable"
done <<< "$git_context"
cp "$root/ci/"{release-validation.json,release-validation-contract.json,cargo-release-groups.json} "$fixture_repo/ci/"
cp "$root/Makefile" "$fixture_repo/"
cp "$root/scripts/release/"{validation-plan.pl,validation-receipt.pl,validation-stages.pl,compare-validation-plans.pl,qualify-validation-plan.sh,qualify-native-validation.sh,ValidationScope.pm,validation-fingerprint.pl} "$fixture_repo/scripts/release/"
cp "$root/scripts/ci/"{run-validation-targets.sh,run-icydb-validation-targets.sh,check-make-execution.sh,verify-file-checksum.sh,validation-input-checkpoint.pl,release-validation-plan.pl,ValidationPlan.pm,validation-evidence.pl,release-receipts.sh} "$fixture_repo/scripts/ci/"
cp "$root/scripts/release/"{group.sh,prepare-extra.sh} "$fixture_repo/scripts/release/"
cp "$root/scripts/ci/cargo-release-groups.jq" "$fixture_repo/scripts/ci/"
cp "$root/scripts/ci/StableVersion.pm" "$fixture_repo/scripts/ci/"
while IFS= read -r path; do
    mkdir -p "$fixture_repo/$(dirname "$path")"
    cp "$root/$path" "$fixture_repo/$path"
done < <(jq -r '.qualification_inputs[]' "$root/ci/release-validation-contract.json")
# Keep the real Makefile byte-identical, including aggregate recipes. Only
# fixture stage execution is substituted; the execution-mode probe uses Make.
export ICYDB_FIXTURE_MAKE
ICYDB_FIXTURE_MAKE="$(command -v make)"
cat > "$fixture/bin/make" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$*" == *' -f - '* ]]; then exec "$ICYDB_FIXTURE_MAKE" "$@"; fi
if [[ "$*" == '--no-print-directory validate' ]]; then
  printf 'full\n' >> target/production-dispatch
  exit 0
fi
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
cd "$fixture_repo"
cat > fixture-stage.sh <<'STAGE'
set -eu
if [[ "$1" == fixture-child ]]; then printf 'nested consumer dispatch\n'; exit 0; fi
printf '%s\n' "$1" >> target/executed
[[ "$1" != "${FAIL_STAGE:-}" ]] || exit 27
if [[ "$1" == tools-check ]]; then bash scripts/ci/run-icydb-validation-targets.sh fixture-child; fi
if [[ "$1" == "${INTERRUPT_STAGE:-}" ]]; then kill -TERM "$PPID"; exit 143; fi
if [[ "$1" == "${DRIFT_STAGE:-}" ]]; then printf '// drift\n' >> crates/icydb-cli/src/main.rs; fi
STAGE
printf '/target/\n' > .gitignore
mkdir -p crates/{icydb-cli,icydb-core}/src
printf 'fn main() {}\n' > crates/icydb-cli/src/main.rs
printf '// engine\n' > crates/icydb-core/src/lib.rs
# The mapping fingerprint uses the canonical group/Cargo metadata projection.
# Give its consumer fixture a complete, dependency-free coordinated inventory.
printf '[workspace]\nresolver="3"\nmembers=["crates/*"]\n[workspace.package]\nedition="2024"\nrust-version="1.99.1"\n[workspace.dependencies]\n' > Cargo.toml
fixture_packages=(icydb-diagnostic-code icydb-schema icydb-model-macros icydb-model icydb-core icydb icydb-cli)
for fixture_package in "${fixture_packages[@]}"; do
    mkdir -p "crates/$fixture_package/src"
    printf '%s={path="crates/%s",version="=0.295.2"}\n' "$fixture_package" "$fixture_package" >> Cargo.toml
    printf '[package]\nname="%s"\nversion="0.295.2"\nedition.workspace=true\nrust-version.workspace=true\n' "$fixture_package" > "crates/$fixture_package/Cargo.toml"
    printf 'pub fn fixture() {}\n' > "crates/$fixture_package/src/lib.rs"
done
jq -cn --args '$ARGS.positional as $members | {schema:1,shared_metadata_files:["ci/dependency-pinning-exceptions.json"],groups:
  ["database","cli"]|map(. as $g | ($members|map(select(if $g=="cli" then .=="icydb-cli" else .!="icydb-cli" end))) as $owned |
  {name:$g,primary:(if $g=="cli" then "icydb-cli" else "icydb" end),version:"package",members:$owned,dependencies:$owned,
  changelog:("releases/"+$g+"/CHANGELOG.md"),metadata_files:[]})}' "${fixture_packages[@]}" > ci/cargo-release-groups.json
printf '[]\n' > ci/dependency-pinning-exceptions.json
for fixture_group in database cli; do
    mkdir -p "releases/$fixture_group"
    printf '# Fixture notes\n' > "releases/$fixture_group/CHANGELOG.md"
done
fixture_group_sha="$(bash scripts/ci/verify-file-checksum.sh --print sha256 ci/cargo-release-groups.json)"
jq -cS --arg hash "$fixture_group_sha" '.files["ci/cargo-release-groups.json"]=$hash' ci/release-validation-contract.json > "$fixture/initial-contract"
mv "$fixture/initial-contract" ci/release-validation-contract.json
cargo generate-lockfile --offline > "$fixture/initial-lock.log" 2>&1
git init -q
if bash scripts/release/qualify-native-validation.sh > "$fixture/native-dirty.log" 2>&1; then
    echo 'Native qualification must refuse unsettled source before setup or gates.' >&2; exit 1
fi
[[ ! -e target ]]
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
perl scripts/release/validation-stages.pl check
for admission_input in Makefile ci/cargo-release-groups.json ci/release-validation.json; do
    cp "$admission_input" "$fixture/admission-input"
    printf '\n' >> "$admission_input"
    refuse perl scripts/release/validation-stages.pl check
    mv "$fixture/admission-input" "$admission_input"
done
perl scripts/release/validation-stages.pl check
for group in database cli; do
    git checkout -q -B "test-$group" "$base"
    if [[ "$group" == cli ]]; then source_file=crates/icydb-cli/src/main.rs
    else source_file=crates/icydb-core/src/lib.rs; fi
    printf '// owner change\n' >> "$source_file"
    git commit -qam 'owner source change'
    head="$(git rev-parse HEAD)"
    mkdir -p "target/$group"
    perl scripts/release/validation-plan.pl "$group" "$head" "$group-v1.0.0" "target/$group/auto.json" auto
    jq -e --arg group "$group" '.scope == "group" and
      ([.stages[] | select(.target=="clippy-cli" or .target=="check-cli" or .target=="test-cli" or .target=="package-cli") | .required] | all == ($group=="cli"))' "target/$group/auto.json" > /dev/null
    : > target/executed
    bash scripts/release/qualify-validation-plan.sh "$group" "$group-v1.0.0" workspace > "$fixture/$group-full.log"
    jq -r '.stages[]' ci/release-validation.json > target/expected
    cmp target/expected target/executed
    if [[ "$group" == cli || "$group" == database ]]; then
        : > target/executed
        FAIL_STAGE="$( [[ "$group" == cli ]] && printf test || printf check-automation )" bash scripts/release/qualify-validation-plan.sh "$group" "$group-v1.0.0" auto > "$fixture/$group-selected.log"
        jq -r '.stages[] | select(.required) | .target' target/$group/auto.json > target/expected
        cmp target/expected target/executed
        receipt="$(sed -n '/\/receipt.json$/p' "$fixture/$group-selected.log")"
        jq -e '.contract=="icydb-release-validation-qualification/2" and .counts.executed==11 and .counts.reused==0 and .release_omission_admitted==false' "$receipt" > /dev/null
        # The excluded-failure probe deliberately changes configuration. A real
        # matched comparison must reject it, then admit a same-source/config pair.
        full="$(sed -n '\|/receipt.json$|p' "$fixture/$group-full.log")"
        RELEASE_GROUP="$group" RELEASE_SOURCE="$head" refuse perl scripts/release/compare-validation-plans.pl \
            "${full%/receipt.json}" "${receipt%/receipt.json}"
        bash scripts/release/qualify-validation-plan.sh "$group" "$group-v1.0.0" auto > "$fixture/$group-matched.log"
        selected="$(sed -n '\|/receipt.json$|p' "$fixture/$group-matched.log")"
        RELEASE_GROUP="$group" RELEASE_SOURCE="$head" perl scripts/release/compare-validation-plans.pl \
            "${full%/receipt.json}" "${selected%/receipt.json}" > "$fixture/comparison.json"
        jq -e '.full.executed==16 and .selected.executed==11 and (.excluded|length)==5 and .release_omission_admitted==false' \
            "$fixture/comparison.json" > /dev/null
        # Qualify the actual production caller with fixture-only mapping proof.
        # Its stages remain substitutes; no product/host qualification is claimed.
        git checkout -qb "production-gate-$group"
        # Use real Cargo metadata and the real group editor for the complete
        # selected-gate -> prepared/staged/committed/tagged/publication boundary.
        printf '[workspace]\nresolver = "3"\nmembers = ["crates/*"]\n[workspace.package]\nedition = "2024"\nrust-version = "1.99.1"\n[workspace.dependencies]\n' > Cargo.toml
        mini_packages=(icydb-diagnostic-code icydb-schema icydb-model-macros icydb-model icydb-core icydb icydb-cli)
        for mini in "${mini_packages[@]}"; do
            mkdir -p "crates/$mini/src"
            printf '[package]\nname = "%s"\nversion = "0.295.2"\nedition.workspace = true\nrust-version.workspace = true\n' "$mini" > "crates/$mini/Cargo.toml"
            printf 'pub fn fixture() {}\n' > "crates/$mini/src/lib.rs"
            printf '%s={path="crates/%s",version="=0.295.2"}\n' "$mini" "$mini" >> Cargo.toml
        done
        printf 'fn main() {}\n' > crates/icydb-cli/src/main.rs
        jq -cn --args '$ARGS.positional as $members | {schema:1,shared_metadata_files:["ci/dependency-pinning-exceptions.json"],groups:
          ["database","cli"]|map(. as $g | {name:$g,primary:(if $g=="cli" then "icydb-cli" else "icydb" end),version:"package",
          members:($members|map(select(if $g=="cli" then .=="icydb-cli" else .!="icydb-cli" end))),
          dependencies:($members|map(select(if $g=="cli" then .=="icydb-cli" else .!="icydb-cli" end))),
          changelog:("releases/"+$g+"/CHANGELOG.md"),metadata_files:["releases/"+$g+"/0.296.md"]})}' \
          "${mini_packages[@]}" > ci/cargo-release-groups.json
        jq -cn --args '$ARGS.positional | map(select(.!="icydb-cli") | {rule:"cargo-exact",file:"Cargo.toml",subject:.,value:"=0.295.2",reason:"Coordinated fixture contract.",evidence:"CHANGELOG.md"})' \
            "${mini_packages[@]}" > ci/dependency-pinning-exceptions.json
        for mini_group in database cli; do
            mkdir -p "releases/$mini_group"
            printf '# Notes\n\n## [0.296.0]\n\nFixture release.\n' > "releases/$mini_group/CHANGELOG.md"
            cp "releases/$mini_group/CHANGELOG.md" "releases/$mini_group/0.296.md"
        done
        mini_sha="$(bash scripts/ci/verify-file-checksum.sh --print sha256 ci/cargo-release-groups.json)"
        jq -cS --arg hash "$mini_sha" '.files["ci/cargo-release-groups.json"]=$hash' ci/release-validation-contract.json > "$fixture/mini-contract"
        mv "$fixture/mini-contract" ci/release-validation-contract.json
        cargo generate-lockfile --offline > "$fixture/mini-lock.log" 2>&1
        git add .; git commit -qm 'fixture coordinated workspace'
        perl scripts/release/validation-fingerprint.pl "$(git rev-parse HEAD)" > "$fixture/mini-fingerprint.json"
        jq -cS --slurpfile identity "$fixture/mini-fingerprint.json" \
          '{schema:1,comparisons:[. + $identity[0]]}' "$fixture/comparison.json" > ci/release-validation-qualified.json
        git add ci/release-validation-qualified.json
        git commit -qm 'fixture mapping admission'
        git tag -am baseline "$group-v0.295.2"
        printf '// production command\n' >> "$source_file"
        git commit -qam 'fixture private CLI change'
        production_source="$(git rev-parse HEAD)"
        : > target/production-dispatch
        CARGO_PROFILE_DEV_DEBUG=0 RELEASE_GROUP="$group" RELEASE_SOURCE="$production_source" RELEASE_PREVIOUS=0.295.2 \
            bash scripts/release/verify.sh > "$fixture/production-configuration.log" 2>&1
        [[ "$(cat target/production-dispatch)" == full ]]
        RUSTFLAGS='--cfg unqualified_mapping' GITHUB_STEP_SUMMARY="$fixture/fallback-summary.md" \
            RELEASE_GROUP="$group" RELEASE_SOURCE="$(git rev-parse HEAD)" RELEASE_PREVIOUS=0.295.2 \
            bash scripts/release/verify.sh > "$fixture/production-unqualified.log" 2>&1
        rg -F 'Native mapping, host/compiler or Cargo configuration is not qualified.' "$fixture/fallback-summary.md"
        rg -F 'Selection diagnostics retained:' "$fixture/production-unqualified.log"
        RELEASE_GROUP="$( [[ "$group" == cli ]] && printf database || printf cli )" GITHUB_STEP_SUMMARY="$fixture/database-summary.md" RELEASE_PREVIOUS=0.295.2 RELEASE_SOURCE="$production_source" \
            bash scripts/release/verify.sh > "$fixture/production-database.log" 2>&1
        rg -F 'Native mapping, host/compiler or Cargo configuration is not qualified.' "$fixture/database-summary.md"
        # A transitive automation helper is part of the omission mapping even
        # when the top-level roster, Makefile and dependency graph stay fixed.
        printf '\n# changed automation owner\n' >> scripts/ci/check-make-execution.sh
        git commit -qam 'fixture automation owner drift'
        perl scripts/release/validation-fingerprint.pl "$(git rev-parse HEAD)" > "$fixture/automation-fingerprint.json"
        [[ "$(jq -r .mapping_sha256 "$fixture/automation-fingerprint.json")" != "$(jq -r .mapping_sha256 "$fixture/mini-fingerprint.json")" ]]
        : > target/production-dispatch
        RELEASE_GROUP="$group" RELEASE_SOURCE="$(git rev-parse HEAD)" RELEASE_PREVIOUS=0.295.2 \
            bash scripts/release/verify.sh > "$fixture/automation-drift.log" 2>&1
        [[ "$(cat target/production-dispatch)" == full ]]
        git checkout -q --detach "$production_source"
        : > target/production-dispatch
        RELEASE_GROUP="$group" RELEASE_SOURCE="$production_source" RELEASE_PREVIOUS=0.295.2 \
            bash scripts/release/verify.sh > "$fixture/production-selected.log" 2>&1
        [[ ! -s target/production-dispatch ]]
        pointer="$(git rev-parse --absolute-git-dir)/release-state/receipts/$group-$production_source.gate"
        durable="$(dirname "$pointer")/$(cat "$pointer")"
        RELEASE_GROUP="$group" perl scripts/release/validation-gate.pl check "$durable" "$production_source" > "$fixture/durable-gate.json"
        RELEASE_GROUP="$group" refuse perl scripts/release/validation-gate.pl check "$durable" "$base"
        mv target/release-validation "$fixture/removed-validation-cache"
        RELEASE_GROUP="$group" perl scripts/release/validation-gate.pl check "$durable" "$production_source" > "$fixture/uncached-gate.json"
        cmp "$fixture/durable-gate.json" "$fixture/uncached-gate.json"
        mv "$fixture/removed-validation-cache" target/release-validation
        selected_transition() {
            local RELEASE_GROUP="$group" RELEASE_SOURCE="$production_source" RELEASE_PREVIOUS=0.295.2
            local RELEASE_VERSION=0.296.0 RELEASE_DATE=2026-11-04 RELEASE_KIND=minor RELEASE_COMMIT
            export RELEASE_GROUP RELEASE_SOURCE RELEASE_PREVIOUS RELEASE_VERSION RELEASE_DATE RELEASE_KIND RELEASE_COMMIT
            bash scripts/ci/cargo-release-group.sh prepare "$group" > "$fixture/selected-prepare.log" 2>&1
            bash scripts/release/prepare-extra.sh >> "$fixture/selected-prepare.log" 2>&1
            bash scripts/release/finalize-notes.sh >> "$fixture/selected-prepare.log" 2>&1
            bash scripts/ci/release-candidate-receipt.sh record minor "$production_source"
            candidate="$(dirname "$pointer")/$group-v0.296.0.candidate"
            [[ "$(head -n 1 "$candidate")" == "format=3:$group" ]]
            git add .
            bash scripts/ci/release-candidate-receipt.sh verify-staged
            git commit -qm 'fixture selected metadata release'
            RELEASE_COMMIT="$(git rev-parse HEAD)"
            bash scripts/ci/release-candidate-receipt.sh verify-commit
            git tag -am 'fixture selected release' $group-v0.296.0
            bash scripts/ci/record-release-gate-receipt.sh
            bash scripts/ci/verify-release-gate-receipt.sh "$RELEASE_COMMIT"
            refuse bash scripts/ci/verify-release-gate-receipt.sh "$RELEASE_COMMIT" --publication
            mv "$candidate" "$fixture/hidden-selected-candidate"
            refuse bash scripts/ci/record-release-gate-receipt.sh
            refuse bash scripts/ci/verify-release-gate-receipt.sh "$RELEASE_COMMIT"
            mv "$fixture/hidden-selected-candidate" "$candidate"
            # Own-version preparation cannot invalidate mapping qualification.
            perl scripts/release/validation-fingerprint.pl "$RELEASE_COMMIT" > "$fixture/version-fingerprint.json"
            [[ "$(jq -r .mapping_sha256 "$fixture/version-fingerprint.json")" == "$(jq -r .mapping_sha256 "$fixture/mini-fingerprint.json")" ]]
            # An interleaved other-group version is canonical metadata, not a
            # dependency/feature change. It cannot invalidate the mapping.
            other_group="$( [[ "$group" == cli ]] && printf database || printf cli )"
            RELEASE_GROUP="$other_group" bash scripts/ci/cargo-release-group.sh prepare "$other_group" > "$fixture/other-prepare.log" 2>&1
            RELEASE_GROUP="$other_group" bash scripts/release/prepare-extra.sh >> "$fixture/other-prepare.log" 2>&1
            RELEASE_GROUP="$other_group" bash scripts/release/finalize-notes.sh >> "$fixture/other-prepare.log" 2>&1
            git add .; git commit -qm 'fixture interleaved other-group metadata'
            perl scripts/release/validation-fingerprint.pl "$(git rev-parse HEAD)" > "$fixture/other-fingerprint.json"
            [[ "$(jq -r .mapping_sha256 "$fixture/other-fingerprint.json")" == "$(jq -r .mapping_sha256 "$fixture/mini-fingerprint.json")" ]]
            if [[ "$group" == database ]]; then
                printf '// next database correction\n' >> crates/icydb-core/src/lib.rs
                git commit -qam 'fixture next database correction'
                perl scripts/release/validation-plan.pl database "$(git rev-parse HEAD)" database-v0.296.0 target/interleaved.json auto
                jq -e '.scope=="group"' target/interleaved.json > /dev/null
            fi
        }
        selected_transition > "$fixture/selected-transition.log" 2>&1
        git checkout -q --detach "$production_source"
        retained_run="$(jq -r .run "$durable/gate.json")"
        cp "$durable/view/$retained_run/0.log" "$fixture/retained-original.log"
        printf 'changed proof\n' >> "$durable/view/$retained_run/0.log"
        RELEASE_GROUP="$group" refuse perl scripts/release/validation-gate.pl check "$durable" "$production_source"
        cp "$fixture/retained-original.log" "$durable/view/$retained_run/0.log"
        FAIL_STAGE="$( [[ "$group" == cli ]] && printf clippy-cli || printf clippy )" RELEASE_GROUP="$group" RELEASE_SOURCE="$production_source" RELEASE_PREVIOUS=0.295.2 \
            refuse bash scripts/release/verify.sh
        [[ ! -e "$pointer" ]]
        # Source ownership uncertainty routes through the original full gate.
        printf '// uncertain input\n' > unclassified.rs
        git add unclassified.rs; git commit -qm 'fixture shared input'
        RELEASE_GROUP="$group" RELEASE_SOURCE="$(git rev-parse HEAD)" RELEASE_PREVIOUS=0.295.2 \
            bash scripts/release/verify.sh > "$fixture/production-full.log" 2>&1
        [[ "$(cat target/production-dispatch)" == full ]]
        rg -F 'Shared or uncertain changes exceed the selected group boundary.' "$fixture/production-full.log"
        rg -F 'Changed input: unclassified.rs' "$fixture/production-full.log"
        git checkout -q "test-$group"
    fi
    FAIL_STAGE="$( [[ "$group" == cli ]] && printf clippy-cli || printf clippy )" refuse bash scripts/release/qualify-validation-plan.sh "$group" "$group-v1.0.0" auto
    refuse perl scripts/release/validation-plan.pl unknown "$head" "$group-v1.0.0" "target/$group/unknown.json" auto
    refuse perl scripts/release/validation-plan.pl "$group" "$base" "$group-v1.0.0" "target/$group/stale.json" auto
    refuse perl scripts/release/validation-plan.pl "$group" "$head" other-v1.0.0 "target/$group/wrong-baseline.json" auto
    printf 'changed lock\n' >> Cargo.lock
    git commit -qam 'shared lock change'
    perl scripts/release/validation-plan.pl "$group" "$(git rev-parse HEAD)" "$group-v1.0.0" "target/$group/shared.json" auto
    jq -e '.scope=="workspace" and ([.stages[].required] | all)' "target/$group/shared.json" > /dev/null
done
for database_change in added deleted dependency tooling fixture; do
    git checkout -q -B database-expansion "$base"
    case "$database_change" in
      added) printf 'fn main() {}\n' > crates/icydb-core/src/new_target.rs ;;
      deleted) rm crates/icydb-core/src/lib.rs ;;
      dependency) printf '\n[features]\nnew_contract=[]\n' >> crates/icydb-core/Cargo.toml ;;
      tooling) printf '// tooling\n' > scripts/new-tool.rs ;;
      fixture) mkdir -p qualification/build; printf '// artifact owner\n' > qualification/build/new.rs ;;
    esac
    printf '// product change\n' >> crates/icydb/src/lib.rs
    git add .; git commit -qm "database $database_change scope uncertainty"
    perl scripts/release/validation-plan.pl database "$(git rev-parse HEAD)" database-v1.0.0 "target/db-$database_change.json" auto
    jq -e '.scope=="workspace" and ([.stages[].required]|all)' "target/db-$database_change.json" >/dev/null
    cases=$((cases+1))
done
# Shared's unavailable observation must remain a consumer refusal, never an
# empty diff eligible for narrow validation.
head="$(git rev-parse HEAD)"
refuse perl scripts/release/validation-plan.pl cli "$head" cli-v9.9.9 target/missing-base.json auto
git tag cli-v1.0.1
refuse perl scripts/release/validation-plan.pl cli "$head" cli-v1.0.1 target/lightweight-base.json auto
unrelated="$(git commit-tree "$(git rev-parse 'HEAD^{tree}')" -m 'unrelated baseline')"
git tag -am unrelated cli-v1.0.2 "$unrelated"
refuse perl scripts/release/validation-plan.pl cli "$head" cli-v1.0.2 target/nonancestor-base.json auto
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
successful_receipts="$(find target/release-validation -name receipt.json | wc -l | tr -d ' ')"
DRIFT_STAGE=clippy-cli refuse bash scripts/release/qualify-validation-plan.sh cli cli-v1.0.0 auto
git restore crates/icydb-cli/src/main.rs
INTERRUPT_STAGE=clippy-cli refuse bash scripts/release/qualify-validation-plan.sh cli cli-v1.0.0 auto
# No failed/interrupted attempt may leave a successful receipt.
[[ "$(find target/release-validation -name receipt.json | wc -l | tr -d ' ')" == "$successful_receipts" ]]
bash scripts/release/qualify-validation-plan.sh cli cli-v1.0.0 auto > "$fixture/retry.log"
receipt="$(sed -n '/\/receipt.json$/p' "$fixture/retry.log")"
attempt="${receipt%/receipt.json}"
run=("$(jq -r '.evidence.run' "$receipt")")
export RELEASE_GROUP=cli RELEASE_SOURCE
RELEASE_SOURCE="$(git rev-parse HEAD)"
cp "${run[0]}/plan-events.tsv" target/events.original
sed $'/clippy-cli\tPASS/d' target/events.original > "${run[0]}/plan-events.tsv"
refuse perl scripts/release/validation-receipt.pl "$attempt/plan.json" "${run[0]}" "$attempt/evidence.json"
cp target/events.original "${run[0]}/plan-events.tsv"
perl scripts/release/validation-receipt.pl "$attempt/plan.json" "${run[0]}" "$attempt/evidence.json" > target/readmitted.json
cp "${run[0]}/timings.tsv" target/timings.original
sed $'s/clippy-cli\tPASS/clippy-cli\tFAIL/' target/timings.original > "${run[0]}/timings.tsv"
refuse perl scripts/release/validation-receipt.pl "$attempt/plan.json" "${run[0]}" "$attempt/evidence.json"
cp target/timings.original "${run[0]}/timings.tsv"
printf 'unknown\tPASS\tunknown.log\n' >> "${run[0]}/plan-events.tsv"
refuse perl scripts/release/validation-receipt.pl "$attempt/plan.json" "${run[0]}" "$attempt/evidence.json"
cp target/events.original "${run[0]}/plan-events.tsv"
# Qualify the consumer's saved-proof pointer and Shared atomic publication:
# changed raw logs cannot become a new successful baseline or replace a receipt.
# shellcheck source=scripts/ci/release-receipts.sh
source scripts/ci/release-receipts.sh
cp "${run[0]}/0.log" target/log.original
cp "$receipt" target/receipt.original
printf 'changed retained execution log\n' >> "${run[0]}/0.log"
write_receipt() {
    release_receipt_write "$receipt" perl scripts/release/validation-receipt.pl \
        "$attempt/plan.json" "${run[0]}" "$attempt/evidence.json"
}
refuse write_receipt
cmp "$receipt" target/receipt.original
cp target/log.original "${run[0]}/0.log"
# Invalid text cannot replace the original consumer receipt. Restoring the
# exact saved bytes must permit byte-identical readmission from this UTF-8 root.
printf '\377' >> "${run[0]}/plan-events.tsv"
refuse write_receipt
cmp "$receipt" target/receipt.original
cp target/events.original "${run[0]}/plan-events.tsv"
perl scripts/release/validation-receipt.pl "$attempt/plan.json" "${run[0]}" "$attempt/evidence.json" > target/utf8-readmitted.json
cmp "$receipt" target/utf8-readmitted.json
RUSTFLAGS='--cfg changed_receipt_environment' refuse perl scripts/release/validation-receipt.pl "$attempt/plan.json" "${run[0]}" "$attempt/evidence.json"
# A new tag object on the same commit also changes the admitted baseline.
baseline_object="$(jq -r .baseline_tag_object "$attempt/plan.json.context.json")"
git tag -fam replaced cli-v1.0.0 "$base" > /dev/null
refuse perl scripts/release/validation-receipt.pl "$attempt/plan.json" "${run[0]}" "$attempt/evidence.json"
git update-ref refs/tags/cli-v1.0.0 "$baseline_object"
# Moving the baseline invalidates receipt admission.
git tag -fam moved cli-v1.0.0 HEAD > /dev/null
refuse perl scripts/release/validation-receipt.pl "$attempt/plan.json" "${run[0]}" "$attempt/evidence.json"
printf '// moved source\n' >> crates/icydb-cli/src/main.rs
git commit -qam 'moved source'
refuse perl scripts/release/validation-receipt.pl "$attempt/plan.json" "${run[0]}" "$attempt/evidence.json"
printf 'IcyDB selection, exact dispatch, full expansion, required/excluded failures, drift, interruption and fresh recovery passed (%s rejection/expansion cases). Stage bodies were substitutes.\n' "$cases"

#!/usr/bin/env bash
# GitHub expressions in jq mutation programs must remain literal.
# shellcheck disable=SC2016
set -euo pipefail

# Actual parser/policies over copied inputs: no workflows or Git effects run.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
export PATH="$ROOT/.tools/host/bin:$PATH"
export YQ="${YQ:-$ROOT/.tools/host/bin/yq}"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-workflow-policy.XXXXXX")"
fixture_complete=false
finish() {
  local status=$?
  [[ "$fixture_complete" == true || "$status" != 0 ]] || status=1
  if [[ "$status" == 0 ]]; then rm -rf "$fixture"
  else echo "Workflow fixture failure retained: $fixture" >&2; fi
  exit "$status"
}
trap finish EXIT
mkdir -p "$fixture/.github/workflows" "$fixture/scripts/ci" "$fixture/testing/integration/src"
cp "$ROOT/Makefile" "$fixture/"
cp "$ROOT/.github/CODEOWNERS" "$fixture/.github/"
cp "$ROOT/scripts/ci/"{check-ci-workflow-invariants.sh,ci-workflow-invariants.jq,dependency-pins.jq,run-icydb-validation-targets.sh} "$fixture/scripts/ci/"
cp "$ROOT/testing/integration/src/lib.rs" "$fixture/testing/integration/src/"
core_subject="$fixture/.github/workflows/ci.yml"
adapter_subject="$fixture/.github/workflows/canic-adapter.yml"
subject="$core_subject"
count=0

reset() {
  cp "$ROOT/.github/workflows/ci.yml" "$core_subject"
  cp "$ROOT/.github/workflows/canic-adapter.yml" "$adapter_subject"
}
check() {
  local expected="$1" name="$2" status=0
  bash "$fixture/scripts/ci/check-ci-workflow-invariants.sh" > "$fixture/$name.log" 2>&1 || status=$?
  # Bash 3.2 can continue after a failed assertion in an if branch. A policy
  # mismatch must terminate explicitly before it can count as a completed case.
  if [[ "$expected" == pass && "$status" != 0 || "$expected" != pass && "$status" == 0 ]]; then
    printf 'Workflow case %s expected %s, observed status %s\n' "$name" "$expected" "$status" >&2
    cat "$fixture/$name.log" >&2
    exit 1
  fi
  count=$((count + 1))
}
mutate() { "$YQ" -i "$1" "$subject"; }

reset
check pass baseline
status=0
( check fail unexpected-baseline-acceptance ) > "$fixture/assertion-control.log" 2>&1 || status=$?
[[ "$status" == 1 ]]
reset
mutate 'del(.jobs.dependency_msrv.env.RUSTUP_TOOLCHAIN)'
check fail missing-msrv-selection
reset
mutate '.jobs.dependency_msrv.env.RUSTUP_TOOLCHAIN = "${{ env.RUST_CURRENT }}"'
check fail wrong-msrv-selection
reset
mutate '(.jobs.dependency_msrv.steps[] | select(.with.toolchain == "${{ env.RUSTUP_TOOLCHAIN }}")).with.toolchain = "${{ env.RUST_CURRENT }}"'
check fail wrong-msrv-installation
reset
mutate '(.jobs.dependency_msrv.steps[] | select(.with.toolchain == "${{ env.RUSTUP_TOOLCHAIN }}")) |= del(.with.targets)'
check fail missing-msrv-wasm-target
reset
mutate '(.jobs.dependency_msrv.steps[] | select(.run == "bash scripts/ci/check-public-msrv.sh")).env.RUSTUP_TOOLCHAIN = "${{ env.RUST_CURRENT }}"'
check fail overridden-msrv-compiler
reset
mutate '.jobs.dependency_msrv.steps |= map(select(.run != "bash scripts/ci/check-public-msrv.sh")) | .jobs.static.steps += [{"run": "bash scripts/ci/check-public-msrv.sh"}]'
check fail msrv-gate-wrong-job
reset
mutate '(.jobs.dependency_msrv.steps[] | select(.run == "bash scripts/ci/check-public-msrv.sh")).continue-on-error = true'
check fail ignored-msrv-failure
reset
mutate '(.jobs.dependency_msrv.steps[] | select(.run == "bash scripts/ci/check-public-msrv.sh")).if = "github.event_name == '\''pull_request'\''"'
check fail conditional-msrv-gate
reset
mutate '.jobs.dependency_msrv.steps |= map(select(.run != "cargo fetch --locked"))'
check fail missing-msrv-cache-preparation
reset
mutate '.jobs.dependency_msrv.steps |= (map(select(.run != "cargo fetch --locked")) + [.[] | select(.run == "cargo fetch --locked")])'
check fail late-msrv-cache-preparation
reset
mutate '(.jobs.macos_host.steps[] | select(has("run")) | .run) |= sub("make check-portable-automation"; "make help")'
check fail missing-native-portable-gate
reset
perl -pi -e 's/\$\(MAKE\) --no-print-directory check-portable-automation/\$\(MAKE\) --no-print-directory help/' "$fixture/Makefile"
check fail missing-static-portable-gate
cp "$ROOT/Makefile" "$fixture/Makefile"
reset
mutate '(.jobs.static.steps[] | select(has("run")) | .run) |= sub("/\\.tools/rust/common/bin"; "/missing/bin")'
check fail missing-static-rust-tool-path
reset
mutate '(.jobs.macos_host.steps[] | select(has("run")) | .run) |= sub("/\\.tools/rust/common/bin"; "/missing/bin")'
check fail missing-native-rust-tool-path
reset
mutate '.env.RUSTC_WRAPPER = "sccache"'
check fail unprepared-inherited-wrapper
reset
mutate '.jobs.rust.steps |= map(select((.uses // "" | test("^mozilla-actions/sccache-action@")) | not))'
check fail unprepared-job-wrapper
reset
mutate '.jobs.rust.steps |= map(select(.run != "rustup target add wasm32-unknown-unknown"))'
check fail missing-workspace-wasm-target
reset
mutate '(.jobs.rust.steps[] | select(.run == "rustup target add wasm32-unknown-unknown")).if = "matrix.lane == '\''tier-b'\''"'
check fail wasm-target-only-in-tier-b
reset
mutate '(.jobs.rust.steps[] | select(.run == "rustup target add wasm32-unknown-unknown")).continue-on-error = true'
check fail ignored-workspace-wasm-target-failure
reset
mutate '.jobs.rust.steps |= (map(select(.run != "rustup target add wasm32-unknown-unknown")) + [.[] | select(.run == "rustup target add wasm32-unknown-unknown")])'
check fail late-workspace-wasm-target
reset
mutate '(.jobs.rust.steps[] | select(.env.MAKE_TARGET == "${{ matrix.make_target }}")) |= del(.env.MAKE_TARGET)'
check fail missing-matrix-make-target
reset
mutate '.jobs.rust.steps |= map(select(.run != "cargo fetch --locked"))'
check fail missing-locked-cache-preparation
reset
mutate '(.jobs.rust.steps[] | select(.run == "cargo fetch --locked")).run = "cargo fetch"'
check fail unlocked-cache-preparation
reset
mutate '.jobs.rust.steps |= (map(select(.run != "cargo fetch --locked")) + [.[] | select(.run == "cargo fetch --locked")])'
check fail cache-preparation-after-offline-check
reset
mutate '(.jobs.rust.steps[] | select(.run == "cargo fetch --locked")).if = "github.event_name == '\''pull_request'\''"'
check fail conditional-cache-preparation
reset
mutate '(.jobs.rust.steps[] | select(.run == "cargo fetch --locked")).continue-on-error = true'
check fail ignored-cache-preparation-failure
reset
mutate '(.jobs.rust.steps[] | select(.run == "cargo fetch --locked")) as $fetch | .jobs.rust.steps |= map(select(.run != "cargo fetch --locked")) | .jobs.static.steps += [$fetch]'
check fail cache-preparation-wrong-job
subject="$adapter_subject"
reset
mutate '.jobs.static.steps |= map(select(.name != "Prepare locked Testkit CLI for substitute-server fixtures"))'
check fail missing-static-cli-preparation
reset
mutate '(.jobs.static.steps[] | select(.name == "Prepare locked Testkit CLI for substitute-server fixtures") | .run) |= sub("bash scripts/ci/testkit-runner.sh --check"; "true")'
check fail missing-static-cli-admission
subject="$core_subject"
reset
mutate '.jobs.static.steps += [{"run": "make install-testkit testkit-check"}]'
check fail static-official-server-prerequisite
reset
mutate '(.jobs.static.steps[] | select(.name == "Install pinned local tools") | .run) |= sub("LOCAL_TOOL_INSTALL_TARGETS="; "")'
check fail static-server-extension
reset
mutate '(.jobs.rust.steps[] | select(.name == "Install pinned local tools") | .run) |= sub("make tools-check LOCAL_TOOL_CHECK_TARGETS="; "true")'
check fail incomplete-common-admission
reset
mutate '.jobs.rust.steps |= map(select(.uses != "./.github/actions/retain-failure-evidence"))'
check fail missing-tooling-failure-retention
reset
mutate 'del(.env.IC_TOOLS)'
check fail missing-workstation-ic-selection
reset
mutate '.jobs.macos_host.env.IC_TOOLS = "wasm-opt"'
check fail incomplete-workstation-ic-selection
subject="$adapter_subject"
for job in rust wire_browser wasm_size_report; do
  reset
  mutate "del(.jobs.$job.env.IC_TOOLS)"
  check fail "missing-$job-ic-selection"
  reset
  mutate ".jobs.$job.env.IC_TOOLS = \"quill\""
  check fail "wrong-$job-ic-selection"
done
for job in static rust macos_host wasm_size_report; do
  reset
  mutate "(.jobs.$job.steps[] | select(.uses == \"./.github/actions/retain-failure-evidence\")) |= del(.with.\"ic-tools\")"
  check fail "missing-$job-evidence-ic-selection"
done
reset
mutate '(.jobs.wasm_size_report.steps[] | select(.name == "Install pinned local tools") | .run) |= sub("make install-ic-tools"; "true")'
check fail missing-wasm-ic-preparation
reset
mutate '(.jobs.wasm_size_report.steps[] | select(.name == "Install pinned local tools") | .run) |= sub("make ic-tools-check"; "true")'
check fail missing-wasm-ic-admission
reset
mutate '.jobs.rust.steps |= map(select(.name != "Prepare governed IC tools for canister builds"))'
check fail missing-build-ic-preparation
reset
mutate '(.jobs.rust.steps[] | select(.name == "Prepare governed IC tools for canister builds")).if = "matrix.lane == '\''workspace'\''"'
check fail ic-preparation-missing-tier-b
reset
mutate '(.jobs.rust.steps[] | select(.name == "Prepare governed IC tools for canister builds")).if = "matrix.lane == '\''tier-b'\''"'
check fail ic-preparation-missing-workspace
reset
mutate '(.jobs.rust.steps[] | select(.name == "Prepare governed IC tools for canister builds")).continue-on-error = true'
check fail ignored-build-ic-preparation
reset
mutate '(.jobs.rust.steps[] | select(.name == "Prepare governed IC tools for canister builds") | .run) |= sub("make ic-tools-check"; "true")'
check fail missing-build-ic-admission
reset
mutate '.jobs.rust.steps |= (map(select(.name != "Prepare governed IC tools for canister builds")) + [.[] | select(.name == "Prepare governed IC tools for canister builds")])'
check fail late-build-ic-preparation
reset
mutate '.jobs.rust.steps |= map(select(.name != "Prepare locked Testkit CLI and server for live tests"))'
check fail missing-live-server-preparation
reset
mutate '(.jobs.rust.steps[] | select(.name == "Prepare locked Testkit CLI and server for live tests")) |= del(.if)'
check fail unconditional-live-server-preparation
reset
mutate '(.jobs.rust.steps[] | select(.name == "Prepare locked Testkit CLI and server for live tests")).if = "matrix.lane == '\''tier-b'\''"'
check fail missing-workspace-server-preparation
reset
mutate '(.jobs.rust.steps[] | select(.name == "Prepare locked Testkit CLI and server for live tests")).if = "matrix.lane == '\''tier-a'\''"'
check fail server-preparation-wrong-lane
reset
mutate '(.jobs.rust.strategy.matrix.include[] | select(.lane == "tier-a")).make_target = "ci-sql-tier-b" | (.jobs.rust.strategy.matrix.include[] | select(.lane == "tier-b")).make_target = "ci-adapter-sql-tier-a"'
check fail live-target-wrong-lane
reset
mutate '(.jobs.rust.steps[] | select(.name == "Prepare locked Testkit CLI and server for live tests")).continue-on-error = true'
check fail ignored-server-preparation-failure
reset
mutate '.jobs.macos_host.steps |= map(select((.run // "" | contains("make install-dev")) | not))'
check fail missing-native-server-preparation
reset
perl -pi -e 's/^(\t\+)\$\(WORKSPACE_TEST_ENV\) (.*cargo test)/$1\$(IC_TESTKIT_ENV) \$(WORKSPACE_TEST_ENV) $2/' "$fixture/Makefile"
check fail native-test-server-prerequisite
cp "$ROOT/Makefile" "$fixture/Makefile"
reset
# Comments cannot override the parsed value of an individual checkout input.
perl -pi -e 'if (!$done && s/persist-credentials: false/persist-credentials: true # persist-credentials: false/) { $done=1 }' "$subject"
check fail credentials-comment
reset
mutate '.jobs.check.needs = ["wire_browser", "rust", "static", "macos_host"]'
check pass reordered-needs
reset
mutate 'del(.jobs.wire_browser)'
check fail missing-browser-lane
reset
mutate '.jobs.check.needs = ["rust", "static", "macos_host"]'
check fail missing-browser-result-dependency
subject="$core_subject"
reset
# Serialize the same workflow as quoted JSON, a valid YAML representation.
"$YQ" -o json '.' "$subject" > "$fixture/quoted"
mv "$fixture/quoted" "$subject"
check pass quoted-workflow
reset
mutate '.jobs.check.needs style="flow" | .jobs.check.needs style="" | .jobs.static.steps[-1].run style="folded"'
check pass block-needs-folded-run
reset
mutate '.jobs.rust.steps[0].with.persist-credentials = true | .jobs.static.steps[0].with.persist-credentials = false'
check fail credentials-wrong-checkout
reset
mutate '.jobs.rust.steps[0].with.persist-credentials = "false"'
check pass quoted-false
reset
mutate '.jobs.rust.steps[0].uses = "actions/checkout@main"'
check fail shared-action-pin
reset
mutate '.jobs.rust.uses = "owner/repo/.github/workflows/build.yml@main"'
check fail shared-reusable-workflow-pin
subject="$adapter_subject"
reset
mutate '.jobs.wasm_size_report.needs = "check"'
check fail dependent-wasm-evidence
reset
mutate '.jobs.release.needs = ["wasm_size_report", "check"]'
check fail release-prerequisite
subject="$core_subject"
# Artifact tests must remain in the composed graph and include both SQL and
# fleet assertions; checking an ignored-only subset drops the SQL obligation.
reset
perl -0pi -e 's/(test-canister-artifact-contract:\n\t\+)[^\n]*/${1}\$(CARGO_WORK_ENV) cargo test --locked -p icydb-testing-build/' "$fixture/Makefile"
check fail artifact-contract-product-graph
cp "$ROOT/Makefile" "$fixture/Makefile"
reset
sed 's/-- --include-ignored --nocapture/-- --ignored --nocapture/' "$fixture/Makefile" > "$fixture/mutated.make"
mv "$fixture/mutated.make" "$fixture/Makefile"
check fail artifact-contract-omits-sql-checks
cp "$ROOT/Makefile" "$fixture/Makefile"
# Current providers are credited only by these unfiltered selections. Mutate
# actual Make inputs: target existence or a similarly named command is insufficient.
for selection in \
  'empty-native-oracle-admitted|bash scripts/ci/run-nonempty-cargo-test.sh|cargo test' \
  'absent-workspace-target|^_ci-workspace-tests:$|_unselected-tests:' \
  'echoed-only-provider|cargo test --locked --no-fail-fast|echo cargo test --locked --no-fail-fast' \
  'missing-cargo-provider|cargo test --locked --no-fail-fast|:' \
  'unrouted-workspace-provider|^[[:space:]]*_ci-workspace-tests|_ci-workspace-clippy' \
  'unrouted-canister-tests|^[[:space:]]*_test-canister-libs|_ci-workspace-clippy' \
  'unrouted-artifact-contract|^[[:space:]]*test-canister-artifact-contract|_ci-workspace-clippy' \
  'unrouted-durability-tests|^[[:space:]]*_test-durability-integration$|_ci-workspace-clippy' \
  'unrouted-tier-b-provider|^[[:space:]]*_ci-tier-b-sql-perf$|_ci-tier-b-sql-canister' \
  'excluded-provider|--workspace --all-targets --exclude icydb-testing-integration --verbose|--workspace --all-targets --exclude icydb-core --exclude icydb-testing-integration --verbose' \
  'filtered-workspace|--workspace --all-targets --exclude icydb-testing-integration --verbose|--workspace --all-targets --exclude icydb-testing-integration provider_filter --verbose' \
  'missing-tier-b-provider|--test sql_canister --verbose|--test recovery_closeout --verbose' \
  'ignored-tier-b-provider|--test sql_canister --verbose|--test sql_canister --verbose -- --ignored' \
  'skipped-tier-b-provider|--test sql_perf_audit --verbose -- --nocapture|--test sql_perf_audit --verbose -- --skip required_provider' \
  'unexpected-durability-target|--test schema_guard|--test query_canister' \
  'duplicate-durability-target|--test schema_guard|--test sql_guard' \
  'filtered-durability-tests|--test schema_guard|--test schema_guard recovery_filter' \
  'ignored-durability-tests|--test schema_guard|--test schema_guard -- --ignored' \
  'compiled-only-durability-tests|--test schema_guard|--test schema_guard --no-run' \
  'compiled-only-provider|cargo test --locked --no-fail-fast|cargo check --locked'; do
  reset
  cp "$ROOT/Makefile" "$fixture/Makefile"
  IFS='|' read -r name before after <<< "$selection"
  sed "s|$before|$after|" "$fixture/Makefile" > "$fixture/mutated.make"
  mv "$fixture/mutated.make" "$fixture/Makefile"
  check fail "$name"
done
# Every boundary family must remain selected, not merely one live binary.
for binary in convergence_candidate durable_mutation_job_scale recovery_closeout logical_memory \
  schema_migration_closeout lifecycle_participant read_authority sql_guard schema_guard; do
  cp "$ROOT/Makefile" "$fixture/Makefile"
  sed "s/--test $binary//" "$fixture/Makefile" > "$fixture/mutated.make"
  mv "$fixture/mutated.make" "$fixture/Makefile"
  check fail "missing-durability-$binary"
done
cp "$ROOT/Makefile" "$fixture/Makefile"
reset
# Verbosity and ordinary libtest display controls do not change execution coverage.
sed 's/--test sql_canister --verbose/--test sql_canister -- --nocapture/' "$fixture/Makefile" > "$fixture/mutated.make"
mv "$fixture/mutated.make" "$fixture/Makefile"
check pass unfiltered-display-control
cp "$ROOT/Makefile" "$fixture/Makefile"

subject="$core_subject"
for command in 'cargo fetch --locked --manifest-path testing/canic-adapter/Cargo.toml' 'make install-testkit' 'make test-wire-browser'; do
  reset
  COMMAND="$command" "$YQ" -i '.jobs.rust.steps += [{"run": strenv(COMMAND)}]' "$subject"
  check fail product-dispatches-composition
done
subject="$adapter_subject"
reset
mutate '.jobs.rust.steps += [{"if": "matrix.lane == '\''tier-a'\''", "run": "make install-testkit"}]'
check fail native-adapter-comparison-server-prerequisite
reset
mutate '(.jobs.rust.steps[] | select(.run == "rustup target add wasm32-unknown-unknown")).continue-on-error = true'
check fail ignored-adapter-wasm-preparation
reset
mutate 'del(.on.push.paths)'
check fail unscoped-adapter-trigger
reset
mutate '.on.pull_request.paths += ["crates/**"]'
check fail adapter-blocks-unrelated-core
subject="$core_subject"
reset
mutate '.jobs.rust.strategy.fail-fast = true'
check fail matrix-fail-fast
reset
mutate '.jobs.static.runs-on = ["self-hosted", "ubuntu-latest"]'
check fail mutable-runner
reset
mutate '.on.push.tags = ["v*"]'
check fail duplicate-tag-trigger
reset
mutate 'del(.permissions) | .jobs.static.permissions = {"contents": "read"}'
check fail permissions-wrong-owner
reset
mutate '.jobs.rust.steps += [{"run": "gh api repos/owner/repo"}]'
check fail gh-prerequisite-wrong-job
reset
mutate '.jobs.rust.steps += [{"run": "make install-gh"}, {"run": "gh api repos/owner/repo"}]'
check pass gh-prerequisite-ordered
reset
printf '\n---\nname: second document\n' >> "$subject"
check fail multiple-documents
reset
printf '\njobs: [\n' >> "$subject"
check fail malformed-yaml
reset
mv "$subject" "$fixture/hidden.yml"
check fail missing-discovery
printf '[OK] workflow policy fixtures passed: %s cases\n' "$count"
fixture_complete=true

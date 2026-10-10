#!/usr/bin/env bash
# GitHub expressions in jq mutation programs must remain literal.
# shellcheck disable=SC2016
set -euo pipefail

# Actual parser/policies over copied inputs: no workflows or Git effects run.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
export PATH="$ROOT/.tools/host/bin:$PATH"
export YQ="${YQ:-$ROOT/.tools/host/bin/yq}"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-workflow-policy.XXXXXX")"
finish() {
  local status=$?
  if [[ "$status" == 0 ]]; then rm -rf "$fixture"
  else echo "Workflow fixture failure retained: $fixture" >&2; fi
}
trap finish EXIT
mkdir -p "$fixture/.github/workflows" "$fixture/scripts/ci" "$fixture/testing/integration/src"
cp "$ROOT/Makefile" "$fixture/"
cp "$ROOT/.github/CODEOWNERS" "$fixture/.github/"
cp "$ROOT/scripts/ci/"{check-ci-workflow-invariants.sh,ci-workflow-invariants.jq,dependency-pins.jq,run-icydb-validation-targets.sh} "$fixture/scripts/ci/"
cp "$ROOT/testing/integration/src/lib.rs" "$fixture/testing/integration/src/"
subject="$fixture/.github/workflows/ci.yml"
count=0

reset() { cp "$ROOT/.github/workflows/ci.yml" "$subject"; }
check() {
  local expected="$1" name="$2" status=0
  bash "$fixture/scripts/ci/check-ci-workflow-invariants.sh" > "$fixture/$name.log" 2>&1 || status=$?
  if [[ "$expected" == pass ]]; then [[ "$status" == 0 ]]
  else [[ "$status" != 0 ]]; fi
  count=$((count + 1))
}
mutate() { "$YQ" -i "$1" "$subject"; }

reset
check pass baseline
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
mutate '.jobs.dependency_msrv.steps |= map(select(.run != "make fetch"))'
check fail missing-msrv-cache-preparation
reset
mutate '.jobs.dependency_msrv.steps |= (map(select(.run != "make fetch")) + [{"run": "make fetch"}])'
check fail late-msrv-cache-preparation
reset
mutate '(.jobs.macos_host.steps[] | select(has("run")) | .run) |= sub("make check-portable-automation"; "make help")'
check fail missing-native-portable-gate
reset
perl -pi -e 's/\$\(MAKE\) --no-print-directory check-portable-automation/\$\(MAKE\) --no-print-directory help/' "$fixture/Makefile"
check fail missing-static-portable-gate
cp "$ROOT/Makefile" "$fixture/Makefile"
reset
mutate '(.jobs.static.steps[] | select(has("run")) | .run) |= sub("/\\.tools/rust/bin"; "/missing/bin")'
check fail missing-static-rust-tool-path
reset
mutate '(.jobs.macos_host.steps[] | select(has("run")) | .run) |= sub("/\\.tools/rust/bin"; "/missing/bin")'
check fail missing-native-rust-tool-path
reset
mutate '.env.RUSTC_WRAPPER = "sccache"'
check fail unprepared-inherited-wrapper
reset
mutate '.jobs.rust.steps |= map(select((.uses // "" | test("^mozilla-actions/sccache-action@")) | not))'
check fail unprepared-job-wrapper
reset
mutate '.jobs.rust.steps |= map(select(.run != "make fetch"))'
check fail missing-locked-cache-preparation
reset
mutate '.jobs.rust.steps |= (map(select(.run != "make fetch")) + [{"run": "make fetch"}])'
check fail cache-preparation-after-offline-check
reset
mutate '(.jobs.rust.steps[] | select(.run == "make fetch")).if = "github.event_name == '\''pull_request'\''"'
check fail conditional-cache-preparation
reset
mutate '(.jobs.rust.steps[] | select(.run == "make fetch")).continue-on-error = true'
check fail ignored-cache-preparation-failure
reset
mutate '.jobs.rust.steps |= map(select(.run != "make fetch")) | .jobs.static.steps += [{"run": "make fetch"}]'
check fail cache-preparation-wrong-job
reset
mutate '.jobs.static.steps |= map(select(.name != "Prepare locked Testkit CLI for substitute-server fixtures"))'
check fail missing-static-cli-preparation
reset
mutate '(.jobs.static.steps[] | select(.name == "Prepare locked Testkit CLI for substitute-server fixtures") | .run) |= sub("bash scripts/ci/testkit-runner.sh --check"; "true")'
check fail missing-static-cli-admission
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
mutate '.jobs.rust.steps |= map(select(.name != "Prepare locked Testkit CLI and server for live tests"))'
check fail missing-live-server-preparation
reset
mutate '(.jobs.rust.steps[] | select(.name == "Prepare locked Testkit CLI and server for live tests")) |= del(.if)'
check fail unconditional-live-server-preparation
reset
mutate '(.jobs.rust.steps[] | select(.name == "Prepare locked Testkit CLI and server for live tests")).if = "matrix.lane == '\''tier-a'\''"'
check fail server-preparation-wrong-lane
reset
mutate '(.jobs.rust.strategy.matrix.include[] | select(.lane == "tier-a")).make_target = "ci-sql-tier-b" | (.jobs.rust.strategy.matrix.include[] | select(.lane == "tier-b")).make_target = "ci-sql-tier-a"'
check fail live-target-wrong-lane
reset
mutate '(.jobs.rust.steps[] | select(.name == "Prepare locked Testkit CLI and server for live tests")).continue-on-error = true'
check fail ignored-server-preparation-failure
reset
mutate '.jobs.macos_host.steps |= map(select(.run != "make install-dev"))'
check fail missing-native-server-preparation
reset
perl -pi -e 's/^\t\$\(WORKSPACE_TEST_ENV\) (.*cargo test)/\t\$\(IC_TESTKIT_ENV\) \$\(WORKSPACE_TEST_ENV\) $1/' "$fixture/Makefile"
check fail native-test-server-prerequisite
cp "$ROOT/Makefile" "$fixture/Makefile"
reset
# Comments cannot override the parsed value of an individual checkout input.
perl -pi -e 'if (!$done && s/persist-credentials: false/persist-credentials: true # persist-credentials: false/) { $done=1 }' "$subject"
check fail credentials-comment
reset
mutate '.jobs.check.needs = ["rust", "static", "macos_host"]'
check pass reordered-needs
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
reset
mutate '.jobs.wasm_size_report.needs = "check"'
check fail dependent-wasm-evidence
reset
mutate '.jobs.release.needs = ["wasm_size_report", "check"]'
check fail release-prerequisite
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

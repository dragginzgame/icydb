#!/usr/bin/env bash
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
mutate '.env.RUSTC_WRAPPER = "sccache"'
check fail unprepared-inherited-wrapper
reset
mutate '.jobs.rust.steps |= map(select((.uses // "" | test("^mozilla-actions/sccache-action@")) | not))'
check fail unprepared-job-wrapper
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
mutate 'del(.jobs.rust.timeout-minutes) | .jobs.static.steps[0].timeout-minutes = 35'
check fail timeout-wrong-owner
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

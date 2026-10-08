#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
export PATH="$ROOT/.tools/host/bin:$PATH"
YQ="${YQ:-$ROOT/.tools/host/bin/yq}"

status=0

fail() {
  echo "[ERROR] $1" >&2
  status=1
}

# YAML structure is parsed once; shell recipe policy reads decoded run values.
ci_job_runs() {
  jq -r --arg job "$1" '.jobs[$job].steps[]?.run // empty' <<< "$ci_json"
}

make_target_recipe() {
  local target="$1"
  awk -v target="$target" '
    $0 == target ":" {
      in_target = 1
      next
    }
    in_target && /^[^[:space:]#][^=]*:/ {
      exit
    }
    in_target {
      print
    }
  ' Makefile
}

shopt -s nullglob
workflow_files=(.github/workflows/*.yml .github/workflows/*.yaml)
if [[ -z "${workflow_files[*]:-}" ]]; then
  echo "[ERROR] no GitHub Actions workflows found" >&2
  exit 1
fi

ci_json=""
for workflow in "${workflow_files[@]}"; do
  parsed="$("$YQ" -p yaml -o json -I 0 '.' "$workflow")" || {
    echo "[ERROR] cannot parse workflow: $workflow" >&2; exit 1;
  }
  jq -se 'length == 1 and (.[0] | type == "object")' <<< "$parsed" >/dev/null || {
    echo "[ERROR] expected one workflow mapping: $workflow" >&2; exit 1;
  }
  findings="$(jq -r -L "$ROOT/scripts/ci" --arg file "$workflow" \
    -f "$ROOT/scripts/ci/ci-workflow-invariants.jq" <<< "$parsed")" || {
    echo "[ERROR] cannot validate workflow: $workflow" >&2; exit 1;
  }
  if [[ -n "$findings" ]]; then
    fail "$workflow: $findings"
  fi
  if [[ "$workflow" == .github/workflows/ci.yml ]]; then ci_json="$parsed"; fi
done
[[ -n "$ci_json" ]] || { echo '[ERROR] central CI workflow is missing' >&2; exit 1; }

# Cargo observes inherited wrappers even during formatter installation. Each
# job selecting sccache must provision it; static jobs use Cargo directly.
unprepared_wrapper_jobs="$(jq -r '
  . as $workflow | .jobs | to_entries[]
  | select((.value.env.RUSTC_WRAPPER // $workflow.env.RUSTC_WRAPPER // "") == "sccache")
  | select(any(.value.steps[]?; (.uses // "" | startswith("mozilla-actions/sccache-action@"))) | not)
  | .key
' <<< "$ci_json")"
if [[ -n "$unprepared_wrapper_jobs" ]]; then
  fail "jobs select sccache without provisioning it: $unprepared_wrapper_jobs"
fi

for target in ci-static ci-core ci-workspace ci-sql-tier-a ci-sql-tier-b; do
  if ! rg -q "^${target}:$" Makefile; then
    fail "Make is missing the shared $target validation authority"
  fi
done

if ! ci_job_runs static | rg -q '(^|[[:space:]])make[[:space:]]+ci-static([[:space:]]|$)' ||
   ! ci_job_runs rust | rg -q --fixed-strings 'make "$MAKE_TARGET"'; then
  fail "CI jobs must consume the shared local validation targets"
fi

if ! ci_job_runs macos_host | rg -q '(^|[[:space:]])make[[:space:]]+check-portable-automation([[:space:]]|$)' ||
   ! make_target_recipe check-invariants | rg -q --fixed-strings \
     '$(MAKE) --no-print-directory check-portable-automation'; then
  fail "static and native CI must consume the same portable automation gate"
fi

if ! rg -q '^CORE_TEST_ENV := RUST_TEST_THREADS=8$' Makefile ||
   ! make_target_recipe _test-core-no-default |
     rg -q --fixed-strings '$(CORE_TEST_ENV)' ||
   ! make_target_recipe _ci-core-no-default-test |
     rg -q --fixed-strings '$(CORE_TEST_ENV)' ||
   ! rg -q '^WORKSPACE_TEST_ENV := RUST_TEST_THREADS=2$' Makefile ||
   ! make_target_recipe _test-workspace |
     rg -q --fixed-strings '$(WORKSPACE_TEST_ENV)' ||
   ! make_target_recipe _ci-workspace-tests |
     rg -q --fixed-strings '$(WORKSPACE_TEST_ENV)'; then
  fail "local release and CI core/workspace tests must retain bounded libtest concurrency"
fi

for target in _test-canister-libs test-integration-feedback \
  _test-durability-integration test-sql-canister-matrix \
  _ci-tier-a-integration ci-sql-tier-b; do
  if ! make_target_recipe "$target" |
    rg -q --fixed-strings '$(WORKSPACE_TEST_ENV)'; then
    fail "$target must share the bounded workspace/integration test concurrency"
  fi
done

if ! ci_job_runs rust | rg -q --fixed-strings 'make install-tools tools-check' ||
   ! rg -q --fixed-strings 'scripts/ci/run-with-pocketic-server.sh' Makefile ||
   ! rg -q --fixed-strings 'PocketIcStartupConfig::from_env(' testing/integration/src/lib.rs; then
  fail "PocketIC workflows must install one locked binary and Tier B must use one governed server"
fi

tier_b_perf_target_refs="$(rg -c --fixed-strings '_ci-tier-b-sql-perf' Makefile || true)"
if [[ "$tier_b_perf_target_refs" -lt 3 ]] ||
   ! rg -q '^_ci-tier-b-sql-perf:$' Makefile; then
  fail "Tier B must retain the total-only SQL performance gate"
fi

for job in static rust macos_host wasm_size_report; do
  if ! ci_job_runs "$job" | rg -q --fixed-strings '.tools/rust/bin'; then
    fail "$job must expose the shared Rust tool directory to later CI steps"
  fi
done

# Shared bytes are verified by their manifest; consumer regressions qualify
# execution and log retention instead of freezing the runner's diagnostic prose.
if [[ ! -x scripts/ci/run-icydb-validation-targets.sh ]] ||
   ! rg -q 'VALIDATION_RUNNER := .*run-icydb-validation-targets.sh' Makefile ||
   ! make_target_recipe check-portable-automation | rg -q --fixed-strings \
     'bash scripts/ci/verify-shared-tooling-snapshot.sh' ||
   ! make_target_recipe check-portable-automation | rg -q --fixed-strings \
     'bash scripts/ci/test-shared-tooling-adapters.sh' ||
   ! rg -q '^validate-fast:$' Makefile ||
   ! rg -q '^test-integration-feedback:$' Makefile ||
   ! rg -q '^test-durability:$' Makefile; then
  fail "the shared runner and focused consumer validation gates are incomplete"
fi

for owned_path in \
  '/.github/workflows/' \
  '/.github/dependabot.yml' \
  '/scripts/ci/' \
  '/Makefile' \
  '/rust-toolchain.toml'
do
  if ! rg -q --fixed-strings "$owned_path @dragginzgame" .github/CODEOWNERS; then
    fail "CODEOWNERS is missing CI authority $owned_path"
  fi
done

if ! rg -q '^install-gh:$' Makefile ||
   ! rg -q 'bash scripts/ci/install-gh\.sh' Makefile ||
   ! ci_job_runs static | rg -q '(^|[[:space:]])make[[:space:]]+install-gh([[:space:]]|$)'; then
  fail "the shared GitHub CLI installation path is incomplete"
fi

if [[ $status -ne 0 ]]; then
  exit "$status"
fi

echo "CI workflow invariants passed"

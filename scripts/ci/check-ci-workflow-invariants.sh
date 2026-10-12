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
    $0 ~ ("^" target ":[[:space:]]*") {
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

# Qualify the semantic Cargo selection credited by the SQL provider registry.
# Reject filters/skip/ignored controls rather than accepting a target name alone.
# This intentionally admits the maintained unfiltered recipes, without executing
# Make, Cargo, or interpolating its environment-variable adapters.
ci_provider_test_selection() {
  local target="$1" package="$2" binary="$3" recipe word value index=0
  local cargo_test=false workspace=false all_targets=false excluded=false
  local selected_package=false selected_binary=false locked=false libtest=false
  local words=()
  recipe="$(make_target_recipe "$target" | awk '/^\t/ { print }')"
  recipe="${recipe//$'\\\n'/ }"
  [[ "$recipe" =~ [^[:space:]] ]] || return 1
  read -r -a words <<< "$recipe"
  while [[ "$index" -lt "${#words[@]}" ]]; do
    word="${words[$index]}"; index=$((index + 1))
    if [[ "$cargo_test" == false ]]; then
      # Match literal Make adapters without executing their contents.
      # shellcheck disable=SC2016
      case "$word" in
        '$(CARGO_WORK_ENV)'|'+$(CARGO_WORK_ENV)'|'$(WORKSPACE_TEST_ENV)'|'+$(WORKSPACE_TEST_ENV)') continue ;;
        cargo|+cargo) ;;
        *) return 1 ;;
      esac
      [[ "$index" -lt "${#words[@]}" && "${words[$index]}" == test ]] || return 1
      index=$((index + 1)); cargo_test=true; continue
    fi
    if [[ "$libtest" == true ]]; then
      [[ "$word" == --nocapture ]] || return 1
      continue
    fi
    case "$word" in
      --locked) locked=true ;;
      --verbose|-v|--no-fail-fast) ;;
      --workspace) workspace=true ;;
      --all-targets) all_targets=true ;;
      --) libtest=true ;;
      --exclude|-p|--package|--test)
        [[ "$index" -lt "${#words[@]}" ]] || return 1
        value="${words[$index]}"; index=$((index + 1))
        case "$word" in
          --exclude) [[ "$package" == workspace && "$value" == icydb-testing-integration ]] || return 1; excluded=true ;;
          -p|--package) [[ "$package" != workspace && "$value" == "$package" ]] || return 1; selected_package=true ;;
          --test) [[ "$package" != workspace && "$value" == "$binary" ]] || return 1; selected_binary=true ;;
        esac
        ;;
      *) return 1 ;;
    esac
  done
  [[ "$cargo_test" == true && "$locked" == true ]] || return 1
  if [[ "$package" == workspace ]]; then
    [[ "$workspace" == true && "$all_targets" == true && "$excluded" == true ]]
  else
    [[ "$workspace" == false && "$all_targets" == false && "$selected_package" == true && "$selected_binary" == true ]]
  fi
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

for route in 'ci-workspace _ci-workspace-tests' \
  'ci-sql-tier-b _ci-tier-b-sql-canister' 'ci-sql-tier-b _ci-tier-b-sql-perf'; do
  read -r parent child <<< "$route"
  if ! make_target_recipe "$parent" | awk '/^\t/ { print }' |
    rg -q "(^|[[:space:]])$child([[:space:]]|$)"; then
    fail "$parent must execute its credited provider selection $child"
  fi
done

if ! ci_provider_test_selection _ci-workspace-tests workspace '' ||
   ! ci_provider_test_selection _ci-tier-b-sql-canister icydb-testing-integration sql_canister ||
   ! ci_provider_test_selection _ci-tier-b-sql-perf icydb-testing-integration sql_perf_audit; then
  fail "SQL coverage providers require unfiltered workspace and Tier B Cargo test selections"
fi

# Native oracle samples must reject an empty libtest match rather than making
# their compile step stand in for a comparison. Reuse the common executed-test guard.
for target in _ci-tier-a-sqlite _ci-tier-a-mutation; do
  if ! make_target_recipe "$target" | rg -q --fixed-strings 'bash scripts/ci/run-nonempty-cargo-test.sh'; then
    fail "$target must require a nonempty native oracle execution"
  fi
done

for target in _ci-workspace-tests _ci-tier-a-integration; do
  if make_target_recipe "$target" | rg -q --fixed-strings '$(IC_TESTKIT_ENV)'; then
    fail "$target must run native tests without a server prerequisite"
  fi
done

if ! make_target_recipe ci-sql-tier-b | rg -q --fixed-strings '$(POCKET_IC_RUNNER)' ||
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
  if ! ci_job_runs "$job" | rg -q --fixed-strings '.tools/rust/common/bin'; then
    fail "$job must expose the shared Rust tool directory to later CI steps"
  fi
done

# Shared bytes are verified by their manifest; consumer regressions qualify
# execution and log retention instead of freezing the runner's diagnostic prose.
if [[ ! -x scripts/ci/run-icydb-validation-targets.sh ]] ||
   ! rg -q 'VALIDATION_RUNNER := .*run-icydb-validation-targets.sh' Makefile ||
   ! rg -q '^check-portable-automation: shared-tooling-check$' Makefile ||
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

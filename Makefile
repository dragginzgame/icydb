.PHONY: help version tags patch minor major package publish release-prepare release-clean release-stage release-commit release-push \
        release-patch release-minor release-major release \
        test test-unit test-integration-feedback test-durability \
        test-canister-artifact-contract test-sql-canister-matrix \
        test-sql-tier-c-shard test-sql-tier-c-merge \
        test-sql-tier-c-replay \
        build-sql-perf-wasm build-canister-local build-canister-production \
        test-sql-introspection-baseline \
        test-sql-perf-p1-shard test-sql-perf-p1-merge \
        test-sql-perf-scale-shard test-sql-perf-p2-shard test-sql-perf-p2-merge \
        test-sql-perf-instrumentation test-sql-perf-calibration-review test-sql-perf-baseline \
        test-sql-perf-baseline-contract \
        build check clippy fmt fmt-check validate validate-fast clean install install-dev update-dev install-gh install-hooks \
        fetch test-watch all ensure-clean security-check check-versioning \
        test-no-default-smoke \
        wasm-size-report wasm-audit-report \
        lint-workflows shellcheck check-invariants check-feature-matrix \
        ci-static ci-core ci-workspace ci-sql-tier-a ci-sql-tier-b \
        _test-icydb-no-default _test-core-no-default _test-workspace _test-canister-libs \
        _test-durability-core-commit _test-durability-core-mutation-job _test-durability-integration \
        _ci-format _ci-core-no-default-check _ci-core-no-default-test \
        _ci-core-sql-check _ci-core-sql-clippy \
        _ci-core-diagnostics-check _ci-core-diagnostics-clippy \
        _ci-workspace-clippy _ci-workspace-integration-clippy _ci-workspace-tests \
        _ci-tier-a-sqlite _ci-tier-a-mutation _ci-tier-a-integration \
        print-cargo-home print-cargo-target-dir

# Resolve the repo root from this Makefile so scripts can query these values
# via `make -C "$$ROOT"` and share a single source of truth.
ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))

# Cargo runs integration-test binaries from the owning package directory. Resolve
# caller-supplied artifact paths here so relative CLI/workflow inputs retain their
# repository-root meaning after Cargo crosses that working-directory boundary.
workspace_path = $(if $(strip $(1)),$(if $(filter /%,$(strip $(1))),$(strip $(1)),$(abspath $(ROOT_DIR)/$(strip $(1)))))

# Keep workspace cargo state repo-local so sibling repos compiling on the same
# filesystem do not contend on a shared cargo home or target directory.
CARGO_WORK_HOME := $(ROOT_DIR)/.cache/cargo/icydb
CARGO_WORK_TARGET_DIR := $(ROOT_DIR)/target/icydb
RELEASE_TMP_DIR := $(ROOT_DIR)/.cache/release-tmp
CARGO_WORK_ENV := CARGO_HOME="$(CARGO_WORK_HOME)" CARGO_TARGET_DIR="$(CARGO_WORK_TARGET_DIR)"
CARGO_PUBLISH_ENV := CARGO_TARGET_DIR="$(CARGO_WORK_TARGET_DIR)"
IC_TESTKIT_ENV := TMPDIR="$(ROOT_DIR)/.cache"
PERF_POCKET_IC_ENV := POCKET_IC_BIN="$(POCKET_IC_BIN)"
VALIDATION_RUNNER := bash "$(ROOT_DIR)/scripts/ci/run-validation-targets.sh"
ACTIONLINT_VERSION ?= 1.7.12
ACTIONLINT_INSTALL_DIR ?= $(HOME)/.local/bin
ACTIONLINT_BIN ?= $(ACTIONLINT_INSTALL_DIR)/actionlint
P1_SHARD_DIR ?= $(ROOT_DIR)/artifacts/perf-audit/sql_perf_p1_shards
P1_REPORT_OUT ?= $(ROOT_DIR)/artifacts/perf-audit/sql_perf_deterministic_matrix
P1_BASELINE_PATH ?=
PERF_CALIBRATION_COHORT ?=
PERF_CALIBRATION_RUN ?=
PERF_CALIBRATION_RUN_1_DIR ?=
PERF_CALIBRATION_RUN_2_DIR ?=
PERF_CALIBRATION_RUN_3_DIR ?=
PERF_CALIBRATION_REVIEW_PATH ?= $(ROOT_DIR)/artifacts/perf-audit/sql_perf_calibration_review.json
PERF_BASELINE_DIR ?=
P2_SELECTION_PATH ?= $(ROOT_DIR)/artifacts/perf-audit/sql_perf_p2_candidates.json
P2_SHARD_DIR ?= $(ROOT_DIR)/artifacts/perf-audit/sql_perf_p2_shards
P2_REPORT_PATH ?= $(ROOT_DIR)/artifacts/perf-audit/sql_perf_p2_report.json
P2_BASELINE_PATH ?=
P2_CURRENT_PATH ?= $(P2_REPORT_PATH)
PERF_COMPARISON_PATH ?= $(ROOT_DIR)/artifacts/perf-audit/sql_perf_comparison.json
PERF_INSTRUMENTATION_PATH ?= $(ROOT_DIR)/artifacts/perf-audit/sql_perf_instrumentation.json
SQL_PERF_WASM_PATH ?= $(ROOT_DIR)/artifacts/perf-audit/sql_perf_canister.wasm
SCALE_BASELINE_PATH ?=
SCALE_CURRENT_PATH ?= $(SCALE_REPORT_PATH)
SCALE_SHARD_DIR ?= $(ROOT_DIR)/artifacts/perf-audit/sql_perf_scale_shards
SCALE_REPORT_PATH ?= $(ROOT_DIR)/artifacts/perf-audit/sql_perf_scale_report.json
TIER_C_ARTIFACT_DIR ?= $(ROOT_DIR)/artifacts/correctness/sql_tier_c
TIER_C_FAILURE_ARTIFACT ?=

# Print repo-local cargo paths for standalone shell scripts that need Makefile-
# owned defaults without duplicating the path definitions.
print-cargo-home:
	@printf '%s\n' "$(CARGO_WORK_HOME)"

print-cargo-target-dir:
	@printf '%s\n' "$(CARGO_WORK_TARGET_DIR)"

# Check for clean git state
ensure-clean:
	@if ! git diff --quiet --ignore-submodules HEAD --; then \
		echo "🚨 Working directory not clean! Please commit or stash your changes."; \
		exit 1; \
	fi

# Default target
help:
	@echo "Available commands:"
	@echo ""
	@echo "Setup / Installation:"
	@echo "  install          Install the local icydb CLI binary"
	@echo "  install-dev      Install developer dependencies, GitHub CLI, actionlint, and the formatting hook"
	@echo "  update-dev       Update developer tooling and ensure GitHub CLI and the formatting hook are installed"
	@echo "  install-gh       Ensure the GitHub CLI is installed"
	@echo "  install-hooks    Configure the formatting-only pre-commit hook"
	@echo ""
	@echo "Version Management:"
	@echo "  version          Show current version"
	@echo "  tags             List available git tags"
	@echo "  patch            Test a source candidate, then bump patch version files (0.0.x)"
	@echo "  minor            Confirm and test a source candidate, then bump minor files (0.x.0)"
	@echo "  major            Confirm and test a source candidate, then bump major files (x.0.0)"
	@echo "  release-clean    Remove repo-local release build and temporary artifacts"
	@echo "  release-stage    Stage known release files"
	@echo "  release-commit   Verify the tested candidate transition, commit, and tag"
	@echo "  release-push     Atomically push the release commit and exact tag, then clean artifacts"
	@echo "  release-patch    Human-owned one-shot patch release"
	@echo "  release-minor    Confirm, bump, stage, commit, tag, and push a minor release"
	@echo "  release-major    Confirm, bump, stage, commit, tag, and push a major release"
	@echo "  release          CI-driven release (local target is no-op)"
	@echo "  package          Build publishable crate tarballs"
	@echo "  publish          Publish crates; reuse the exact release-commit receipt when available"
	@echo ""
	@echo "Development:"
	@echo "  test             Run all tests; lets ic-testkit download pinned PocketIC when uncached"
	@echo "  test-integration-feedback TEST_TARGET=... TEST_NAME=..."
	@echo "                  Run one exact integration test, then its complete binary"
	@echo "  test-durability  Run the focused commit, mutation-job, convergence, recovery, and perf matrix"
	@echo "  test-canister-artifact-contract"
	@echo "                  Build and inspect all 32 independent production/local canister artifacts"
	@echo "  test-sql-canister-matrix"
	@echo "                  Run the live generated SQL canister endpoint matrix"
	@echo "  test-sql-tier-c-shard TIER_C_SHARD=0"
	@echo "                  Run one exact native Tier C correctness shard (0 through 7)"
	@echo "  test-sql-tier-c-merge"
	@echo "                  Merge all eight Tier C receipts and publish typed coverage"
	@echo "  test-sql-tier-c-replay TIER_C_FAILURE_ARTIFACT=..."
	@echo "                  Reproduce one minimized Tier C failure exactly"
	@echo "  build-sql-perf-wasm"
	@echo "                  Build the one wasm-release subject shared by every performance shard"
	@echo "  test-sql-introspection-baseline"
	@echo "                  Measure the exact 0.224 introspection instruction and Candid contract"
	@echo "  build-canister-local CANISTER=demo_rpg"
	@echo "                  Build and stage the exact local/test feature profile"
	@echo "  build-canister-production CANISTER=demo_rpg"
	@echo "                  Build and stage the exact production feature profile"
	@echo "  test-sql-perf-p1-shard P1_SHARD=0"
	@echo "                  Run one deterministic P1 performance shard (0 through 7)"
	@echo "  test-sql-perf-p1-merge P1_BASELINE_PATH=..."
	@echo "                  Merge strict P1/scale shards against one reviewed P1 baseline"
	@echo "  test-sql-perf-p1-merge PERF_CALIBRATION_COHORT=... PERF_CALIBRATION_RUN=1"
	@echo "                  Produce one explicit clean run in a three-run initial calibration"
	@echo "  test-sql-perf-scale-shard SCALE_SHARD=0"
	@echo "                  Run one deterministic scale shard (0 through 7)"
	@echo "  test-sql-perf-p2-shard P2_SHARD=0"
	@echo "                  Run one deterministic P2 confirmation shard (0 through 7)"
	@echo "  test-sql-perf-p2-merge"
	@echo "                  Merge all eight strict P2 shard artifacts"
	@echo "  test-sql-perf-instrumentation"
	@echo "                  Capture attributed versus total-only sentinel overhead"
	@echo "  test-sql-perf-calibration-review PERF_CALIBRATION_RUN_1_DIR=... PERF_CALIBRATION_RUN_2_DIR=... PERF_CALIBRATION_RUN_3_DIR=..."
	@echo "                  Validate and review one exact three-run initial calibration cohort"
	@echo "  test-sql-perf-baseline P2_BASELINE_PATH=..."
	@echo "                  Compare reviewed P2 and scale baselines"
	@echo "  test-sql-perf-baseline-contract PERF_BASELINE_DIR=..."
	@echo "                  Validate one reviewed Tier D bundle against current artifact contracts"
	@echo "  build            Build all crates"
	@echo "  check            Run cargo check"
	@echo "  clippy           Run clippy checks"
	@echo "  validate         Run formatting, invariants, feature checks, clippy, and tests"
	@echo "  validate-fast    Run the quick formatting, automation, invariant, and workspace-check preflight"
	@echo "  fetch            Fetch locked dependencies into the repo-local Cargo cache"
	@echo "  fmt              Format code"
	@echo "  fmt-check        Check formatting"
	@echo "  clean            Clean build artifacts"
	@echo "  wasm-size-report Build and report wasm sizes for default_empty + one/ten entity audit canisters"
	@echo "  wasm-audit-report Build wasm + write Twiggy audit reports for default_empty + one/ten entity audit canisters"
	@echo "  wasm-query-attribution Build symbol-bearing typed/dynamic/SQL query attribution artifacts"
	@echo "  lint-workflows   Lint GitHub Actions workflows with pinned actionlint"
	@echo "  shellcheck       Lint repository shell automation"
	@echo ""
	@echo "Utilities:"
	@echo "  test-watch       Run tests in watch mode"
	@echo "  all              Run all checks, tests, and build"
	@echo "  security-check   Verify GitHub Protected Tags (informational)"
	@echo ""
	@echo "Examples:"
	@echo "  make patch       # Bump patch version"
	@echo "  make test        # Run tests"
	@echo "  make build       # Build project"
	@echo "  make wasm-size-report SIZE_REPORT_ARGS=\"--sql-variants both\""
	@echo "  make wasm-size-report SIZE_REPORT_ARGS=\"--canister ten_entity_typed_query\""

#
# Installing
#

# Install the developer CLI as `icydb` into cargo's normal bin directory.
install:
	cargo install --path "$(ROOT_DIR)/crates/icydb-cli" --bin icydb --locked --force

# Install local developer prerequisites, tools, and the formatting hook.
install-dev:
	ACTIONLINT_VERSION="$(ACTIONLINT_VERSION)" ACTIONLINT_INSTALL_DIR="$(ACTIONLINT_INSTALL_DIR)" scripts/dev/workstation-setup.sh install

# Update user-local Rust/Cargo/actionlint/ICP developer tooling and the hook.
update-dev:
	ACTIONLINT_VERSION="$(ACTIONLINT_VERSION)" ACTIONLINT_INSTALL_DIR="$(ACTIONLINT_INSTALL_DIR)" scripts/dev/workstation-setup.sh update

# Keep one idempotent GitHub CLI installer for workstation setup and CI jobs.
install-gh:
	bash scripts/ci/install-gh.sh

# Keep hook installation explicit and refuse to replace an unrelated local
# hook authority. Git resolves the relative path from this repository.
install-hooks:
	@set -e; \
	git -C "$(ROOT_DIR)" rev-parse --git-dir >/dev/null; \
	current="$$(git -C "$(ROOT_DIR)" config --local --get core.hooksPath || true)"; \
	if [ -n "$$current" ] && [ "$$current" != ".githooks" ]; then \
		echo "Refusing to replace existing core.hooksPath: $$current" >&2; \
		exit 1; \
	fi; \
	git -C "$(ROOT_DIR)" config --local core.hooksPath .githooks; \
	chmod +x "$(ROOT_DIR)/.githooks/pre-commit"; \
	echo "Formatting-only pre-commit hook installed (.githooks/pre-commit)."

#
# Version management (the source candidate is gated before any version mutation)
#

version:
	@$(CARGO_WORK_ENV) cargo get workspace.package.version

tags:
	@git tag --sort=-version:refname | head -10

patch:
	@$(MAKE) --no-print-directory release-prepare
	@set -e; \
	candidate_commit="$$(git rev-parse --verify HEAD)"; \
	scripts/ci/release-candidate-receipt.sh verify-tested-tree "$$candidate_commit"; \
	TMPDIR="$(RELEASE_TMP_DIR)" $(MAKE) --no-print-directory validate; \
	scripts/ci/release-candidate-receipt.sh verify-tested-tree "$$candidate_commit"; \
	TMPDIR="$(RELEASE_TMP_DIR)" $(CARGO_WORK_ENV) scripts/ci/bump-version.sh patch; \
	RELEASE_RECEIPT_DIR="$(ROOT_DIR)/.cache/release-receipts" \
		scripts/ci/release-candidate-receipt.sh record patch "$$candidate_commit"

minor:
	@$(CARGO_WORK_ENV) scripts/ci/confirm-version-bump.sh minor
	@$(MAKE) --no-print-directory release-prepare
	@set -e; \
	candidate_commit="$$(git rev-parse --verify HEAD)"; \
	scripts/ci/release-candidate-receipt.sh verify-tested-tree "$$candidate_commit"; \
	TMPDIR="$(RELEASE_TMP_DIR)" $(MAKE) --no-print-directory validate; \
	scripts/ci/release-candidate-receipt.sh verify-tested-tree "$$candidate_commit"; \
	TMPDIR="$(RELEASE_TMP_DIR)" $(CARGO_WORK_ENV) scripts/ci/bump-version.sh minor; \
	RELEASE_RECEIPT_DIR="$(ROOT_DIR)/.cache/release-receipts" \
		scripts/ci/release-candidate-receipt.sh record minor "$$candidate_commit"

major:
	@$(CARGO_WORK_ENV) scripts/ci/confirm-version-bump.sh major
	@$(MAKE) --no-print-directory release-prepare
	@set -e; \
	candidate_commit="$$(git rev-parse --verify HEAD)"; \
	scripts/ci/release-candidate-receipt.sh verify-tested-tree "$$candidate_commit"; \
	TMPDIR="$(RELEASE_TMP_DIR)" $(MAKE) --no-print-directory validate; \
	scripts/ci/release-candidate-receipt.sh verify-tested-tree "$$candidate_commit"; \
	TMPDIR="$(RELEASE_TMP_DIR)" $(CARGO_WORK_ENV) scripts/ci/bump-version.sh major; \
	RELEASE_RECEIPT_DIR="$(ROOT_DIR)/.cache/release-receipts" \
		scripts/ci/release-candidate-receipt.sh record major "$$candidate_commit"

release-prepare:
	@mkdir -p "$(RELEASE_TMP_DIR)"

release-clean:
	@bash scripts/ci/cleanup-release-workspace.sh

release: ensure-clean
	@echo "Release artifacts are handled by CI on the main release commit"

package: ensure-clean
	$(CARGO_WORK_ENV) cargo package

publish: ensure-clean
	$(CARGO_PUBLISH_ENV) scripts/ci/publish-workspace.sh

release-stage:
	git add Cargo.toml Cargo.lock README.md scripts/ci/sync-release-surface-version.sh $$(git ls-files -m -- '*/Cargo.toml' CHANGELOG.md 'docs/changelog/*.md' || true)

release-commit:
	@version="$$( $(CARGO_WORK_ENV) cargo get workspace.package.version )"; \
	if git rev-parse "v$$version" >/dev/null 2>&1; then \
		echo "❌ Tag v$$version already exists. Aborting." >&2; \
		exit 1; \
	fi; \
	RELEASE_RECEIPT_DIR="$(ROOT_DIR)/.cache/release-receipts" \
		scripts/ci/release-candidate-receipt.sh verify-staged; \
	git commit -m "Release $$version"
	@RELEASE_RECEIPT_DIR="$(ROOT_DIR)/.cache/release-receipts" \
		scripts/ci/release-candidate-receipt.sh verify-commit
	@version="$$( $(CARGO_WORK_ENV) cargo get workspace.package.version )"; \
	git tag -a "v$$version" -m "Release $$version"; \
	RELEASE_RECEIPT_DIR="$(ROOT_DIR)/.cache/release-receipts" \
		scripts/ci/record-release-gate-receipt.sh

release-push:
	@set -e; \
	version="$$( $(CARGO_WORK_ENV) cargo get workspace.package.version )"; \
	branch="$$(git symbolic-ref --quiet --short HEAD)"; \
	git push --no-follow-tags --atomic origin \
		"HEAD:refs/heads/$$branch" \
		"refs/tags/v$$version:refs/tags/v$$version"
	@bash scripts/ci/cleanup-release-workspace.sh

release-patch:
	@$(MAKE) --no-print-directory patch
	@$(MAKE) --no-print-directory release-stage
	@$(MAKE) --no-print-directory release-commit
	@$(MAKE) --no-print-directory release-push

release-minor:
	@$(MAKE) --no-print-directory minor
	@$(MAKE) --no-print-directory release-stage
	@$(MAKE) --no-print-directory release-commit
	@$(MAKE) --no-print-directory release-push

release-major:
	@$(MAKE) --no-print-directory major
	@$(MAKE) --no-print-directory release-stage
	@$(MAKE) --no-print-directory release-commit
	@$(MAKE) --no-print-directory release-push

#
# Tests
#

test:
	$(VALIDATION_RUNNER) test-unit test-canister-artifact-contract

test-unit:
	$(VALIDATION_RUNNER) \
		_test-icydb-no-default \
		_test-core-no-default \
		_test-workspace \
		_test-canister-libs

_test-icydb-no-default:
	$(CARGO_WORK_ENV) cargo test --no-fail-fast -p icydb --no-default-features

_test-core-no-default:
	$(CARGO_WORK_ENV) cargo test --no-fail-fast -p icydb-core --no-default-features

_test-workspace:
	$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) cargo test --no-fail-fast --workspace --all-targets --exclude canister_demo_rpg --exclude canister_test_sql --exclude canister_test_sql_bounded

_test-canister-libs:
	$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) cargo test --no-fail-fast -p canister_test_sql -p canister_test_sql_bounded --lib

test-no-default-smoke:
	$(VALIDATION_RUNNER) _test-icydb-no-default _test-core-no-default

test-integration-feedback:
	@test -n "$(TEST_TARGET)" || { echo "TEST_TARGET must name one icydb-testing-integration test binary" >&2; exit 1; }
	@test -n "$(TEST_NAME)" || { echo "TEST_NAME must name one exact test in $(TEST_TARGET)" >&2; exit 1; }
	$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) cargo test --locked -p icydb-testing-integration \
		--test "$(TEST_TARGET)" "$(TEST_NAME)" -- --exact --nocapture
	$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) cargo test --locked --no-fail-fast \
		-p icydb-testing-integration --test "$(TEST_TARGET)"

test-durability:
	$(VALIDATION_RUNNER) \
		_test-durability-core-commit \
		_test-durability-core-mutation-job \
		_test-durability-integration

_test-durability-core-commit:
	$(CARGO_WORK_ENV) cargo test --locked -p icydb-core --lib 'db::commit::'

_test-durability-core-mutation-job:
	$(CARGO_WORK_ENV) cargo test --locked -p icydb-core --lib 'db::mutation_job::'

_test-durability-integration:
	$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) cargo test --locked --no-fail-fast \
		-p icydb-testing-integration \
		--test convergence_candidate \
		--test durable_mutation_job_scale \
		--test recovery_closeout \
		--test sql_perf_audit

test-canister-artifact-contract:
	$(CARGO_WORK_ENV) cargo test --locked -p icydb-testing-integration \
		--test canister_artifact_contract \
		production_and_local_source_declarations_match_the_frozen_endpoint_policy \
		-- --ignored --exact --nocapture

test-sql-canister-matrix:
	$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) cargo test --no-fail-fast -p icydb-testing-integration --test sql_canister -- --nocapture

test-sql-tier-c-shard:
	@test -n "$(TIER_C_SHARD)" || { echo "TIER_C_SHARD must be an index from 0 through 7" >&2; exit 1; }
	@mkdir -p "$(TIER_C_ARTIFACT_DIR)"
	@rm -f "$(TIER_C_ARTIFACT_DIR)/tier-c-shard-$(TIER_C_SHARD).json"
	ICYDB_SQL_TIER_C_SHARD_INDEX="$(TIER_C_SHARD)" \
	ICYDB_SQL_TIER_C_ARTIFACT_DIR="$(TIER_C_ARTIFACT_DIR)" \
	$(CARGO_WORK_ENV) \
	cargo test --locked -p icydb-core --lib --features sql,diagnostics \
		db::session::tests::tier_c_reference::tier_c_native_shard_emits_exact_receipt \
		-- --ignored --exact --nocapture --test-threads=1
	@test -s "$(TIER_C_ARTIFACT_DIR)/tier-c-shard-$(TIER_C_SHARD).json" || { echo "Tier C shard produced no current receipt" >&2; exit 1; }

test-sql-tier-c-merge:
	@rm -f "$(TIER_C_ARTIFACT_DIR)/tier-c-merged.json"
	ICYDB_SQL_TIER_C_ARTIFACT_DIR="$(TIER_C_ARTIFACT_DIR)" \
	$(CARGO_WORK_ENV) \
	cargo test --locked -p icydb-core --lib --features sql,diagnostics \
		db::session::tests::tier_c_reference::tier_c_native_receipts_merge_exactly_and_require_clean_evidence \
		-- --ignored --exact --nocapture --test-threads=1
	@test -s "$(TIER_C_ARTIFACT_DIR)/tier-c-merged.json" || { echo "Tier C merge produced no current receipt" >&2; exit 1; }

test-sql-tier-c-replay:
	@test -n "$(TIER_C_FAILURE_ARTIFACT)" || { echo "TIER_C_FAILURE_ARTIFACT must name one failure.<blake3>.json artifact" >&2; exit 1; }
	ICYDB_SQL_TIER_C_FAILURE_ARTIFACT="$(TIER_C_FAILURE_ARTIFACT)" \
	$(CARGO_WORK_ENV) \
	cargo test --locked -p icydb-core --lib --features sql,diagnostics \
		db::session::tests::tier_c_reference::tier_c_failure_artifact_replays_exact_minimized_failure \
		-- --ignored --exact --nocapture --test-threads=1

build-sql-perf-wasm:
	ICYDB_SQL_PERF_WASM_PATH="$(SQL_PERF_WASM_PATH)" \
	$(CARGO_WORK_ENV) \
	cargo test -p icydb-testing-integration --test sql_perf_matrix_audit \
		sql_perf_builds_shared_wasm_subject -- --ignored --exact --nocapture

test-sql-introspection-baseline:
	@test -n "$(POCKET_IC_BIN)" || { echo "POCKET_IC_BIN must name the exact PocketIC binary used for introspection evidence" >&2; exit 1; }
	@test -s "$(SQL_PERF_WASM_PATH)" || { echo "run make build-sql-perf-wasm before introspection measurement" >&2; exit 1; }
	ICYDB_SQL_PERF_WASM_PATH="$(SQL_PERF_WASM_PATH)" \
	$(PERF_POCKET_IC_ENV) $(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) \
	cargo test --locked -p icydb-testing-integration --test sql_perf_matrix_audit \
		sql_introspection_0_224_contract_is_exactly_measured \
		-- --ignored --exact --nocapture --test-threads=1

build-canister-local:
	@test -n "$(CANISTER)" || { echo "CANISTER must name one maintained canister" >&2; exit 1; }
	$(CARGO_WORK_ENV) cargo run --locked -p icydb-testing-integration \
		--bin build_fixture_canister -- "$(CANISTER)" --build-profile local \
		--profile debug --candid-export on

build-canister-production:
	@test -n "$(CANISTER)" || { echo "CANISTER must name one maintained canister" >&2; exit 1; }
	$(CARGO_WORK_ENV) cargo run --locked -p icydb-testing-integration \
		--bin build_fixture_canister -- "$(CANISTER)" --build-profile production \
		--profile wasm-release --candid-export on

test-sql-perf-p1-shard:
	@test -n "$(P1_SHARD)" || { echo "P1_SHARD must be an index from 0 through 7" >&2; exit 1; }
	@test -n "$(POCKET_IC_BIN)" || { echo "POCKET_IC_BIN must name the exact PocketIC binary used for performance evidence" >&2; exit 1; }
	@test -s "$(SQL_PERF_WASM_PATH)" || { echo "run make build-sql-perf-wasm before SQL performance measurement" >&2; exit 1; }
	ICYDB_SQL_PERF_P1_SHARD_INDEX="$(P1_SHARD)" \
	ICYDB_SQL_PERF_P1_SHARD_DIR="$(P1_SHARD_DIR)" \
	ICYDB_SQL_PERF_WASM_PATH="$(SQL_PERF_WASM_PATH)" \
	$(PERF_POCKET_IC_ENV) $(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) \
	cargo test -p icydb-testing-integration --test sql_perf_matrix_audit \
		sql_perf_p1_shard_reports_hotspots -- --ignored --nocapture

test-sql-perf-p1-merge:
	@if [ -n "$(P1_BASELINE_PATH)" ]; then \
		test -z "$(PERF_CALIBRATION_COHORT)$(PERF_CALIBRATION_RUN)" || { echo "P1 baseline and calibration inputs are mutually exclusive" >&2; exit 1; }; \
	else \
		test -n "$(PERF_CALIBRATION_COHORT)" || { echo "set P1_BASELINE_PATH or both PERF_CALIBRATION_COHORT and PERF_CALIBRATION_RUN" >&2; exit 1; }; \
		test -n "$(PERF_CALIBRATION_RUN)" || { echo "set P1_BASELINE_PATH or both PERF_CALIBRATION_COHORT and PERF_CALIBRATION_RUN" >&2; exit 1; }; \
	fi
	ICYDB_SQL_PERF_P1_SHARD_DIR="$(P1_SHARD_DIR)" \
	ICYDB_SQL_PERF_P1_BASELINE_PATH="$(call workspace_path,$(P1_BASELINE_PATH))" \
	ICYDB_SQL_PERF_CALIBRATION_COHORT="$(PERF_CALIBRATION_COHORT)" \
	ICYDB_SQL_PERF_CALIBRATION_RUN="$(PERF_CALIBRATION_RUN)" \
	ICYDB_SQL_PERF_SCALE_SHARD_DIR="$(SCALE_SHARD_DIR)" \
	ICYDB_SQL_PERF_SCALE_REPORT_PATH="$(SCALE_REPORT_PATH)" \
	ICYDB_SQL_PERF_MATRIX_OUT="$(P1_REPORT_OUT)" \
	ICYDB_SQL_PERF_P2_SELECTION_PATH="$(P2_SELECTION_PATH)" \
	$(CARGO_WORK_ENV) \
	cargo test -p icydb-testing-integration --test sql_perf_matrix_audit \
		sql_perf_p1_merges_saved_shards -- --ignored --nocapture

test-sql-perf-scale-shard:
	@test -n "$(SCALE_SHARD)" || { echo "SCALE_SHARD must be an index from 0 through 7" >&2; exit 1; }
	@test -n "$(POCKET_IC_BIN)" || { echo "POCKET_IC_BIN must name the exact PocketIC binary used for performance evidence" >&2; exit 1; }
	@test -s "$(SQL_PERF_WASM_PATH)" || { echo "run make build-sql-perf-wasm before SQL performance measurement" >&2; exit 1; }
	ICYDB_SQL_PERF_SCALE_SHARD_INDEX="$(SCALE_SHARD)" \
	ICYDB_SQL_PERF_SCALE_SHARD_DIR="$(SCALE_SHARD_DIR)" \
	ICYDB_SQL_PERF_WASM_PATH="$(SQL_PERF_WASM_PATH)" \
	$(PERF_POCKET_IC_ENV) $(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) \
	cargo test -p icydb-testing-integration --test sql_perf_matrix_audit \
		sql_perf_scale_shard_measures_declared_ladders -- --ignored --nocapture

test-sql-perf-p2-shard:
	@test -n "$(P2_SHARD)" || { echo "P2_SHARD must be an index from 0 through 7" >&2; exit 1; }
	@test -n "$(POCKET_IC_BIN)" || { echo "POCKET_IC_BIN must name the exact PocketIC binary used for performance evidence" >&2; exit 1; }
	@test -s "$(SQL_PERF_WASM_PATH)" || { echo "run make build-sql-perf-wasm before SQL performance measurement" >&2; exit 1; }
	ICYDB_SQL_PERF_P2_SHARD_INDEX="$(P2_SHARD)" \
	ICYDB_SQL_PERF_P2_SELECTION_PATH="$(P2_SELECTION_PATH)" \
	ICYDB_SQL_PERF_P2_SHARD_DIR="$(P2_SHARD_DIR)" \
	ICYDB_SQL_PERF_WASM_PATH="$(SQL_PERF_WASM_PATH)" \
	$(PERF_POCKET_IC_ENV) $(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) \
	cargo test -p icydb-testing-integration --test sql_perf_matrix_audit \
		sql_perf_p2_shard_confirms_selected_candidates -- --ignored --nocapture

test-sql-perf-p2-merge:
	ICYDB_SQL_PERF_P2_SELECTION_PATH="$(P2_SELECTION_PATH)" \
	ICYDB_SQL_PERF_P2_SHARD_DIR="$(P2_SHARD_DIR)" \
	ICYDB_SQL_PERF_P2_REPORT_PATH="$(P2_REPORT_PATH)" \
	$(CARGO_WORK_ENV) \
	cargo test -p icydb-testing-integration --test sql_perf_matrix_audit \
		sql_perf_p2_merges_saved_shards -- --ignored --nocapture

test-sql-perf-instrumentation:
	@test -n "$(POCKET_IC_BIN)" || { echo "POCKET_IC_BIN must name the exact PocketIC binary used for performance evidence" >&2; exit 1; }
	@test -s "$(SQL_PERF_WASM_PATH)" || { echo "run make build-sql-perf-wasm before SQL performance measurement" >&2; exit 1; }
	ICYDB_SQL_PERF_INSTRUMENTATION_REPORT_PATH="$(PERF_INSTRUMENTATION_PATH)" \
	ICYDB_SQL_PERF_WASM_PATH="$(SQL_PERF_WASM_PATH)" \
	$(PERF_POCKET_IC_ENV) $(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) \
	cargo test -p icydb-testing-integration --test sql_perf_matrix_audit \
		sql_perf_calibrates_attribution_overhead -- --ignored --nocapture

test-sql-perf-calibration-review:
	@test -d "$(call workspace_path,$(PERF_CALIBRATION_RUN_1_DIR))" || { echo "PERF_CALIBRATION_RUN_1_DIR must name the ordinal-1 evidence bundle" >&2; exit 1; }
	@test -d "$(call workspace_path,$(PERF_CALIBRATION_RUN_2_DIR))" || { echo "PERF_CALIBRATION_RUN_2_DIR must name the ordinal-2 evidence bundle" >&2; exit 1; }
	@test -d "$(call workspace_path,$(PERF_CALIBRATION_RUN_3_DIR))" || { echo "PERF_CALIBRATION_RUN_3_DIR must name the ordinal-3 evidence bundle" >&2; exit 1; }
	ICYDB_SQL_PERF_CALIBRATION_RUN_1_DIR="$(call workspace_path,$(PERF_CALIBRATION_RUN_1_DIR))" \
	ICYDB_SQL_PERF_CALIBRATION_RUN_2_DIR="$(call workspace_path,$(PERF_CALIBRATION_RUN_2_DIR))" \
	ICYDB_SQL_PERF_CALIBRATION_RUN_3_DIR="$(call workspace_path,$(PERF_CALIBRATION_RUN_3_DIR))" \
	ICYDB_SQL_PERF_CALIBRATION_REVIEW_PATH="$(PERF_CALIBRATION_REVIEW_PATH)" \
	$(CARGO_WORK_ENV) \
	cargo test -p icydb-testing-integration --test sql_perf_matrix_audit \
		sql_perf_reviews_initial_calibration_cohort -- --ignored --exact --nocapture

test-sql-perf-baseline:
	@test -n "$(P2_BASELINE_PATH)" || { echo "P2_BASELINE_PATH must name a reviewed merged P2 baseline" >&2; exit 1; }
	@test -n "$(SCALE_BASELINE_PATH)" || { echo "SCALE_BASELINE_PATH must name a reviewed merged scale baseline" >&2; exit 1; }
	ICYDB_SQL_PERF_BASELINE_PATH="$(call workspace_path,$(P2_BASELINE_PATH))" \
	ICYDB_SQL_PERF_CURRENT_PATH="$(call workspace_path,$(P2_CURRENT_PATH))" \
	ICYDB_SQL_PERF_COMPARISON_PATH="$(PERF_COMPARISON_PATH)" \
	ICYDB_SQL_PERF_SCALE_BASELINE_PATH="$(call workspace_path,$(SCALE_BASELINE_PATH))" \
	ICYDB_SQL_PERF_SCALE_CURRENT_PATH="$(call workspace_path,$(SCALE_CURRENT_PATH))" \
	$(CARGO_WORK_ENV) \
	cargo test -p icydb-testing-integration --test sql_perf_matrix_audit \
		sql_perf_compares_saved_baseline -- --ignored --nocapture

test-sql-perf-baseline-contract:
	@test -d "$(call workspace_path,$(PERF_BASELINE_DIR))" || { echo "PERF_BASELINE_DIR must name a reviewed Tier D baseline bundle" >&2; exit 1; }
	ICYDB_SQL_PERF_BASELINE_BUNDLE_DIR="$(call workspace_path,$(PERF_BASELINE_DIR))" \
	$(CARGO_WORK_ENV) \
	cargo test --locked -p icydb-testing-integration --test sql_perf_matrix_audit \
		sql_perf_baseline_bundle_is_current -- --ignored --exact --nocapture

wasm-size-report:
	$(CARGO_WORK_ENV) bash scripts/ci/wasm-size-report.sh $(SIZE_REPORT_ARGS)

wasm-audit-report:
	$(CARGO_WORK_ENV) bash scripts/ci/wasm-audit-report.sh $(AUDIT_REPORT_ARGS)

wasm-query-attribution:
	$(CARGO_WORK_ENV) bash scripts/ci/wasm-query-attribution.sh $(ATTRIBUTION_ARGS)

#
# Development commands
#

fetch:
	$(CARGO_WORK_ENV) cargo fetch --locked

build:
	$(CARGO_WORK_ENV) cargo build --release --workspace

check:
	$(CARGO_WORK_ENV) cargo check --workspace

clippy:
	$(CARGO_WORK_ENV) cargo clippy -p icydb-core --no-default-features --features sql -- -D warnings
	$(CARGO_WORK_ENV) cargo clippy -p icydb-core --no-default-features --features diagnostics -- -D warnings
	$(CARGO_WORK_ENV) cargo clippy --workspace --all-targets -- -D warnings

fmt:
	$(CARGO_WORK_ENV) cargo sort --workspace
	$(CARGO_WORK_ENV) cargo sort-derives
	$(CARGO_WORK_ENV) cargo fmt --all

fmt-check:
	$(CARGO_WORK_ENV) cargo sort --workspace --check
	$(CARGO_WORK_ENV) cargo sort-derives --check
	$(CARGO_WORK_ENV) cargo fmt --all -- --check

validate:
	$(VALIDATION_RUNNER) \
		fmt-check \
		lint-workflows \
		shellcheck \
		check-invariants \
		check-feature-matrix \
		check \
		clippy \
		test

# Fast local/Codex preflight. This intentionally does not replace `validate`:
# feature-specific clippy lanes and executable tests remain in the full gate.
validate-fast:
	$(VALIDATION_RUNNER) \
		fmt-check \
		lint-workflows \
		shellcheck \
		check-invariants \
		check

clean:
	$(CARGO_WORK_ENV) cargo clean


# Security and versioning checks
security-check:
	@echo "Security checks are enforced via GitHub settings:"
	@echo "- Enable Protected Tags for pattern 'v*' (Settings → Tags)"
	@echo "- Restrict who can create tags and disable force pushes"
	@echo "- Require PR + CI on 'main' via branch protection"
	@echo "This target is informational only; no local script runs."

check-versioning: security-check
	@$(CARGO_WORK_ENV) cargo set-version --help >/dev/null
	@$(MAKE) --no-print-directory version >/dev/null
	@$(MAKE) --no-print-directory help >/dev/null
	@echo "Versioning tooling checks passed."

check-invariants:
	bash scripts/ci/check-ci-workflow-invariants.sh
	bash scripts/ci/check-deployment-inventory-invariants.sh
	bash scripts/ci/check-dependency-graph-invariants.sh
	bash scripts/ci/check-executor-no-production-panics.sh
	bash scripts/ci/check-generated-endpoint-invariants.sh
	bash scripts/ci/check-index-range-spec-invariants.sh
	bash scripts/ci/check-layer-authority-invariants.sh
	bash scripts/ci/check-mutation-atomicity-invariants.sh
	bash scripts/ci/check-release-cleanup-invariants.sh
	bash scripts/ci/check-durability-doc-invariants.sh
	bash scripts/ci/check-read-admission-invariants.sh
	bash scripts/ci/check-schema-model-boundary-invariants.sh
	bash scripts/ci/check-sql-branch-ownership-invariants.sh
	bash scripts/ci/check-wasm-post-link-invariants.sh
	bash scripts/ci/check-memory-id-invariants.sh

check-feature-matrix:
	$(CARGO_WORK_ENV) cargo check -p icydb --no-default-features
	$(CARGO_WORK_ENV) cargo check -p icydb-core --no-default-features
	$(CARGO_WORK_ENV) cargo check -p icydb --no-default-features --features sql
	$(CARGO_WORK_ENV) cargo check -p icydb-core --no-default-features --features sql
	$(CARGO_WORK_ENV) cargo check -p icydb --no-default-features --features diagnostics
	$(CARGO_WORK_ENV) cargo check -p icydb-core --no-default-features --features diagnostics
	$(CARGO_WORK_ENV) cargo check --workspace --no-default-features

lint-workflows:
	@if [ ! -x "$(ACTIONLINT_BIN)" ]; then \
		echo "actionlint not found at $(ACTIONLINT_BIN). Run 'make install-dev' first." >&2; \
		exit 1; \
	fi; \
	version="$$("$(ACTIONLINT_BIN)" -version 2>&1 | sed -n '1{s/[[:space:]].*//;p;}')"; \
	if [ "$$version" != "$(ACTIONLINT_VERSION)" ]; then \
		echo "actionlint version $$version found at $(ACTIONLINT_BIN), expected $(ACTIONLINT_VERSION)." >&2; \
		exit 1; \
	fi; \
	"$(ACTIONLINT_BIN)"

shellcheck:
	shellcheck --exclude=SC2001,SC2016 \
		scripts/app/*.sh scripts/ci/*.sh scripts/dev/*.sh .githooks/pre-commit

# GitHub Actions consumes these exact local targets as parallel lanes. The
# terminal `check` job remains the one branch-protection and release gate.
ci-static:
	$(VALIDATION_RUNNER) _ci-format lint-workflows shellcheck check-invariants

_ci-format:
	$(CARGO_WORK_ENV) cargo fmt --all -- --check

ci-core:
	$(VALIDATION_RUNNER) \
		_ci-core-no-default-check \
		_ci-core-no-default-test \
		_ci-core-sql-check \
		_ci-core-sql-clippy \
		_ci-core-diagnostics-check \
		_ci-core-diagnostics-clippy

_ci-core-no-default-check:
	$(CARGO_WORK_ENV) cargo check --locked -p icydb -p icydb-core --no-default-features
	$(CARGO_WORK_ENV) cargo check --locked --workspace --no-default-features

_ci-core-no-default-test:
	$(CARGO_WORK_ENV) cargo test --locked --no-fail-fast \
		-p icydb -p icydb-core --no-default-features

_ci-core-sql-check:
	$(CARGO_WORK_ENV) cargo check --locked \
		-p icydb -p icydb-core --no-default-features --features sql

_ci-core-sql-clippy:
	$(CARGO_WORK_ENV) cargo clippy --locked \
		-p icydb-core --no-default-features --features sql -- -D warnings

_ci-core-diagnostics-check:
	$(CARGO_WORK_ENV) cargo check --locked \
		-p icydb -p icydb-core --no-default-features --features diagnostics

_ci-core-diagnostics-clippy:
	$(CARGO_WORK_ENV) cargo clippy --locked \
		-p icydb-core --no-default-features --features diagnostics -- -D warnings

ci-workspace:
	$(VALIDATION_RUNNER) \
		_ci-workspace-clippy \
		_ci-workspace-integration-clippy \
		_ci-workspace-tests

_ci-workspace-clippy:
	$(CARGO_WORK_ENV) cargo clippy --locked --workspace --all-targets \
		--exclude icydb-testing-integration -- -D warnings

_ci-workspace-integration-clippy:
	$(CARGO_WORK_ENV) cargo clippy --locked -p icydb-testing-integration \
		--test sql_correctness --test sql_canister -- -D warnings

_ci-workspace-tests:
	$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) cargo test --locked --no-fail-fast \
		--workspace --all-targets --exclude icydb-testing-integration --verbose

ci-sql-tier-a:
	$(VALIDATION_RUNNER) \
		_ci-tier-a-sqlite \
		_ci-tier-a-mutation \
		_ci-tier-a-integration

_ci-tier-a-sqlite:
	$(CARGO_WORK_ENV) cargo test --locked -p icydb-core \
		--no-default-features --features sql \
		db::session::tests::sqlite_reference --verbose

_ci-tier-a-mutation:
	$(CARGO_WORK_ENV) cargo test --locked -p icydb-core \
		--no-default-features --features sql \
		db::session::tests::mutation_reference --verbose

_ci-tier-a-integration:
	$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) cargo test --locked --no-fail-fast \
		-p icydb-testing-integration --test sql_correctness --verbose

ci-sql-tier-b:
	$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) cargo test --locked --no-fail-fast \
		-p icydb-testing-integration --test sql_canister --verbose

# Run tests in watch mode
test-watch:
	$(CARGO_WORK_ENV) cargo watch -x test

# Build and test everything through explicit, sequential workflow steps.
all: ensure-clean
	$(MAKE) --no-print-directory clean
	$(MAKE) --no-print-directory validate
	$(MAKE) --no-print-directory build

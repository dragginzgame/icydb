.PHONY: help version tags patch minor major package publish release-stage release-commit release-push \
        release-patch release-minor release-major release \
        test test-bump test-sql-canister-matrix test-sql-tier-c-shard test-sql-tier-c-merge \
        test-sql-tier-c-replay \
        test-sql-perf-p1-shard test-sql-perf-p1-merge \
        test-sql-perf-scale-shard test-sql-perf-p2-shard test-sql-perf-p2-merge \
        test-sql-perf-instrumentation test-sql-perf-baseline \
        build check clippy fmt fmt-check clean install install-dev update-dev \
        fetch test-watch all ensure-clean security-check check-versioning \
        ensure-hooks install-hooks test-no-default-smoke \
        wasm-size-report wasm-audit-report \
        lint-workflows check-invariants check-feature-matrix \
        print-cargo-home print-cargo-target-dir

# Resolve the repo root from this Makefile so scripts can query these values
# via `make -C "$$ROOT"` and share a single source of truth.
ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))

# Keep workspace cargo state repo-local so sibling repos compiling on the same
# filesystem do not contend on a shared cargo home or target directory.
CARGO_WORK_HOME := $(ROOT_DIR)/.cache/cargo/icydb
CARGO_WORK_TARGET_DIR := $(ROOT_DIR)/target/icydb
CARGO_WORK_ENV := CARGO_HOME="$(CARGO_WORK_HOME)" CARGO_TARGET_DIR="$(CARGO_WORK_TARGET_DIR)"
CARGO_PUBLISH_ENV := CARGO_TARGET_DIR="$(CARGO_WORK_TARGET_DIR)"
IC_TESTKIT_ENV := IC_TESTKIT_ALLOW_POCKET_IC_DOWNLOAD=1 TMPDIR="$(ROOT_DIR)/.cache"
ACTIONLINT_VERSION ?= 1.7.12
ACTIONLINT_INSTALL_DIR ?= $(HOME)/.local/bin
ACTIONLINT_BIN ?= $(ACTIONLINT_INSTALL_DIR)/actionlint
P1_SHARD_DIR ?= $(ROOT_DIR)/artifacts/perf-audit/sql_perf_p1_shards
P1_REPORT_OUT ?= $(ROOT_DIR)/artifacts/perf-audit/sql_perf_deterministic_matrix
P1_BASELINE_PATH ?=
PERF_CALIBRATION_COHORT ?=
PERF_CALIBRATION_RUN ?=
P2_SELECTION_PATH ?= $(ROOT_DIR)/artifacts/perf-audit/sql_perf_p2_candidates.json
P2_SHARD_DIR ?= $(ROOT_DIR)/artifacts/perf-audit/sql_perf_p2_shards
P2_REPORT_PATH ?= $(ROOT_DIR)/artifacts/perf-audit/sql_perf_p2_report.json
P2_BASELINE_PATH ?=
P2_CURRENT_PATH ?= $(P2_REPORT_PATH)
PERF_COMPARISON_PATH ?= $(ROOT_DIR)/artifacts/perf-audit/sql_perf_comparison.json
PERF_INSTRUMENTATION_PATH ?= $(ROOT_DIR)/artifacts/perf-audit/sql_perf_instrumentation.json
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
	@echo "  install-dev      Install local developer dependencies, actionlint, and git hooks"
	@echo "  update-dev       Update user-local Rust/Cargo/actionlint/ICP tools"
	@echo "  install-hooks    Configure git hooks"
	@echo ""
	@echo "Version Management:"
	@echo "  version          Show current version"
	@echo "  tags             List available git tags"
	@echo "  patch            Run release gate, then bump patch version files (0.0.x)"
	@echo "  minor            Confirm, run release gate, then bump minor version files (0.x.0)"
	@echo "  major            Confirm, run full release gate, then bump major version files (x.0.0)"
	@echo "  release-stage    Stage known release files"
	@echo "  release-commit   Commit version files and create the release tag"
	@echo "  release-push     Push the release commit and tags"
	@echo "  release-patch    Human-owned one-shot patch release"
	@echo "  release-minor    Confirm, bump, stage, commit, tag, and push a minor release"
	@echo "  release-major    Confirm, bump, stage, commit, tag, and push a major release"
	@echo "  release          CI-driven release (local target is no-op)"
	@echo "  package          Build publishable crate tarballs"
	@echo "  publish          Publish workspace crates to registry in dependency order"
	@echo ""
	@echo "Development:"
	@echo "  test             Run all tests; lets ic-testkit download pinned PocketIC when uncached"
	@echo "  test-sql-canister-matrix"
	@echo "                  Run the live generated SQL canister endpoint matrix"
	@echo "  test-sql-tier-c-shard TIER_C_SHARD=0"
	@echo "                  Run one exact native Tier C correctness shard (0 through 7)"
	@echo "  test-sql-tier-c-merge"
	@echo "                  Merge all eight Tier C receipts and publish typed coverage"
	@echo "  test-sql-tier-c-replay TIER_C_FAILURE_ARTIFACT=..."
	@echo "                  Reproduce one minimized Tier C failure exactly"
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
	@echo "  test-sql-perf-baseline P2_BASELINE_PATH=..."
	@echo "                  Compare reviewed P2 and scale baselines"
	@echo "  build            Build all crates"
	@echo "  check            Run cargo check"
	@echo "  clippy           Run clippy checks"
	@echo "  fetch            Fetch locked dependencies into the repo-local Cargo cache"
	@echo "  fmt              Format code"
	@echo "  fmt-check        Check formatting"
	@echo "  clean            Clean build artifacts"
	@echo "  wasm-size-report Build and report wasm sizes for default_empty + one/ten entity audit canisters"
	@echo "  wasm-audit-report Build wasm + write Twiggy audit reports for default_empty + one/ten entity audit canisters"
	@echo "  lint-workflows   Lint GitHub Actions workflows with pinned actionlint"
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
	@echo "  make wasm-size-report SIZE_REPORT_ARGS=\"--canister ten_entity_fluent_rows\""

#
# Installing
#

# Install the developer CLI as `icydb` into cargo's normal bin directory.
install:
	cargo install --path "$(ROOT_DIR)/crates/icydb-cli" --bin icydb --locked --force

# Install local developer prerequisites, tools, and repository hooks.
install-dev:
	ACTIONLINT_VERSION="$(ACTIONLINT_VERSION)" ACTIONLINT_INSTALL_DIR="$(ACTIONLINT_INSTALL_DIR)" scripts/dev/workstation-setup.sh install

# Update user-local Rust/Cargo/actionlint/ICP developer tooling.
update-dev:
	ACTIONLINT_VERSION="$(ACTIONLINT_VERSION)" ACTIONLINT_INSTALL_DIR="$(ACTIONLINT_INSTALL_DIR)" scripts/dev/workstation-setup.sh update

# Optional explicit install target (idempotent)
install-hooks ensure-hooks:
	@if [ -d .git ]; then \
		git config --local core.hooksPath .githooks || true; \
		chmod +x .githooks/* 2>/dev/null || true; \
		echo "✅ Git hooks configured (core.hooksPath -> .githooks)"; \
	else \
		echo "⚠️  Not a git repo, skipping hooks setup"; \
	fi


#
# Version management (guarded bumps confirm before running the release gate)
#

version:
	@$(CARGO_WORK_ENV) cargo get workspace.package.version

tags:
	@git tag --sort=-version:refname | head -10

patch: ensure-clean fmt test-bump
	@$(CARGO_WORK_ENV) scripts/ci/bump-version.sh patch

minor:
	@$(CARGO_WORK_ENV) scripts/ci/confirm-version-bump.sh minor
	@$(MAKE) ensure-clean
	@$(MAKE) fmt
	@$(MAKE) test-bump
	@$(CARGO_WORK_ENV) scripts/ci/bump-version.sh minor

major:
	@$(CARGO_WORK_ENV) scripts/ci/confirm-version-bump.sh major
	@$(MAKE) ensure-clean
	@$(MAKE) fmt
	@$(MAKE) test
	@$(CARGO_WORK_ENV) scripts/ci/bump-version.sh major

release: ensure-clean
	@echo "Release handled by CI on tag push"

package: ensure-clean
	$(CARGO_WORK_ENV) cargo package

publish: ensure-clean
	$(CARGO_PUBLISH_ENV) scripts/ci/publish-workspace.sh

release-stage:
	git add Cargo.toml Cargo.lock README.md scripts/ci/sync-release-surface-version.sh $$(git ls-files -m -- '*/Cargo.toml' || true)

release-commit:
	@version="$$( $(CARGO_WORK_ENV) cargo get workspace.package.version )"; \
	if git rev-parse "v$$version" >/dev/null 2>&1; then \
		echo "❌ Tag v$$version already exists. Aborting." >&2; \
		exit 1; \
	fi; \
	if git diff --cached --quiet --ignore-submodules HEAD --; then \
		echo "No staged release files; run make release-stage first." >&2; \
		exit 1; \
	fi; \
	git commit -m "Release $$version"; \
	git tag -a "v$$version" -m "Release $$version"

release-push:
	git push --follow-tags

release-patch: patch release-stage release-commit release-push

release-minor: minor release-stage release-commit release-push

release-major: major release-stage release-commit release-push

#
# Tests
#

test: clippy test-unit

test-bump: clippy test-unit

test-unit:
	$(CARGO_WORK_ENV) cargo test -p icydb --no-default-features
	$(CARGO_WORK_ENV) cargo test -p icydb-core --no-default-features
	$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) cargo test --workspace --all-targets --exclude canister_demo_rpg --exclude canister_test_sql --exclude canister_test_sql_bounded
	$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) cargo test -p canister_test_sql --lib
	$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) cargo test -p canister_test_sql_bounded --lib

test-no-default-smoke:
	$(CARGO_WORK_ENV) cargo test -p icydb --no-default-features
	$(CARGO_WORK_ENV) cargo test -p icydb-core --no-default-features

test-sql-canister-matrix:
	IC_TESTKIT_ALLOW_POCKET_IC_DOWNLOAD=1 $(CARGO_WORK_ENV) cargo test -p icydb-testing-integration --test sql_canister --features icydb/sql-explain -- --nocapture

test-sql-tier-c-shard:
	@test -n "$(TIER_C_SHARD)" || { echo "TIER_C_SHARD must be an index from 0 through 7" >&2; exit 1; }
	ICYDB_SQL_TIER_C_SHARD_INDEX="$(TIER_C_SHARD)" \
	ICYDB_SQL_TIER_C_ARTIFACT_DIR="$(TIER_C_ARTIFACT_DIR)" \
	$(CARGO_WORK_ENV) \
	cargo test --locked -p icydb-core --features sql \
		db::session::tests::tier_c_reference::tier_c_native_shard_emits_exact_receipt \
		-- --ignored --exact --nocapture --test-threads=1

test-sql-tier-c-merge:
	ICYDB_SQL_TIER_C_ARTIFACT_DIR="$(TIER_C_ARTIFACT_DIR)" \
	$(CARGO_WORK_ENV) \
	cargo test --locked -p icydb-core --features sql \
		db::session::tests::tier_c_reference::tier_c_native_receipts_merge_exactly_and_require_clean_evidence \
		-- --ignored --exact --nocapture --test-threads=1

test-sql-tier-c-replay:
	@test -n "$(TIER_C_FAILURE_ARTIFACT)" || { echo "TIER_C_FAILURE_ARTIFACT must name one failure.<blake3>.json artifact" >&2; exit 1; }
	ICYDB_SQL_TIER_C_FAILURE_ARTIFACT="$(TIER_C_FAILURE_ARTIFACT)" \
	$(CARGO_WORK_ENV) \
	cargo test --locked -p icydb-core --features sql \
		db::session::tests::tier_c_reference::tier_c_failure_artifact_replays_exact_minimized_failure \
		-- --ignored --exact --nocapture --test-threads=1

test-sql-perf-p1-shard:
	@test -n "$(P1_SHARD)" || { echo "P1_SHARD must be an index from 0 through 7" >&2; exit 1; }
	ICYDB_SQL_PERF_P1_SHARD_INDEX="$(P1_SHARD)" \
	ICYDB_SQL_PERF_P1_SHARD_DIR="$(P1_SHARD_DIR)" \
	$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) \
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
	ICYDB_SQL_PERF_P1_BASELINE_PATH="$(P1_BASELINE_PATH)" \
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
	ICYDB_SQL_PERF_SCALE_SHARD_INDEX="$(SCALE_SHARD)" \
	ICYDB_SQL_PERF_SCALE_SHARD_DIR="$(SCALE_SHARD_DIR)" \
	$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) \
	cargo test -p icydb-testing-integration --test sql_perf_matrix_audit \
		sql_perf_scale_shard_measures_declared_ladders -- --ignored --nocapture

test-sql-perf-p2-shard:
	@test -n "$(P2_SHARD)" || { echo "P2_SHARD must be an index from 0 through 7" >&2; exit 1; }
	ICYDB_SQL_PERF_P2_SHARD_INDEX="$(P2_SHARD)" \
	ICYDB_SQL_PERF_P2_SELECTION_PATH="$(P2_SELECTION_PATH)" \
	ICYDB_SQL_PERF_P2_SHARD_DIR="$(P2_SHARD_DIR)" \
	$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) \
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
	ICYDB_SQL_PERF_INSTRUMENTATION_REPORT_PATH="$(PERF_INSTRUMENTATION_PATH)" \
	$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) \
	cargo test -p icydb-testing-integration --test sql_perf_matrix_audit \
		sql_perf_calibrates_attribution_overhead -- --ignored --nocapture

test-sql-perf-baseline:
	@test -n "$(P2_BASELINE_PATH)" || { echo "P2_BASELINE_PATH must name a reviewed merged P2 baseline" >&2; exit 1; }
	@test -n "$(SCALE_BASELINE_PATH)" || { echo "SCALE_BASELINE_PATH must name a reviewed merged scale baseline" >&2; exit 1; }
	ICYDB_SQL_PERF_BASELINE_PATH="$(P2_BASELINE_PATH)" \
	ICYDB_SQL_PERF_CURRENT_PATH="$(P2_CURRENT_PATH)" \
	ICYDB_SQL_PERF_COMPARISON_PATH="$(PERF_COMPARISON_PATH)" \
	ICYDB_SQL_PERF_SCALE_BASELINE_PATH="$(SCALE_BASELINE_PATH)" \
	ICYDB_SQL_PERF_SCALE_CURRENT_PATH="$(SCALE_CURRENT_PATH)" \
	$(CARGO_WORK_ENV) \
	cargo test -p icydb-testing-integration --test sql_perf_matrix_audit \
		sql_perf_compares_saved_baseline -- --ignored --nocapture

wasm-size-report:
	$(CARGO_WORK_ENV) bash scripts/ci/wasm-size-report.sh $(SIZE_REPORT_ARGS)

wasm-audit-report:
	$(CARGO_WORK_ENV) bash scripts/ci/wasm-audit-report.sh $(AUDIT_REPORT_ARGS)

#
# Development commands
#

fetch:
	$(CARGO_WORK_ENV) cargo fetch --locked

build: ensure-clean ensure-hooks
	$(CARGO_WORK_ENV) cargo build --release --workspace

check: ensure-hooks fmt-check
	$(MAKE) check-invariants
	$(MAKE) check-feature-matrix
	$(CARGO_WORK_ENV) cargo check --workspace

clippy: ensure-hooks
	$(MAKE) check-invariants
	$(MAKE) check-feature-matrix
	$(CARGO_WORK_ENV) cargo clippy -p icydb-core --no-default-features --features sql -- -D warnings
	$(CARGO_WORK_ENV) cargo clippy -p icydb-core --no-default-features --features diagnostics -- -D warnings
	$(CARGO_WORK_ENV) cargo clippy --workspace --all-targets -- -D warnings

fmt: ensure-hooks
	$(CARGO_WORK_ENV) cargo sort --workspace
	$(CARGO_WORK_ENV) cargo sort-derives
	$(CARGO_WORK_ENV) cargo fmt --all

fmt-check: ensure-hooks
	$(CARGO_WORK_ENV) cargo sort --workspace --check
	$(CARGO_WORK_ENV) cargo sort-derives --check
	$(CARGO_WORK_ENV) cargo fmt --all -- --check

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
	bash scripts/ci/check-dependency-graph-invariants.sh
	bash scripts/ci/check-executor-no-production-panics.sh
	bash scripts/ci/check-generated-build-config-invariants.sh
	bash scripts/ci/check-index-range-spec-invariants.sh
	bash scripts/ci/check-layer-authority-invariants.sh
	bash scripts/ci/check-mutation-atomicity-invariants.sh
	bash scripts/ci/check-durability-doc-invariants.sh
	bash scripts/ci/check-read-admission-invariants.sh
	bash scripts/ci/check-sql-branch-ownership-invariants.sh
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

# Run tests in watch mode
test-watch:
	$(CARGO_WORK_ENV) cargo watch -x test

# Build and test everything
all: ensure-clean ensure-hooks clean fmt-check clippy check test build

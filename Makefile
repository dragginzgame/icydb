.DEFAULT_GOAL := help

.PHONY: help version tags package publish release-clean \
        test test-unit test-integration-feedback test-durability test-documentation \
        test-canister-artifact-contract test-sql-canister-matrix check-wire-browser test-wire-browser \
        test-sql-tier-c-shard test-sql-tier-c-merge \
        test-sql-tier-c-replay \
        build-canister-local build-canister-production \
        build check clippy validate validate-fast clean install install-dev update-dev install-gh install-hooks \
        fetch fetch-adapter test-watch all ensure-clean security-check check-versioning \
        test-no-default-smoke \
        wasm-size-report wasm-audit-report \
        lint-workflows shellcheck check-dependency-pins check-invariants check-portable-automation check-feature-matrix \
        ci-static ci-core ci-workspace ci-sql-tier-a ci-sql-tier-b \
        _test-icydb-no-default _test-core-no-default _test-workspace _test-adapter-contracts _test-canister-libs \
        _test-durability-core-commit _test-durability-core-mutation-job _test-durability-integration \
        _ci-format _ci-core-no-default-check _ci-core-no-default-test \
        _ci-core-sql-check _ci-core-sql-clippy \
        _ci-workspace-clippy _ci-workspace-integration-clippy _ci-workspace-tests _ci-adapter-workspace-clippy _ci-adapter-workspace-tests \
        _ci-tier-a-sqlite _ci-tier-a-mutation _ci-tier-a-integration \
        _ci-tier-b-sql-canister _ci-tier-b-sql-perf \
        print-cargo-home print-cargo-target-dir check-dependency-pins \
        _icydb-ic-tool-policy install-testkit testkit-check install-release-editor release-editor-check

# Resolve the repo root from this Makefile so scripts can query these values
# via `make -C "$$ROOT"` and share a single source of truth.
ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
override SHARED_TOOLING_ROOT := $(ROOT_DIR)
LOCAL_TOOL_INSTALL_TARGETS := install-ic-tools install-release-editor
LOCAL_TOOL_CHECK_TARGETS := ic-tools-check release-editor-check
# IC executables are an explicit IcyDB selection under the shared baseline.
IC_TOOLS ?= icp ic-wasm wasm-opt
export PATH := $(ROOT_DIR)/.tools/ic/bin:$(PATH)
# Include derive sorting in the shared manifest/derive/Rustfmt flow.
FORMAT_RUST_ARGS := --sort-derives
include $(ROOT_DIR)/make/consumer.mk

# Keep workspace cargo state repo-local so sibling repos compiling on the same
# filesystem do not contend on a shared cargo home or target directory.
CARGO_WORK_HOME := $(ROOT_DIR)/.cache/cargo/icydb
CARGO_WORK_TARGET_DIR := $(ROOT_DIR)/target/icydb
RELEASE_TMP_DIR := $(ROOT_DIR)/.cache/release-tmp
ADAPTER_MANIFEST := $(ROOT_DIR)/testing/canic-adapter/Cargo.toml
ADAPTER_MANIFEST_FLAG := --manifest-path "$(ADAPTER_MANIFEST)"
CARGO_WORK_ENV := CARGO_HOME="$(CARGO_WORK_HOME)" CARGO_TARGET_DIR="$(CARGO_WORK_TARGET_DIR)"
CARGO_PUBLISH_ENV := CARGO_TARGET_DIR="$(CARGO_WORK_TARGET_DIR)"
TESTKIT_RUNNER := bash "$(ROOT_DIR)/scripts/ci/testkit-runner.sh"
RELEASE_EDITOR_TOOL = bash "$(ROOT_DIR)/scripts/dev/install-rust-tools.sh" --consumer "$(ROOT_DIR)" --package ic-build --lockfile "$(ROOT_DIR)/Cargo.lock" --bin ic-build-release-metadata --profile debug --features cli
RELEASE_EDITOR_CHECK = $(RELEASE_EDITOR_TOOL) --check
# Admit the locked editor before invoking a release consumer. A failed producer
# cannot supply its path; recovery/verification only check prepared tools.
RELEASE_EDITOR_ENV = editor="$$( $(CARGO_WORK_ENV) $(RELEASE_EDITOR_CHECK) )" || exit; $(CARGO_WORK_ENV) CARGO_RELEASE_METADATA_EDITOR="$$editor" CARGO_RELEASE_METADATA_MAX_BYTES=8388608
TESTKIT_SERVER_CHECK = runner="$$( $(TESTKIT_RUNNER) --check )" && "$$runner" check --directory "$(ROOT_DIR)/.tools/ic-testkit-server"
# Refuse failed offline admission before invoking Cargo. Explicit server
# overrides remain subject to Testkit's runtime admission, as before.
IC_TESTKIT_ENV = server="$(POCKET_IC_BIN)"; if [ -z "$$server" ]; then server="$$( $(TESTKIT_SERVER_CHECK) )" || exit; fi; TMPDIR="$(ROOT_DIR)/.cache" POCKET_IC_BIN="$$server"
# Workspace and integration lanes share a lower cap to limit concurrent
# PocketIC test bodies; core-only lanes retain their wider bounded parallelism.
CORE_TEST_ENV := RUST_TEST_THREADS=8
WORKSPACE_TEST_ENV := RUST_TEST_THREADS=2
VALIDATION_RUNNER := bash "$(ROOT_DIR)/scripts/ci/run-icydb-validation-targets.sh" --input Cargo.toml --input Cargo.lock --input rust-toolchain.toml
ADAPTER_VALIDATION_RUNNER := $(VALIDATION_RUNNER) --input testing/canic-adapter/Cargo.toml --input testing/canic-adapter/Cargo.lock
POCKET_IC_RUNNER := bash "$(ROOT_DIR)/scripts/ci/run-with-pocketic-server.sh"
ACTIONLINT_VERSION := $(shell awk '$$1 == "version" {print $$2}' "$(ROOT_DIR)/scripts/ci/actionlint-checksums.tsv")
ACTIONLINT_INSTALL_DIR ?= $(HOME)/.local/bin
ACTIONLINT_BIN ?= $(ACTIONLINT_INSTALL_DIR)/actionlint
YQ ?= $(ROOT_DIR)/.tools/host/bin/yq
TIER_C_ARTIFACT_DIR ?= $(ROOT_DIR)/artifacts/correctness/sql_tier_c
TIER_C_FAILURE_ARTIFACT ?=

# Print repo-local cargo paths for standalone shell scripts that need Makefile-
# owned defaults without duplicating the path definitions.
print-cargo-home:
	@printf '%s\n' "$(CARGO_WORK_HOME)"

print-cargo-target-dir:
	@printf '%s\n' "$(CARGO_WORK_TARGET_DIR)"

# Explicit setup only: validation never installs the CLI or server.
install-testkit:
	+@runner="$$( $(TESTKIT_RUNNER) )" && "$$runner" setup --directory "$(ROOT_DIR)/.tools/ic-testkit-server"

testkit-check:
	+@$(TESTKIT_SERVER_CHECK)

install-release-editor:
	+@$(CARGO_WORK_ENV) $(RELEASE_EDITOR_TOOL)

release-editor-check:
	+@$(CARGO_WORK_ENV) $(RELEASE_EDITOR_TOOL) --check

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
	@echo "  install-tools    Install pinned repository-local host, IC and Rust tools"
	@echo "  tools-check      Verify the selected local tools offline"
	@echo "  shared-tooling-check Verify the reviewed shared snapshot offline"
	@echo "  install-testkit  Prepare the explicit adapter Testkit CLI/server after make fetch-adapter"
	@echo "  testkit-check    Verify Testkit offline and print its admitted server path"
	@echo "  install          Install the local icydb CLI binary"
	@echo "  install-dev      Install developer dependencies, GitHub CLI, actionlint, and the formatting hook"
	@echo "  update-dev       Update developer tooling and hooks without changing dependencies"
	@echo "  install-gh       Ensure the GitHub CLI is installed"
	@echo "  install-hooks    Configure the formatting-only pre-commit hook"
	@echo ""
	@echo "Version Management:"
	@echo "  version          Show current version"
	@echo "  tags             List available git tags"
	@echo "  release-clean    Remove transient release artifacts; Cargo cleanup stays manual"
	@echo "  release-patch    Validate, prepare, commit, tag, and push a patch release"
	@echo "  release-minor    Confirm and run the shared minor release workflow"
	@echo "  release-major    Confirm and run the shared major release workflow"
	@echo "  release-resume VERSION=X.Y.Z  Resume the exact saved release candidate"
	@echo "  package          Build publishable crate tarballs"
	@echo "  publish          Publish crates; reuse the exact shared release receipt when available"
	@echo ""
	@echo "Development:"
	@echo "  cloc             Report workspace Rust runtime and test source counts"
	@echo "  test             Run product workspace tests without Canic or PocketIC"
	@echo "  test-adapter     Run composed Canic tests; prepare install-testkit first"
	@echo "  validate-adapter  Run explicit adapter formatting, automation, lint and tests"
	@echo "  fmt-adapter-check  Check root and adapter Rust/manifests explicitly"
	@echo "  clippy-adapter   Lint the composed Canic workspace and feature contracts"
	@echo "  test-integration-feedback TEST_TARGET=... TEST_NAME=..."
	@echo "                  Run one exact integration test, then its complete binary"
	@echo "  test-durability  Run Core commit and mutation-job suites"
	@echo "  test-documentation  Check links, compiled doc data, the example, and codec evidence"
	@echo "  test-canister-artifact-contract"
	@echo "                  Build and inspect all 34 independent production/local canister artifacts"
	@echo "  test-sql-canister-matrix"
	@echo "                  Run the live generated SQL canister endpoint matrix"
	@echo "  check-wire-browser  Check native/browser frames with prepared Node tools"
	@echo "  test-wire-browser   Qualify installed adapter endpoints in Chromium"
	@echo "  test-sql-tier-c-shard TIER_C_SHARD=0"
	@echo "                  Run one exact native Tier C correctness shard (0 through 7)"
	@echo "  test-sql-tier-c-merge"
	@echo "                  Merge all eight Tier C receipts and publish typed coverage"
	@echo "  test-sql-tier-c-replay TIER_C_FAILURE_ARTIFACT=..."
	@echo "                  Reproduce one minimized Tier C failure exactly"
	@echo "  build-canister-local CANISTER=demo_rpg"
	@echo "                  Build and stage the exact local/test feature profile"
	@echo "  build-canister-production CANISTER=demo_rpg"
	@echo "                  Build and stage the exact production feature profile"
	@echo "  build            Build all crates"
	@echo "  check            Run cargo check --locked"
	@echo "  clippy           Lint root packages; clippy-adapter selects composition"
	@echo "  validate         Validate product packages; validate-adapter selects composition"
	@echo "  validate-fast    Run the quick formatting, automation, invariant, and workspace-check preflight"
	@echo "  fetch            Fetch locked root package dependencies"
	@echo "  fetch-adapter    Fetch the independent Canic qualification lock"
	@echo "  fmt              Format code"
	@echo "  fmt-check        Check formatting"
	@echo "  clean            Manually clean Cargo build artifacts"
	@echo "  wasm-size-report Build and report the maintained Wasm measurement subjects"
	@echo "  wasm-audit-report Build Wasm + write Twiggy reports for maintained measurement subjects"
	@echo "  lint-workflows   Lint GitHub Actions workflows with pinned actionlint"
	@echo "  shellcheck       Lint repository shell automation"
	@echo ""
	@echo "Utilities:"
	@echo "  test-watch       Run tests in watch mode"
	@echo "  all              Run all checks, tests, and build"
	@echo "  security-check   Verify GitHub Protected Tags (informational)"
	@echo ""
	@echo "Examples:"
	@echo "  make release-patch # Maintainer-owned patch release"
	@echo "  make test        # Run tests"
	@echo "  make build       # Build project"
	@echo "  make wasm-size-report SIZE_REPORT_ARGS=\"--sql-variants both\""
	@echo "  make wasm-size-report SIZE_REPORT_ARGS=\"--canister ten_entity_typed_query\""

#
# Installing
#

# Install the developer CLI as `icydb` into cargo's normal bin directory.
install:
	+cargo install --path "$(ROOT_DIR)/crates/icydb-cli" --bin icydb --locked --force

# Install local developer prerequisites, tools, and the formatting hook.
install-dev:
	+ACTIONLINT_INSTALL_DIR="$(ACTIONLINT_INSTALL_DIR)" scripts/dev/workstation-setup.sh install

# Update user-local Rust/Cargo/actionlint/ICP developer tooling and the hook.
update-dev:
	+ACTIONLINT_INSTALL_DIR="$(ACTIONLINT_INSTALL_DIR)" scripts/dev/workstation-setup.sh update

# Keep one idempotent GitHub CLI installer for workstation setup and CI jobs.
install-gh:
	bash scripts/ci/install-gh.sh

# Provisioning and offline bundle verification belong to the shared include.
# IcyDB additionally admits its raw optimizer executable. Testkit owns PocketIC
# client/server version admission when starting the governed server.
ifneq ($(filter wasm-opt,$(IC_TOOLS)),)
ic-tools-check: _icydb-ic-tool-policy
endif

_icydb-ic-tool-policy:
	bash scripts/ci/verify-wasm-optimizer.sh

#
# Version management (the source candidate is gated before any version mutation)
#

version:
	+@$(CARGO_WORK_ENV) YQ="$(YQ)" bash scripts/ci/read-cargo-workspace-version.sh --stable "$(CURDIR)/Cargo.toml"

tags:
	@git tag --sort=-version:refname | head -10

release-clean:
	@bash scripts/ci/cleanup-release-workspace.sh

package: ensure-clean
	+$(CARGO_WORK_ENV) cargo package --locked

publish:
	+$(CARGO_PUBLISH_ENV) scripts/ci/publish-workspace.sh

#
# Tests
#

test:
	+$(VALIDATION_RUNNER) test-unit

test-unit:
	+$(VALIDATION_RUNNER) \
		_test-icydb-no-default \
		_test-core-no-default \
		_test-workspace

_test-icydb-no-default:
	+$(CARGO_WORK_ENV) cargo test --locked --no-fail-fast -p icydb --no-default-features

_test-core-no-default:
	+$(CORE_TEST_ENV) $(CARGO_WORK_ENV) cargo test --locked --no-fail-fast -p icydb-core --no-default-features

_test-workspace:
	+$(WORKSPACE_TEST_ENV) $(CARGO_WORK_ENV) cargo test --locked --no-fail-fast --workspace --all-targets

# Composition is an explicit consumer gate, independent of product validation.
.PHONY: test-adapter validate-adapter clippy-adapter fmt-adapter fmt-adapter-check ci-adapter ci-adapter-sql-tier-a check-adapter-automation _test-adapter-workspace _test-adapter-documentation _ci-adapter-feature-clippy

test-adapter:
	+$(ADAPTER_VALIDATION_RUNNER) _test-adapter-workspace _test-adapter-contracts _test-canister-libs test-canister-artifact-contract

_test-adapter-workspace:
	+$(IC_TESTKIT_ENV) $(WORKSPACE_TEST_ENV) $(CARGO_WORK_ENV) cargo test $(ADAPTER_MANIFEST_FLAG) --locked --no-fail-fast --workspace --all-targets --exclude canister_demo_rpg --exclude canister_test_sql --exclude canister_test_sql_bounded

# Default-feature refusal fixtures run in the adapter workspace selection above;
# this companion exercises every supported endpoint capability and pass target.
_test-adapter-contracts:
	+$(WORKSPACE_TEST_ENV) $(CARGO_WORK_ENV) cargo test $(ADAPTER_MANIFEST_FLAG) --locked --no-fail-fast -p icydb-testing-canic-adapter --all-targets --all-features

_test-canister-libs:
	+$(WORKSPACE_TEST_ENV) $(CARGO_WORK_ENV) cargo test $(ADAPTER_MANIFEST_FLAG) --locked --no-fail-fast -p canister_test_sql -p canister_test_sql_bounded --lib
	+$(WORKSPACE_TEST_ENV) $(CARGO_WORK_ENV) cargo test $(ADAPTER_MANIFEST_FLAG) --locked --no-fail-fast -p canister_test_sql --lib --all-features
	+$(WORKSPACE_TEST_ENV) $(CARGO_WORK_ENV) cargo test $(ADAPTER_MANIFEST_FLAG) --locked --no-fail-fast -p canister_audit_sql_perf --lib --features test-admin-api

test-no-default-smoke:
	+$(VALIDATION_RUNNER) _test-icydb-no-default _test-core-no-default

test-integration-feedback:
	@test -n "$(TEST_TARGET)" || { echo "TEST_TARGET must name one icydb-testing-integration test binary" >&2; exit 1; }
	@test -n "$(TEST_NAME)" || { echo "TEST_NAME must name one exact test in $(TEST_TARGET)" >&2; exit 1; }
	+$(IC_TESTKIT_ENV) $(WORKSPACE_TEST_ENV) $(CARGO_WORK_ENV) cargo test $(ADAPTER_MANIFEST_FLAG) --locked -p icydb-testing-integration \
		--test "$(TEST_TARGET)" "$(TEST_NAME)" -- --exact --nocapture
	+$(IC_TESTKIT_ENV) $(WORKSPACE_TEST_ENV) $(CARGO_WORK_ENV) cargo test $(ADAPTER_MANIFEST_FLAG) --locked --no-fail-fast \
		-p icydb-testing-integration --test "$(TEST_TARGET)"

test-durability:
	+$(VALIDATION_RUNNER) \
		_test-durability-core-commit \
		_test-durability-core-mutation-job

_test-durability-core-commit:
	+$(CARGO_WORK_ENV) cargo test --locked -p icydb-core --lib 'db::commit::'

_test-durability-core-mutation-job:
	+$(CARGO_WORK_ENV) cargo test --locked -p icydb-core --lib 'db::mutation_job::'

_test-durability-integration:
	+$(IC_TESTKIT_ENV) $(WORKSPACE_TEST_ENV) $(CARGO_WORK_ENV) cargo test $(ADAPTER_MANIFEST_FLAG) --locked --no-fail-fast \
		-p icydb-testing-integration \
		--test convergence_candidate \
		--test durable_mutation_job_scale \
		--test recovery_closeout \
		--test logical_memory \
		--test schema_migration_closeout \
		--test lifecycle_participant \
		--test read_authority \
		--test sql_guard \
		--test schema_guard

test-canister-artifact-contract:
	+$(CARGO_WORK_ENV) cargo test $(ADAPTER_MANIFEST_FLAG) --locked -p icydb-testing-integration \
		--test canister_artifact_contract \
		-- --include-ignored --nocapture

check-wire-browser:
	+$(CARGO_WORK_ENV) node testing/wire-browser/check.mjs

test-wire-browser:
	+$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) cargo test $(ADAPTER_MANIFEST_FLAG) --locked -p icydb-testing-integration \
		--test query_canister native_wire_browser_qualifies_the_installed_endpoint_roster \
		-- --ignored --exact --nocapture
	+$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) cargo test $(ADAPTER_MANIFEST_FLAG) --locked -p icydb-testing-integration \
		--test schema_migration_closeout native_wire_browser_qualifies_rejected_migration_findings \
		-- --ignored --exact --nocapture
	+$(IC_TESTKIT_ENV) $(CARGO_WORK_ENV) cargo test $(ADAPTER_MANIFEST_FLAG) --locked -p icydb-testing-integration \
		--test query_canister native_wire_browser_qualifies_physical_integrity_findings \
		-- --ignored --exact --nocapture

test-sql-canister-matrix:
	+$(IC_TESTKIT_ENV) $(WORKSPACE_TEST_ENV) $(CARGO_WORK_ENV) cargo test $(ADAPTER_MANIFEST_FLAG) --locked --no-fail-fast -p icydb-testing-integration --test sql_canister -- --nocapture

test-sql-tier-c-shard:
	@test -n "$(TIER_C_SHARD)" || { echo "TIER_C_SHARD must be an index from 0 through 7" >&2; exit 1; }
	@mkdir -p "$(TIER_C_ARTIFACT_DIR)"
	@rm -f "$(TIER_C_ARTIFACT_DIR)/tier-c-shard-$(TIER_C_SHARD).json"
	+ICYDB_SQL_TIER_C_SHARD_INDEX="$(TIER_C_SHARD)" \
	ICYDB_SQL_TIER_C_ARTIFACT_DIR="$(TIER_C_ARTIFACT_DIR)" \
	$(CARGO_WORK_ENV) \
	cargo test --locked -p icydb-core --lib --features sql \
		db::session::tests::tier_c_reference::tier_c_native_shard_emits_exact_receipt \
		-- --ignored --exact --nocapture --test-threads=1
	@test -s "$(TIER_C_ARTIFACT_DIR)/tier-c-shard-$(TIER_C_SHARD).json" || { echo "Tier C shard produced no current receipt" >&2; exit 1; }

test-sql-tier-c-merge:
	@rm -f "$(TIER_C_ARTIFACT_DIR)/tier-c-merged.json"
	+ICYDB_SQL_TIER_C_ARTIFACT_DIR="$(TIER_C_ARTIFACT_DIR)" \
	$(CARGO_WORK_ENV) \
	cargo test --locked -p icydb-core --lib --features sql \
		db::session::tests::tier_c_reference::tier_c_native_receipts_merge_exactly_and_require_clean_evidence \
		-- --ignored --exact --nocapture --test-threads=1
	@test -s "$(TIER_C_ARTIFACT_DIR)/tier-c-merged.json" || { echo "Tier C merge produced no current receipt" >&2; exit 1; }

test-sql-tier-c-replay:
	@test -n "$(TIER_C_FAILURE_ARTIFACT)" || { echo "TIER_C_FAILURE_ARTIFACT must name one failure.<blake3>.json artifact" >&2; exit 1; }
	+ICYDB_SQL_TIER_C_FAILURE_ARTIFACT="$(TIER_C_FAILURE_ARTIFACT)" \
	$(CARGO_WORK_ENV) \
	cargo test --locked -p icydb-core --lib --features sql \
		db::session::tests::tier_c_reference::tier_c_failure_artifact_replays_exact_minimized_failure \
		-- --ignored --exact --nocapture --test-threads=1

build-canister-local:
	@test -n "$(CANISTER)" || { echo "CANISTER must name one maintained canister" >&2; exit 1; }
	+$(CARGO_WORK_ENV) cargo run --locked -p icydb-testing-build \
		--bin build_fixture_canister -- "$(CANISTER)" --build-profile local \
		--profile debug

build-canister-production:
	@test -n "$(CANISTER)" || { echo "CANISTER must name one maintained canister" >&2; exit 1; }
	+$(CARGO_WORK_ENV) cargo run --locked -p icydb-testing-build \
		--bin build_fixture_canister -- "$(CANISTER)" --build-profile production \
		--profile wasm-release

wasm-size-report:
	+$(CARGO_WORK_ENV) bash scripts/ci/wasm-size-report.sh $(SIZE_REPORT_ARGS)

wasm-audit-report:
	+$(CARGO_WORK_ENV) bash scripts/ci/wasm-audit-report.sh $(AUDIT_REPORT_ARGS)

#
# Development commands
#

fetch:
	+$(CARGO_WORK_ENV) cargo fetch --locked

fetch-adapter:
	+$(CARGO_WORK_ENV) cargo fetch $(ADAPTER_MANIFEST_FLAG) --locked

build:
	+$(CARGO_WORK_ENV) cargo build --locked --release --workspace

check:
	+$(CARGO_WORK_ENV) cargo check --locked --workspace

clippy:
	+$(CARGO_WORK_ENV) cargo clippy --locked --workspace --all-targets -- -D warnings
	+$(CARGO_WORK_ENV) cargo clippy --locked -p icydb-core --no-default-features --features sql -- -D warnings

clippy-adapter:
	+$(ADAPTER_VALIDATION_RUNNER) _ci-adapter-workspace-clippy _ci-workspace-integration-clippy _ci-adapter-feature-clippy

_ci-adapter-feature-clippy:
	+$(CARGO_WORK_ENV) cargo clippy $(ADAPTER_MANIFEST_FLAG) --locked -p canister_audit_one_entity_sql_query -p canister_test_sql_guard \
		--all-targets --all-features -- -D warnings

# Formatting has the same ownership boundary as compiler/test qualification.
fmt-adapter:
	+$(MAKE) --no-print-directory fmt 'FORMAT_RUST_ARGS=--sort-derives --extra-workspace "$(ROOT_DIR)/testing/canic-adapter"'

fmt-adapter-check:
	+$(MAKE) --no-print-directory fmt-check 'FORMAT_RUST_ARGS=--sort-derives --extra-workspace "$(ROOT_DIR)/testing/canic-adapter"'

validate-adapter:
	+$(ADAPTER_VALIDATION_RUNNER) --fail-fast fmt-adapter-check check-adapter-automation clippy-adapter
	+$(ADAPTER_VALIDATION_RUNNER) test-adapter

# Preserve repository-local Cargo state through the shared formatting targets.
fmt fmt-check format-tools-check: export CARGO_HOME := $(CARGO_WORK_HOME)
fmt fmt-check format-tools-check: export CARGO_TARGET_DIR := $(CARGO_WORK_TARGET_DIR)

validate:
	$(MAKE) --no-print-directory tools-check
	# Admit lint before invariant fixtures, product builds or executable tests.
	+$(VALIDATION_RUNNER) --fail-fast \
		fmt-check \
		lint-workflows \
		shellcheck \
		check-dependency-pins \
		clippy \
		check-invariants \
		check
	# Once clippy passes, retain every later long-lane failure in one combined log.
	+$(VALIDATION_RUNNER) \
		check-feature-matrix \
		test

# Explicit broad static preflight. This intentionally does not replace `validate`:
# feature-specific clippy lanes and executable tests remain in the full gate.
validate-fast:
	$(MAKE) --no-print-directory tools-check
	+$(VALIDATION_RUNNER) --fail-fast \
		fmt-check \
		lint-workflows \
		shellcheck \
		check-dependency-pins \
		check-invariants \
		check

clean:
	+CARGO_HOME="$(CARGO_WORK_HOME)" cargo clean


# Security and versioning checks
security-check:
	@echo "Security checks are enforced via GitHub settings:"
	@echo "- Enable Protected Tags for pattern 'v*' (Settings → Tags)"
	@echo "- Restrict who can create tags and disable force pushes"
	@echo "- Require PR + CI on 'main' via branch protection"
	@echo "This target is informational only; no local script runs."

check-versioning: security-check
	+@$(RELEASE_EDITOR_ENV) bash scripts/release/render-candidate.sh --check
	@$(MAKE) --no-print-directory version >/dev/null
	@$(MAKE) --no-print-directory help >/dev/null
	@echo "Versioning tooling checks passed."

# Focused documentation evidence: evaluate compiled owners and the example.
# Structural link checks also run in the fast invariant gate below.
test-documentation:
	perl scripts/ci/test-documentation.pl
	perl scripts/ci/check-documentation.pl
	+$(CARGO_WORK_ENV) cargo test --locked -p icydb-schema --lib documentation_
	+$(CARGO_WORK_ENV) cargo test --locked -p icydb-model-macros --lib documentation_
	+$(CARGO_WORK_ENV) cargo test --locked -p icydb-core --lib --all-features documentation_
	+$(CARGO_WORK_ENV) cargo test --locked -p icydb-core --lib --all-features db::startup::receipt::tests::
	+$(CARGO_WORK_ENV) cargo test --locked -p icydb-core --lib --all-features db::schema::migration_record::tests::
	+$(CARGO_WORK_ENV) cargo test --locked -p icydb-core --lib --all-features db::schema::control_store::tests::
	+$(CARGO_WORK_ENV) cargo test --locked -p icydb-core --lib --all-features db::schema::identity_state::tests::

check-dependency-pins:
	+$(CARGO_WORK_ENV) YQ="$(YQ)" bash scripts/ci/check-dependency-pins.sh --cargo-inheritance

# The static gate and native host qualification execute this same portable
# fixture selection. Host-specific setup, lint and Cargo gates stay with callers.
check-portable-automation: shared-tooling-check
	+$(RELEASE_EDITOR_ENV) bash scripts/release/test-lock-selection.sh
	bash scripts/ci/test-shared-tooling-adapters.sh
	bash scripts/ci/test-workstation-setup.sh
	bash scripts/ci/test-cargo-metadata-adoption.sh
	bash scripts/ci/test-ci-workflow-invariants.sh
	+$(CARGO_WORK_ENV) bash scripts/ci/test-invariant-scanners.sh
	bash scripts/ci/check-ci-workflow-invariants.sh
	bash scripts/release/test-standard-release.sh
	bash scripts/release/test-receipt-callbacks.sh
	bash scripts/ci/test-tooling-fixture-completion.sh
	+$(RELEASE_EDITOR_ENV) bash scripts/release/test-pin-exceptions.sh
	bash scripts/ci/test-publish-workspace.sh
	bash scripts/ci/test-release-runner.sh
	perl scripts/dev/delete-github-tags-up-to.pl --cutoff 0.210 >/dev/null
	bash scripts/ci/test-pre-commit.sh
	perl scripts/ci/test-documentation.pl
	perl scripts/ci/check-documentation.pl

check-adapter-automation:
	bash scripts/ci/test-testkit-tooling.sh
	bash scripts/ci/test-pocketic-server-wrapper.sh
	+$(CARGO_WORK_ENV) bash scripts/ci/check-cargo-dependency-boundaries.sh --consumer "$(ROOT_DIR)/testing/canic-adapter" --external-ic

check-invariants:
	+$(MAKE) --no-print-directory check-portable-automation
	bash scripts/release/test-finalize-notes.sh
	bash scripts/ci/check-deployment-inventory-invariants.sh
	+$(CARGO_WORK_ENV) bash scripts/ci/check-dependency-graph-invariants.sh
	+$(CARGO_WORK_ENV) bash scripts/ci/check-cargo-dependency-boundaries.sh --consumer "$(ROOT_DIR)" --external-ic
	+$(CARGO_WORK_ENV) bash scripts/ci/check-executor-no-production-panics.sh
	bash scripts/ci/check-generated-endpoint-invariants.sh
	bash scripts/ci/check-index-range-spec-invariants.sh
	bash scripts/ci/check-layer-authority-invariants.sh
	bash scripts/ci/check-mutation-atomicity-invariants.sh
	bash scripts/ci/check-release-cleanup-invariants.sh
	+$(RELEASE_EDITOR_ENV) bash scripts/ci/test-release-candidate-receipt.sh
	bash scripts/ci/check-persisted-format-invariants.sh
	bash scripts/ci/check-read-admission-invariants.sh
	bash scripts/ci/test-read-admission-invariants.sh
	bash scripts/ci/check-schema-model-boundary-invariants.sh
	bash scripts/ci/check-sql-branch-ownership-invariants.sh
	bash scripts/ci/check-wasm-post-link-invariants.sh
	bash scripts/ci/check-memory-id-invariants.sh

check-feature-matrix:
	+$(CARGO_WORK_ENV) cargo check --locked -p icydb --no-default-features
	+$(CARGO_WORK_ENV) cargo check --locked -p icydb-core --no-default-features
	+$(CARGO_WORK_ENV) cargo check --locked -p icydb --no-default-features --features sql
	+$(CARGO_WORK_ENV) cargo check --locked -p icydb-core --no-default-features --features sql
	+$(CARGO_WORK_ENV) cargo check --locked --workspace --no-default-features

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
		scripts/app/*.sh scripts/ci/*.sh scripts/dev/*.sh scripts/release/*.sh .githooks/pre-commit

# GitHub Actions consumes these exact local targets as parallel lanes. The
# terminal `check` job aggregates these lanes and native macOS host checks.
# Product branch protection also requires dependency_msrv; adapter evidence is independent;
# their selection is documented in .github/BRANCH_PROTECTION.md.
ci-static:
	+$(VALIDATION_RUNNER) --fail-fast host-tools-check _ci-format lint-workflows shellcheck check-dependency-pins check-invariants

_ci-format:
	$(MAKE) --no-print-directory fmt-check

# Recursive Cargo recipes preserve Make's advertised jobserver descriptors.
# The shared execution admission above rejects skipped or ignored execution.
ci-core:
	+$(VALIDATION_RUNNER) \
		_ci-core-no-default-check \
		_ci-core-sql-check \
		_ci-core-sql-clippy \
		_ci-core-no-default-test

_ci-core-no-default-check:
	+$(CARGO_WORK_ENV) cargo check --locked -p icydb -p icydb-core --no-default-features
	+$(CARGO_WORK_ENV) cargo check --locked --workspace --no-default-features

_ci-core-no-default-test:
	+$(CORE_TEST_ENV) $(CARGO_WORK_ENV) cargo test --locked --no-fail-fast \
		-p icydb -p icydb-core --no-default-features

_ci-core-sql-check:
	+$(CARGO_WORK_ENV) cargo check --locked \
		-p icydb -p icydb-core --no-default-features --features sql

_ci-core-sql-clippy:
	+$(CARGO_WORK_ENV) cargo clippy --locked \
		-p icydb-core --no-default-features --features sql -- -D warnings

ci-workspace:
	+$(VALIDATION_RUNNER) \
		_ci-workspace-clippy \
		_ci-workspace-tests

ci-adapter:
	+$(ADAPTER_VALIDATION_RUNNER) \
		_ci-adapter-workspace-clippy \
		_ci-workspace-integration-clippy \
		_ci-adapter-feature-clippy \
		_ci-adapter-workspace-tests \
		_test-adapter-contracts \
		test-canister-artifact-contract \
		_test-canister-libs \
		_test-adapter-documentation \
		_test-durability-integration

_ci-workspace-clippy:
	+$(CARGO_WORK_ENV) cargo clippy --locked --workspace --all-targets \
		-- -D warnings

_ci-workspace-integration-clippy:
	+$(CARGO_WORK_ENV) cargo clippy $(ADAPTER_MANIFEST_FLAG) --locked -p icydb-testing-integration \
		--tests -- -D warnings

_ci-workspace-tests:
	+$(WORKSPACE_TEST_ENV) $(CARGO_WORK_ENV) cargo test --locked --no-fail-fast \
		--workspace --all-targets --verbose

ci-sql-tier-a:
	+$(VALIDATION_RUNNER) \
		_ci-tier-a-sqlite \
		_ci-tier-a-mutation

_ci-tier-a-sqlite:
	+$(CARGO_WORK_ENV) bash scripts/ci/run-nonempty-cargo-test.sh --locked -p icydb-core \
		--no-default-features --features sql \
		db::session::tests::tier_c_reference::tier_a_native_sqlite_reference --verbose

_ci-tier-a-mutation:
	+$(CARGO_WORK_ENV) bash scripts/ci/run-nonempty-cargo-test.sh --locked -p icydb-core \
		--no-default-features --features sql \
		db::session::tests::tier_c_reference::tier_a_native_mutation_reference --verbose

ci-adapter-sql-tier-a:
	+$(ADAPTER_VALIDATION_RUNNER) _ci-tier-a-integration

_ci-tier-a-integration:
	+$(WORKSPACE_TEST_ENV) $(CARGO_WORK_ENV) cargo test $(ADAPTER_MANIFEST_FLAG) --locked --no-fail-fast \
		-p icydb-testing-integration --test sql_correctness --verbose

ci-sql-tier-b:
	+TMPDIR="$(ROOT_DIR)/.cache" POCKET_IC_BIN="$(POCKET_IC_BIN)" $(WORKSPACE_TEST_ENV) $(CARGO_WORK_ENV) \
		$(POCKET_IC_RUNNER) $(ADAPTER_VALIDATION_RUNNER) \
		_ci-tier-b-sql-canister \
		_ci-tier-b-sql-perf

_ci-tier-b-sql-canister:
	+cargo test $(ADAPTER_MANIFEST_FLAG) --locked --no-fail-fast \
		-p icydb-testing-integration --test sql_canister --verbose

_ci-tier-b-sql-perf:
	+cargo test $(ADAPTER_MANIFEST_FLAG) --locked --no-fail-fast \
		-p icydb-testing-integration --test sql_perf_audit --verbose -- --nocapture

# Run tests in watch mode
test-watch:
	+$(CARGO_WORK_ENV) cargo watch -x "test --locked"

# Build and test everything through explicit, sequential workflow steps while
# preserving the reusable Cargo build cache. `make clean` remains manual.
all: ensure-clean
	$(MAKE) --no-print-directory validate
	$(MAKE) --no-print-directory build

# IcyDB owns metadata preparation, validation and receipt checks. Shared
# make/release.mk owns the standard entrypoints and conflicting-goal admission.
.PHONY: release-version release-preflight release-prepare-version release-prepared-check release-files release-commit-check release-committed-check release-tagged-check release-push-check
# Cargo metadata/receipt children share the selected repository cache and live
# jobserver even when the callback reaches Cargo through multiple shell owners.
release-preflight release-prepare-version release-prepared-check release-commit-check release-committed-check release-tagged-check release-push-check: export CARGO_HOME := $(CARGO_WORK_HOME)
release-preflight release-prepare-version release-prepared-check release-commit-check release-committed-check release-tagged-check release-push-check: export CARGO_TARGET_DIR := $(CARGO_WORK_TARGET_DIR)
release-verify release-prepare-version release-prepared-check release-commit-check release-committed-check release-tagged-check release-push-check: RELEASE_EDITOR_CHECK = bash "$(ROOT_DIR)/scripts/release/select-editor.sh" "$${RELEASE_COMMIT:-$${RELEASE_SOURCE:-HEAD}}"

.PHONY: release-verify
release-version:
	+@$(CARGO_WORK_ENV) YQ="$(YQ)" bash scripts/ci/read-cargo-workspace-version.sh --stable "$(CURDIR)/Cargo.toml"
release-preflight:
	@awk -v version="$(RELEASE_VERSION)" -v date="$(RELEASE_DATE)" \
		-f scripts/ci/finalize-release-changelog.awk CHANGELOG.md >/dev/null
	+@bash scripts/ci/release-candidate-receipt.sh verify-tested-tree "$(RELEASE_SOURCE)"
	@mkdir -p "$(RELEASE_TMP_DIR)"
	@$(MAKE) --no-print-directory fetch
	@if [ "$${CARGO_NET_OFFLINE:-false}" != true ]; then \
		$(MAKE) --no-print-directory install-tools; \
	fi
	@CARGO_NET_OFFLINE=true $(MAKE) --no-print-directory tools-check
release-verify:
	+$(RELEASE_EDITOR_ENV) TMPDIR="$(RELEASE_TMP_DIR)" CARGO_NET_OFFLINE=true $(MAKE) --no-print-directory validate
release-prepare-version:
	+@$(RELEASE_EDITOR_ENV) TMPDIR="$(RELEASE_TMP_DIR)" bash scripts/release/prepare.sh
release-prepared-check:
	+@$(RELEASE_EDITOR_ENV) bash scripts/release/check-metadata.sh
release-files:
	@bash scripts/release/files.sh
release-commit-check:
	+@$(RELEASE_EDITOR_ENV) bash scripts/release/check-metadata.sh
	+@$(RELEASE_EDITOR_ENV) bash scripts/ci/release-candidate-receipt.sh verify-staged
release-committed-check:
	+@$(RELEASE_EDITOR_ENV) bash scripts/ci/release-candidate-receipt.sh verify-commit
	+@$(RELEASE_EDITOR_ENV) RELEASE_COMMIT="$${RELEASE_COMMIT:-$$(git rev-parse --verify HEAD)}" \
		YQ="$(YQ)" bash scripts/ci/cargo-release-metadata.sh check icydb --manifest-order-grouped
release-tagged-check:
	+@$(RELEASE_EDITOR_ENV) bash scripts/ci/record-release-gate-receipt.sh
release-push-check:
	+@$(RELEASE_EDITOR_ENV) bash scripts/ci/release-candidate-receipt.sh verify-commit
	@release_commit="$(RELEASE_COMMIT)"; \
		if [ -z "$$release_commit" ]; then release_commit="$$(git rev-parse --verify HEAD)" || exit; fi; \
		bash scripts/ci/verify-release-gate-receipt.sh "$$release_commit"

# Canic qualification retains its full actor/test coverage outside package resolution.
_ci-adapter-workspace-clippy:
	+$(CARGO_WORK_ENV) cargo clippy $(ADAPTER_MANIFEST_FLAG) --locked --workspace --all-targets --exclude icydb-testing-integration -- -D warnings

_ci-adapter-workspace-tests:
	+$(WORKSPACE_TEST_ENV) $(CARGO_WORK_ENV) cargo test $(ADAPTER_MANIFEST_FLAG) --locked --no-fail-fast --workspace --all-targets --exclude icydb-testing-integration --verbose

_test-adapter-documentation:
	+$(CARGO_WORK_ENV) cargo test $(ADAPTER_MANIFEST_FLAG) --locked -p icydb-testing-model-facade-only --lib tests::

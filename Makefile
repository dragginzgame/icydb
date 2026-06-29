.PHONY: help version tags patch minor major package publish release-stage release-commit release-push \
        release-patch release-minor release-major release \
        test test-bump test-sql-canister-matrix build check clippy fmt fmt-check clean install install-dev update-dev \
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
	@echo "  update-dev       Update user-local Rust/Cargo/actionlint/ICP developer tools"
	@echo "  install-hooks    Configure git hooks"
	@echo ""
	@echo "Version Management:"
	@echo "  version          Show current version"
	@echo "  tags             List available git tags"
	@echo "  patch            Run release gate, then bump patch version files (0.0.x)"
	@echo "  minor            Run release gate, then bump minor version files (0.x.0)"
	@echo "  major            Run full release gate, then bump major version files (x.0.0)"
	@echo "  release-stage    Stage known release files"
	@echo "  release-commit   Commit version files and create the release tag"
	@echo "  release-push     Push the release commit and tags"
	@echo "  release-patch    Human-owned one-shot patch release"
	@echo "  release-minor    Human-owned one-shot minor release"
	@echo "  release-major    Human-owned one-shot major release"
	@echo "  release          CI-driven release (local target is no-op)"
	@echo "  package          Build publishable crate tarballs"
	@echo "  publish          Publish workspace crates to registry in dependency order"
	@echo ""
	@echo "Development:"
	@echo "  test             Run all tests; lets ic-testkit download pinned PocketIC when uncached"
	@echo "  test-sql-canister-matrix"
	@echo "                  Run the live generated SQL canister endpoint matrix"
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
# Version management (always format and test first)
#

version:
	@$(CARGO_WORK_ENV) cargo get workspace.package.version

tags:
	@git tag --sort=-version:refname | head -10

patch: ensure-clean fmt test-bump
	@$(CARGO_WORK_ENV) scripts/ci/bump-version.sh patch

minor: ensure-clean fmt test-bump
	@$(CARGO_WORK_ENV) scripts/ci/bump-version.sh minor

major: ensure-clean fmt test
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

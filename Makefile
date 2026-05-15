.PHONY: help version tags patch minor major release-stage release-commit release-push \
        release-patch release-minor release-major release \
        test test-bump build check clippy fmt fmt-check clean install install-all install-canister-deps update-dev ensure-python3 \
        test-watch all ensure-clean security-check check-versioning \
        ensure-hooks install-hooks check-index-range-spec-invariants \
        wasm-size-report wasm-audit-report test-sql-parity \
	check-architecture-text-scan-invariants check-invariants \
        check-sql-branch-ownership-invariants \
        print-cargo-home print-cargo-target-dir

# Resolve the repo root from this Makefile so scripts can query these values
# via `make -C "$$ROOT"` and share a single source of truth.
ROOT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))

# Keep workspace cargo state repo-local so sibling repos compiling on the same
# filesystem do not contend on a shared cargo home or target directory.
CARGO_WORK_HOME := $(ROOT_DIR)/.cache/cargo/icydb
CARGO_WORK_TARGET_DIR := $(ROOT_DIR)/target/icydb
CARGO_WORK_ENV := CARGO_HOME="$(CARGO_WORK_HOME)" CARGO_TARGET_DIR="$(CARGO_WORK_TARGET_DIR)"

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
	@echo "  install-all      Install canister dependencies and git hooks"
	@echo "  update-dev       Check prerequisites and run safe maintenance checks"
	@echo "  install-canister-deps  Install Wasm target + candid tools"
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
	@echo ""
	@echo "Development:"
	@echo "  test             Run all tests; uncached PocketIC downloads require ICYDB_ALLOW_POCKET_IC_DOWNLOAD=1"
	@echo "  build            Build all crates"
	@echo "  check            Run cargo check"
	@echo "  clippy           Run clippy checks"
	@echo "  fmt              Format code"
	@echo "  fmt-check        Check formatting"
	@echo "  clean            Clean build artifacts"
	@echo "  wasm-size-report Build and report wasm sizes for minimal + one/ten simple/complex audit canisters"
	@echo "  wasm-audit-report Build wasm + write Twiggy audit reports for minimal + one/ten simple/complex under docs/audits/reports"
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
	@echo "  make wasm-size-report SIZE_REPORT_ARGS=\"--canister ten_complex\""

#
# Installing
#

# Install the developer CLI as `icydb` into cargo's normal bin directory.
install:
	cargo install --path "$(ROOT_DIR)/crates/icydb-cli" --bin icydb --locked --force

# Ensure both `python3` and the `python` alias exist for repo scripts and local
# tooling. This target is check-only; it never installs OS packages or uses sudo.
ensure-python3:
	@if command -v python3 >/dev/null 2>&1 && command -v python >/dev/null 2>&1; then \
		echo "python3 available: $$(python3 --version 2>/dev/null)"; \
		echo "python available: $$(python --version 2>/dev/null)"; \
	else \
		echo "Missing local prerequisites:"; \
		command -v python3 >/dev/null 2>&1 || echo "  - python3"; \
		command -v python >/dev/null 2>&1 || echo "  - python alias pointing to python3"; \
		echo ""; \
		echo "Install these with your system package manager, then re-run this target."; \
		exit 1; \
	fi

# Install canister dependencies and repository hooks.
install-all: install-canister-deps install-hooks
	@echo "Canister dependencies and git hooks installed"

# Check local prerequisites and run safe maintenance checks. This target does
# not install cargo tools, update rustup, or mutate Cargo.lock.
update-dev: ensure-python3
	@command -v rustup >/dev/null 2>&1 || { echo "Missing rustup. Install Rust from https://rustup.rs/."; exit 1; }
	@command -v cargo >/dev/null 2>&1 || { echo "Missing cargo. Install the Rust toolchain pinned in README.md."; exit 1; }
	@command -v rg >/dev/null 2>&1 || { echo "Missing ripgrep (rg). Install it with your system package manager."; exit 1; }
	rustup target add wasm32-unknown-unknown
	$(CARGO_WORK_ENV) cargo fetch --locked

# Install wasm target + candid tools
install-canister-deps:
	rustup toolchain install 1.95.0 || true
	rustup target add wasm32-unknown-unknown
	cargo install candid-extractor ic-wasm twiggy --locked || true

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
	POCKET_IC_BIN="$$(bash scripts/ci/ensure-pocket-ic-bin.sh)" $(CARGO_WORK_ENV) cargo test --workspace --all-targets --exclude canister_demo_rpg --exclude canister_test_sql
	POCKET_IC_BIN="$$(bash scripts/ci/ensure-pocket-ic-bin.sh)" $(CARGO_WORK_ENV) cargo test -p canister_test_sql --lib

wasm-size-report:
	$(CARGO_WORK_ENV) bash scripts/ci/wasm-size-report.sh $(SIZE_REPORT_ARGS)

wasm-audit-report:
	$(CARGO_WORK_ENV) bash scripts/ci/wasm-audit-report.sh $(AUDIT_REPORT_ARGS)

#
# Development commands
#

build: ensure-clean ensure-hooks
	$(CARGO_WORK_ENV) cargo build --release --workspace

check: ensure-hooks fmt-check
	$(MAKE) check-invariants
	$(CARGO_WORK_ENV) cargo check --workspace

clippy: ensure-hooks
	$(MAKE) check-invariants
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
	bash scripts/ci/security-check.sh

check-index-range-spec-invariants:
	bash scripts/ci/check-index-range-spec-invariants.sh

check-layer-authority-invariants:
	bash scripts/ci/check-layer-authority-invariants.sh

check-architecture-text-scan-invariants:
	bash scripts/ci/check-architecture-text-scan-invariants.sh

check-sql-branch-ownership-invariants:
	bash scripts/ci/check-sql-branch-ownership-invariants.sh

check-invariants:
	bash scripts/ci/check-index-range-spec-invariants.sh
	bash scripts/ci/check-layer-authority-invariants.sh
	bash scripts/ci/check-architecture-text-scan-invariants.sh
	bash scripts/ci/check-sql-branch-ownership-invariants.sh
	bash scripts/ci/check-memory-id-invariants.sh
	bash scripts/ci/check-field-projection-invariants.sh

# Run tests in watch mode
test-watch:
	$(CARGO_WORK_ENV) cargo watch -x test

# Build and test everything
all: ensure-clean ensure-hooks clean fmt-check clippy check test build

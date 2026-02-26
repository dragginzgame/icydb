.PHONY: help version current tags patch minor major release \
        test build check clippy fmt fmt-check clean install-dev \
        test-watch all ensure-clean security-check check-versioning \
        ensure-hooks install-hooks check-index-range-spec-invariants \
        check-invariants

# in case we need this
CARGO_ENV :=

# Check for clean git state
ensure-clean:
	@if ! git diff-index --quiet HEAD --; then \
		echo "🚨 Working directory not clean! Please commit or stash your changes."; \
		exit 1; \
	fi

# Default target
help:
	@echo "Available commands:"
	@echo ""
	@echo "Setup / Installation:"
	@echo "  install-all      Install both dev and canister dependencies"
	@echo "  install-dev      Install Rust development dependencies"
	@echo "  install-canister-deps  Install Wasm target + candid tools"
	@echo "  install-hooks    Configure git hooks"
	@echo ""
	@echo "Version Management:"
	@echo "  version          Show current version"
	@echo "  tags             List available git tags"
	@echo "  patch            Bump patch version (0.0.x)"
	@echo "  minor            Bump minor version (0.x.0)"
	@echo "  major            Bump major version (x.0.0)"
	@echo "  release          CI-driven release (local target is no-op)"
	@echo ""
	@echo "Development:"
	@echo "  test             Run all tests"
	@echo "  build            Build all crates"
	@echo "  check            Run cargo check"
	@echo "  clippy           Run clippy checks"
	@echo "  fmt              Format code"
	@echo "  fmt-check        Check formatting"
	@echo "  clean            Clean build artifacts"
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

#
# Installing
#

# Install everything (dev + canister deps)
install-all: install-dev install-canister-deps install-hooks
	@echo "✅ All development and canister dependencies installed"

# Install Rust development tooling
install-dev:
	$(CARGO_ENV) cargo install cargo-watch --locked || true
	$(CARGO_ENV) cargo install cargo-edit --locked || true
	$(CARGO_ENV) cargo install cargo-get cargo-sort cargo-sort-derives --locked || true

# Install wasm target + candid tools
install-canister-deps:
	rustup toolchain install 1.93.1 || true
	rustup target add wasm32-unknown-unknown
	$(CARGO_ENV) cargo install candid-extractor ic-wasm --locked || true

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
# Version management (always format first)
#

version:
	@$(CARGO_ENV) cargo get workspace.package.version

tags:
	@git tag --sort=-version:refname | head -10

patch: ensure-clean fmt
	@scripts/ci/bump-version.sh patch

minor: ensure-clean fmt
	@scripts/ci/bump-version.sh minor

major: ensure-clean fmt
	@scripts/ci/bump-version.sh major

release: ensure-clean
	@echo "Release handled by CI on tag push"


#
# Tests
#

test: clippy test-unit

test-unit:
	$(CARGO_ENV) cargo test --workspace --all-targets --verbose

test-canisters:
	@echo "Skipping canister tests (disabled)"

#
# Development commands
#

build: ensure-clean ensure-hooks
	$(CARGO_ENV) cargo build --release --workspace

check: ensure-hooks fmt-check
	$(MAKE) check-invariants
	$(CARGO_ENV) cargo check --workspace

clippy: ensure-hooks
	$(MAKE) check-invariants
	$(CARGO_ENV) cargo clippy --workspace --all-targets -- -D warnings

fmt: ensure-hooks
	$(CARGO_ENV) cargo sort --workspace
	$(CARGO_ENV) cargo sort-derives
	$(CARGO_ENV) cargo fmt --all

fmt-check: ensure-hooks
	$(CARGO_ENV) cargo sort --workspace --check
	$(CARGO_ENV) cargo sort-derives --check
	$(CARGO_ENV) cargo fmt --all -- --check

clean:
	$(CARGO_ENV) cargo clean


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

check-invariants:
	bash scripts/ci/check-index-range-spec-invariants.sh
	bash scripts/ci/check-memory-id-invariants.sh
	bash scripts/ci/check-field-projection-invariants.sh

# Run tests in watch mode
test-watch:
	$(CARGO_ENV) cargo watch -x test

# Build and test everything
all: ensure-clean ensure-hooks clean fmt-check clippy check test build

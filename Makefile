.PHONY: help version tags patch minor major release publish \
        test build check clippy fmt fmt-check clean install install-dev install-env update-dev ensure-python3 \
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
	@echo "  install-env      Bootstrap a fresh Ubuntu development environment"
	@echo "  install-all      Install both dev and canister dependencies"
	@echo "  install-dev      Install development dependencies and ensure python/python3"
	@echo "  update-dev       Update development tools and ensure python/python3"
	@echo "  install-canister-deps  Install Wasm target + candid tools"
	@echo "  install-hooks    Configure git hooks"
	@echo ""
	@echo "Version Management:"
	@echo "  version          Show current version"
	@echo "  tags             List available git tags"
	@echo "  patch            Run tests, then bump patch version (0.0.x)"
	@echo "  minor            Run tests, then bump minor version (0.x.0)"
	@echo "  major            Run tests, then bump major version (x.0.0)"
	@echo "  release          CI-driven release (local target is no-op)"
	@echo "  publish          Publish workspace crates to crates.io in dependency order"
	@echo ""
	@echo "Development:"
	@echo "  test             Run all tests"
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
	@echo "  WASM_SQL_VARIANTS=both make wasm-size-report      # Build SQL-on and SQL-off variants for the wasm audit matrix"
	@echo "  WASM_CANISTER_NAME=ten_complex make wasm-size-report # Build one specific wasm audit canister"

#
# Installing
#

# Install the developer CLI as `icydb` into cargo's normal bin directory.
install:
	cargo install --path "$(ROOT_DIR)/crates/icydb-cli" --bin icydb --locked --force

# Bootstrap a fresh Ubuntu development environment
install-env:
	sudo apt -y update && sudo apt -y upgrade
	sudo apt -y install build-essential ntp ntpdate cmake curl wget libssl-dev pkg-config ripgrep python3 python-is-python3
	sudo apt -y install speedtest-cli fdupes tree cloc
	sudo apt -y install valgrind
	sudo apt -y install binaryen wabt
	sudo apt -y install jq
	sudo apt -y install linux-tools-common linux-tools-generic linux-headers-generic
	sudo apt -y autoremove
	sudo ntpdate ntp.ubuntu.com || true
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
	PATH="$$HOME/.cargo/bin:$$PATH" rustup toolchain install beta
	PATH="$$HOME/.cargo/bin:$$PATH" rustup toolchain install nightly
	PATH="$$HOME/.cargo/bin:$$PATH" rustup target add wasm32-unknown-unknown
	sh -ci "$$(curl -fsSL https://internetcomputer.org/install.sh)"
	mkdir -p "$$HOME/bin"
	wget -O "$$HOME/bin/didc" https://github.com/dfinity/candid/releases/download/2025-12-18/didc-linux64
	chmod +x "$$HOME/bin/didc"
	cd "$(ROOT_DIR)" && \
		wget -O idl2json_cli-x86_64-unknown-linux-musl.tar.gz https://github.com/dfinity/idl2json/releases/download/v0.10.1/idl2json_cli-x86_64-unknown-linux-musl.tar.gz && \
		tar -xzf idl2json_cli-x86_64-unknown-linux-musl.tar.gz && \
		rm -f idl2json_cli-x86_64-unknown-linux-musl.tar.gz && \
		mv -f ./idl2json "$$HOME/bin/idl2json" && \
		chmod +x "$$HOME/bin/idl2json" && \
		mv -f ./yaml2candid "$$HOME/bin/yaml2candid" && \
		chmod +x "$$HOME/bin/yaml2candid"
	wget -O "$$HOME/bin/quill" https://github.com/dfinity/quill/releases/download/v0.5.4/quill-linux-x86_64
	chmod +x "$$HOME/bin/quill"
	PATH="$$HOME/.cargo/bin:$$HOME/bin:$$HOME/.local/share/dfx/bin:$$PATH" $(MAKE) --no-print-directory update-dev

# Ensure both `python3` and the `python` alias exist for repo scripts and local tooling.
ensure-python3:
	@if command -v python3 >/dev/null 2>&1 && command -v python >/dev/null 2>&1; then \
		echo "✅ python3 available: $$(python3 --version 2>/dev/null)"; \
		echo "✅ python available: $$(python --version 2>/dev/null)"; \
	elif command -v apt-get >/dev/null 2>&1; then \
		sudo apt -y update && sudo apt -y install python3 python-is-python3; \
	else \
		echo "python/python3 not found. Install them manually or run make install-env on Ubuntu."; \
		exit 1; \
	fi

# Install everything (dev + canister deps)
install-all: install-dev install-canister-deps install-hooks
	@echo "✅ All development and canister dependencies installed"

# Install Rust development tooling
install-dev: ensure-python3
	cargo install cargo-watch --locked || true
	cargo install cargo-edit --locked || true
	cargo install cargo-get cargo-sort cargo-sort-derives ripgrep --locked || true

# Update development tooling and dependencies
update-dev: ensure-python3
	rustup update
	cargo install \
		cargo-audit cargo-bloat cargo-deny cargo-expand cargo-machete \
		cargo-llvm-lines cargo-sort cargo-tarpaulin cargo-sort-derives \
		ripgrep \
		candid-extractor ic-wasm
	cargo audit
	cargo update --verbose
	dfxvm self update

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

patch: ensure-clean fmt test
	@$(CARGO_WORK_ENV) scripts/ci/bump-version.sh patch

minor: ensure-clean fmt test
	@$(CARGO_WORK_ENV) scripts/ci/bump-version.sh minor

major: ensure-clean fmt test
	@$(CARGO_WORK_ENV) scripts/ci/bump-version.sh major

release: ensure-clean
	@echo "Release handled by CI on tag push"

publish: ensure-clean fmt-check clippy check
	$(CARGO_WORK_ENV) bash scripts/ci/publish-workspace.sh


#
# Tests
#

test: clippy test-unit

test-unit:
	POCKET_IC_BIN="$$(bash scripts/ci/ensure-pocket-ic-bin.sh)" $(CARGO_WORK_ENV) cargo test --workspace --all-targets --exclude canister_demo_rpg --exclude canister_test_sql
	POCKET_IC_BIN="$$(bash scripts/ci/ensure-pocket-ic-bin.sh)" $(CARGO_WORK_ENV) cargo test -p canister_test_sql --lib

wasm-size-report:
	$(CARGO_WORK_ENV) bash scripts/ci/wasm-size-report.sh

wasm-audit-report:
	$(CARGO_WORK_ENV) bash scripts/ci/wasm-audit-report.sh

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

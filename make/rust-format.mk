# Opt-in single-root-workspace formatting; include make/tools.mk first.
# Shared companions: make/tools.mk scripts/ci/check-format-tools.sh scripts/ci/run-formatting.sh make/execution.mk
_shared_format_default_goal := $(.DEFAULT_GOAL)
include $(dir $(lastword $(MAKEFILE_LIST)))execution.mk
FORMAT_CARGO ?= cargo

.PHONY: format-tools-check fmt fmt-check
format-tools-check fmt fmt-check: export CARGO_NET_OFFLINE := true
format-tools-check fmt fmt-check: export RUSTUP_AUTO_INSTALL := 0

format-tools-check:
	+@. "$(HOST_TOOL_VERSIONS)" && bash "$(SHARED_TOOLING_ROOT)/scripts/ci/check-format-tools.sh" "$${SHARED_TOOLING_CARGO_SORT_VERSION:?}" "$(FORMAT_CARGO)"

fmt: format-tools-check
	+@bash "$(SHARED_TOOLING_ROOT)/scripts/ci/run-formatting.sh" --write \
		bash -ec '"$$1" sort --workspace; "$$1" fmt --all' -- "$(FORMAT_CARGO)"

fmt-check: format-tools-check
	+@bash "$(SHARED_TOOLING_ROOT)/scripts/ci/run-formatting.sh" --check \
		bash -ec '"$$1" sort --workspace --check; "$$1" fmt --all -- --check' -- "$(FORMAT_CARGO)"

.DEFAULT_GOAL := $(_shared_format_default_goal)

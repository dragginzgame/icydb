# Rust formatting defaults and consumer adapters; include make/tools.mk first.
# Shared companions: make/tools.mk scripts/ci/check-format-tools.sh scripts/ci/run-formatting.sh make/execution.mk
_shared_format_default_goal := $(.DEFAULT_GOAL)
include $(dir $(lastword $(MAKEFILE_LIST)))execution.mk
FORMAT_CARGO ?= cargo
# A custom policy replaces both complete commands, never the shared recipes.
# Commands are reviewed Makefile source, not externally supplied shell data.
ifneq ($(origin FORMAT_WRITE_COMMAND),undefined)
ifeq ($(origin FORMAT_CHECK_COMMAND),undefined)
$(error Define both FORMAT_WRITE_COMMAND and FORMAT_CHECK_COMMAND)
endif
endif
ifneq ($(origin FORMAT_CHECK_COMMAND),undefined)
ifeq ($(origin FORMAT_WRITE_COMMAND),undefined)
$(error Define both FORMAT_WRITE_COMMAND and FORMAT_CHECK_COMMAND)
endif
endif
FORMAT_WRITE_COMMAND ?= bash -ec '"$$1" sort --workspace --grouped; "$$1" fmt --all' -- "$(FORMAT_CARGO)"
FORMAT_CHECK_COMMAND ?= bash -ec '"$$1" sort --workspace --grouped --check; "$$1" fmt --all -- --check' -- "$(FORMAT_CARGO)"
ifeq ($(strip $(FORMAT_WRITE_COMMAND)),)
$(error FORMAT_WRITE_COMMAND must not be empty)
endif
ifeq ($(strip $(FORMAT_CHECK_COMMAND)),)
$(error FORMAT_CHECK_COMMAND must not be empty)
endif

.PHONY: format-tools-check fmt fmt-check
format-tools-check fmt fmt-check: export CARGO_NET_OFFLINE := true
format-tools-check fmt fmt-check: export RUSTUP_AUTO_INSTALL := 0

format-tools-check:
	+@. "$(RUST_TOOL_VERSIONS)" && bash "$(SHARED_TOOLING_ROOT)/scripts/ci/check-format-tools.sh" "$${SHARED_TOOLING_CARGO_SORT_VERSION:?}" "$(FORMAT_CARGO)"

fmt: format-tools-check
	+@bash "$(SHARED_TOOLING_ROOT)/scripts/ci/run-formatting.sh" --write \
		$(FORMAT_WRITE_COMMAND)

fmt-check: format-tools-check
	+@bash "$(SHARED_TOOLING_ROOT)/scripts/ci/run-formatting.sh" --check \
		$(FORMAT_CHECK_COMMAND)

.DEFAULT_GOAL := $(_shared_format_default_goal)

# Rust formatting defaults and consumer adapters; include make/tools.mk first.
# Shared companions: make/tools.mk scripts/ci/check-format-tools.sh scripts/ci/run-formatting.sh scripts/ci/run-rust-formatting.pl scripts/ci/tool-catalog-records.pl make/execution.mk
_shared_format_default_goal := $(.DEFAULT_GOAL)
include $(dir $(lastword $(MAKEFILE_LIST)))execution.mk
FORMAT_CARGO ?= cargo
FORMAT_RUST_ARGS ?=
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
_shared_format_custom := $(if $(filter undefined,$(origin FORMAT_WRITE_COMMAND)),,yes)
FORMAT_WRITE_COMMAND ?= perl "$(SHARED_TOOLING_ROOT)/scripts/ci/run-rust-formatting.pl" --write --cargo "$(FORMAT_CARGO)" $(FORMAT_RUST_ARGS)
FORMAT_CHECK_COMMAND ?= perl "$(SHARED_TOOLING_ROOT)/scripts/ci/run-rust-formatting.pl" --check --cargo "$(FORMAT_CARGO)" $(FORMAT_RUST_ARGS)
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
	+@record="$$(perl "$(SHARED_TOOLING_ROOT)/scripts/ci/tool-catalog-records.pl" rust "$(RUST_TOOL_VERSIONS)" CARGO_SORT)" || exit $$?; \
		version="$$(printf '%s\n' "$$record" | cut -f2)"; \
		bash "$(SHARED_TOOLING_ROOT)/scripts/ci/check-format-tools.sh" "$$version" "$(FORMAT_CARGO)"

fmt: format-tools-check
	+@$(if $(_shared_format_custom),bash "$(SHARED_TOOLING_ROOT)/scripts/ci/run-formatting.sh" --write) $(FORMAT_WRITE_COMMAND)

fmt-check: format-tools-check
	+@$(if $(_shared_format_custom),bash "$(SHARED_TOOLING_ROOT)/scripts/ci/run-formatting.sh" --check) $(FORMAT_CHECK_COMMAND)

.DEFAULT_GOAL := $(_shared_format_default_goal)

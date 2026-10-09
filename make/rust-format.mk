# Opt-in single-root-workspace formatting; include make/tools.mk first.
# Shared companions: make/tools.mk scripts/ci/check-format-tools.sh
_shared_format_default_goal := $(.DEFAULT_GOAL)
FORMAT_CARGO ?= cargo

.PHONY: format-tools-check fmt fmt-check
format-tools-check fmt fmt-check: export CARGO_NET_OFFLINE := true
format-tools-check fmt fmt-check: export RUSTUP_AUTO_INSTALL := 0

format-tools-check:
	@. "$(HOST_TOOL_VERSIONS)" && bash "$(SHARED_TOOLING_ROOT)/scripts/ci/check-format-tools.sh" "$${SHARED_TOOLING_CARGO_SORT_VERSION:?}" "$(FORMAT_CARGO)"

fmt: format-tools-check
	env "$(FORMAT_CARGO)" sort --workspace
	env "$(FORMAT_CARGO)" fmt --all

fmt-check: format-tools-check
	env "$(FORMAT_CARGO)" sort --workspace --check
	env "$(FORMAT_CARGO)" fmt --all -- --check

.DEFAULT_GOAL := $(_shared_format_default_goal)

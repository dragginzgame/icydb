# Include once from the consumer Makefile after any local path overrides.
# Shared companions: make/execution.mk scripts/ci/check-make-execution.sh scripts/dev/install-host-tools.sh scripts/dev/install-ic-tools.sh scripts/dev/install-rust-tools.sh scripts/dev/cloc.sh scripts/ci/run-validation-targets.sh
# TOKO_TOOLING_ROOT must name a reviewed local snapshot, never a sibling checkout.
# Preserve the consumer's default goal, including when it is declared after us.
_toko_tooling_default_goal := $(.DEFAULT_GOAL)
include $(dir $(lastword $(MAKEFILE_LIST)))execution.mk

TOKO_TOOLING_ROOT ?= $(CURDIR)
HOST_TOOL_VERSIONS ?= $(TOKO_TOOLING_ROOT)/ci/tool-versions.env
RUST_TOOL_VERSIONS ?= $(TOKO_TOOLING_ROOT)/ci/tool-versions.env
IC_TOOL_PINS ?= $(TOKO_TOOLING_ROOT)/ci/ic-tools.tsv
# Products select only the executables their callers need; there is no default set.
IC_TOOLS ?=
# Product-owned tool targets run in this order after the complete common set.
LOCAL_TOOL_INSTALL_TARGETS ?=
LOCAL_TOOL_CHECK_TARGETS ?=
CLOC_REPORT ?= $(TOKO_TOOLING_ROOT)/scripts/dev/cloc.sh
CLOC_ROOT ?= $(CURDIR)
CLOC_MANIFEST ?=
CLOC_DEPTH ?=
export PATH := $(CURDIR)/.tools/host/bin:$(CURDIR)/.tools/rust/common/bin:$(PATH)

.PHONY: install-tools tools-check install-host-tools host-tools-check install-ic-tools ic-tools-check cloc
.PHONY: install-rust-tools rust-tools-check
install-tools tools-check tools-report install-host-tools host-tools-check install-ic-tools ic-tools-check install-rust-tools rust-tools-check cloc: | toko-execution-check

install-tools:
ifneq ($(filter install-ic-tools,$(LOCAL_TOOL_INSTALL_TARGETS)),)
	bash "$(TOKO_TOOLING_ROOT)/scripts/dev/install-ic-tools.sh" --consumer "$(CURDIR)" --pins "$(IC_TOOL_PINS)" $(foreach tool,$(IC_TOOLS),--tool "$(tool)") --preflight
endif
	+bash "$(TOKO_TOOLING_ROOT)/scripts/dev/install-rust-tools.sh" --consumer "$(CURDIR)" --versions "$(RUST_TOOL_VERSIONS)" --preflight
	+$(MAKE) --no-print-directory install-host-tools
	+$(MAKE) --no-print-directory install-rust-tools
	+@for target in $(LOCAL_TOOL_INSTALL_TARGETS); do \
		$(MAKE) --no-print-directory "$$target" || exit "$$?"; done

tools-check:
	+$(MAKE) --no-print-directory host-tools-check
	+$(MAKE) --no-print-directory rust-tools-check
	+@for target in $(LOCAL_TOOL_CHECK_TARGETS); do \
		$(MAKE) --no-print-directory "$$target" || exit "$$?"; done

# Diagnostic only: run every existing offline checker and retain all failures.
# Release/setup admission continues to use the fail-fast tools-check above.
.PHONY: tools-report
tools-report:
	+@VALIDATION_REPOSITORY_ROOT="$(CURDIR)" bash "$(TOKO_TOOLING_ROOT)/scripts/ci/run-validation-targets.sh" \
		host-tools-check rust-tools-check $(LOCAL_TOOL_CHECK_TARGETS)

install-host-tools:
	bash "$(TOKO_TOOLING_ROOT)/scripts/dev/install-host-tools.sh" --consumer "$(CURDIR)" --versions "$(HOST_TOOL_VERSIONS)"

host-tools-check:
	bash "$(TOKO_TOOLING_ROOT)/scripts/dev/install-host-tools.sh" --consumer "$(CURDIR)" --versions "$(HOST_TOOL_VERSIONS)" --check

install-ic-tools:
	bash "$(TOKO_TOOLING_ROOT)/scripts/dev/install-ic-tools.sh" --consumer "$(CURDIR)" --pins "$(IC_TOOL_PINS)" $(foreach tool,$(IC_TOOLS),--tool "$(tool)")

ic-tools-check:
	bash "$(TOKO_TOOLING_ROOT)/scripts/dev/install-ic-tools.sh" --consumer "$(CURDIR)" --pins "$(IC_TOOL_PINS)" $(foreach tool,$(IC_TOOLS),--tool "$(tool)") --check

install-rust-tools:
	+bash "$(TOKO_TOOLING_ROOT)/scripts/dev/install-rust-tools.sh" --consumer "$(CURDIR)" --versions "$(RUST_TOOL_VERSIONS)"

rust-tools-check:
	+bash "$(TOKO_TOOLING_ROOT)/scripts/dev/install-rust-tools.sh" --consumer "$(CURDIR)" --versions "$(RUST_TOOL_VERSIONS)" --check

cloc:
	+@if [ -n "$(CLOC_MANIFEST)" ]; then \
		bash "$(TOKO_TOOLING_ROOT)/scripts/dev/cloc.sh" $(if $(strip $(CLOC_DEPTH)),-d "$(CLOC_DEPTH)") --manifest "$(CLOC_MANIFEST)" "$(CLOC_ROOT)"; \
	else bash "$(CLOC_REPORT)" $(if $(strip $(CLOC_DEPTH)),-d "$(CLOC_DEPTH)") "$(CLOC_ROOT)"; fi

.DEFAULT_GOAL := $(_toko_tooling_default_goal)

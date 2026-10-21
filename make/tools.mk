# Include once from the consumer Makefile after any local path overrides.
# Shared companions: make/execution.mk scripts/ci/check-make-execution.sh scripts/dev/install-host-tools.sh scripts/dev/install-ic-tools.sh scripts/dev/install-rust-tools.sh scripts/dev/cloc.sh
# SHARED_TOOLING_ROOT must name a reviewed local snapshot, never a sibling checkout.
# Preserve the consumer's default goal, including when it is declared after us.
_shared_tooling_default_goal := $(.DEFAULT_GOAL)
include $(dir $(lastword $(MAKEFILE_LIST)))execution.mk

SHARED_TOOLING_ROOT ?= $(CURDIR)
HOST_TOOL_VERSIONS ?= $(SHARED_TOOLING_ROOT)/ci/tool-versions.env
RUST_TOOL_VERSIONS ?= $(SHARED_TOOLING_ROOT)/ci/tool-versions.env
IC_TOOL_PINS ?= $(SHARED_TOOLING_ROOT)/ci/ic-tools.tsv
# Products select only the executables their callers need; there is no default set.
IC_TOOLS ?=
# Product-owned tool targets run in this order after the complete common set.
LOCAL_TOOL_INSTALL_TARGETS ?=
LOCAL_TOOL_CHECK_TARGETS ?=
CLOC_REPORT ?= $(SHARED_TOOLING_ROOT)/scripts/dev/cloc.sh
CLOC_ROOT ?= $(CURDIR)
CLOC_MANIFEST ?=
export PATH := $(CURDIR)/.tools/host/bin:$(CURDIR)/.tools/rust/common/bin:$(PATH)

.PHONY: install-tools tools-check install-host-tools host-tools-check install-ic-tools ic-tools-check cloc
.PHONY: install-rust-tools rust-tools-check

install-tools:
ifneq ($(filter install-ic-tools,$(LOCAL_TOOL_INSTALL_TARGETS)),)
	bash "$(SHARED_TOOLING_ROOT)/scripts/dev/install-ic-tools.sh" --consumer "$(CURDIR)" --pins "$(IC_TOOL_PINS)" $(foreach tool,$(IC_TOOLS),--tool "$(tool)") --preflight
endif
	+bash "$(SHARED_TOOLING_ROOT)/scripts/dev/install-rust-tools.sh" --consumer "$(CURDIR)" --versions "$(RUST_TOOL_VERSIONS)" --preflight
	+$(MAKE) --no-print-directory install-host-tools
	+$(MAKE) --no-print-directory install-rust-tools
	+@for target in $(LOCAL_TOOL_INSTALL_TARGETS); do \
		$(MAKE) --no-print-directory "$$target" || exit "$$?"; done

tools-check:
	+$(MAKE) --no-print-directory host-tools-check
	+$(MAKE) --no-print-directory rust-tools-check
	+@for target in $(LOCAL_TOOL_CHECK_TARGETS); do \
		$(MAKE) --no-print-directory "$$target" || exit "$$?"; done

install-host-tools:
	bash "$(SHARED_TOOLING_ROOT)/scripts/dev/install-host-tools.sh" --consumer "$(CURDIR)" --versions "$(HOST_TOOL_VERSIONS)"

host-tools-check:
	bash "$(SHARED_TOOLING_ROOT)/scripts/dev/install-host-tools.sh" --consumer "$(CURDIR)" --versions "$(HOST_TOOL_VERSIONS)" --check

install-ic-tools:
	bash "$(SHARED_TOOLING_ROOT)/scripts/dev/install-ic-tools.sh" --consumer "$(CURDIR)" --pins "$(IC_TOOL_PINS)" $(foreach tool,$(IC_TOOLS),--tool "$(tool)")

ic-tools-check:
	bash "$(SHARED_TOOLING_ROOT)/scripts/dev/install-ic-tools.sh" --consumer "$(CURDIR)" --pins "$(IC_TOOL_PINS)" $(foreach tool,$(IC_TOOLS),--tool "$(tool)") --check

install-rust-tools:
	+bash "$(SHARED_TOOLING_ROOT)/scripts/dev/install-rust-tools.sh" --consumer "$(CURDIR)" --versions "$(RUST_TOOL_VERSIONS)"

rust-tools-check:
	+bash "$(SHARED_TOOLING_ROOT)/scripts/dev/install-rust-tools.sh" --consumer "$(CURDIR)" --versions "$(RUST_TOOL_VERSIONS)" --check

cloc:
	+@if [ -n "$(CLOC_MANIFEST)" ]; then \
		bash "$(SHARED_TOOLING_ROOT)/scripts/dev/cloc.sh" --manifest "$(CLOC_MANIFEST)" "$(CURDIR)"; \
	else bash "$(CLOC_REPORT)" "$(CLOC_ROOT)"; fi

.DEFAULT_GOAL := $(_shared_tooling_default_goal)

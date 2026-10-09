# Include once from the consumer Makefile after any local path overrides.
# SHARED_TOOLING_ROOT must name a reviewed local snapshot, never a sibling checkout.
# Preserve the consumer's default goal, including when it is declared after us.
_shared_tooling_default_goal := $(.DEFAULT_GOAL)

SHARED_TOOLING_ROOT ?= $(CURDIR)
HOST_TOOL_VERSIONS ?= $(CURDIR)/ci/tool-versions.env
RUST_TOOL_VERSIONS ?= $(HOST_TOOL_VERSIONS)
IC_TOOL_PINS ?= $(CURDIR)/ci/ic-tools.tsv
CLOC_REPORT ?= $(SHARED_TOOLING_ROOT)/scripts/dev/cloc.sh
CLOC_ROOT ?= $(CURDIR)
CLOC_MANIFEST ?=
CLOC_PARENT ?= $(CURDIR)/..
export PATH := $(CURDIR)/.tools/host/bin:$(CURDIR)/.tools/ic/bin:$(CURDIR)/.tools/rust/bin:$(PATH)

.PHONY: install-tools tools-check install-host-tools host-tools-check install-ic-tools ic-tools-check cloc cloc-tooling
.PHONY: install-rust-tools rust-tools-check

install-tools:
	+$(MAKE) --no-print-directory install-host-tools
	+$(MAKE) --no-print-directory install-ic-tools

tools-check:
	+$(MAKE) --no-print-directory host-tools-check
	+$(MAKE) --no-print-directory ic-tools-check

install-host-tools:
	bash "$(SHARED_TOOLING_ROOT)/scripts/dev/install-host-tools.sh" --consumer "$(CURDIR)" --versions "$(HOST_TOOL_VERSIONS)" --with-ripgrep --with-cloc

host-tools-check:
	bash "$(SHARED_TOOLING_ROOT)/scripts/dev/install-host-tools.sh" --consumer "$(CURDIR)" --versions "$(HOST_TOOL_VERSIONS)" --with-ripgrep --with-cloc --check

install-ic-tools:
	bash "$(SHARED_TOOLING_ROOT)/scripts/dev/install-ic-tools.sh" --consumer "$(CURDIR)" --pins "$(IC_TOOL_PINS)"

ic-tools-check:
	bash "$(SHARED_TOOLING_ROOT)/scripts/dev/install-ic-tools.sh" --consumer "$(CURDIR)" --pins "$(IC_TOOL_PINS)" --check

install-rust-tools:
	bash "$(SHARED_TOOLING_ROOT)/scripts/dev/install-rust-tools.sh" --consumer "$(CURDIR)" --versions "$(RUST_TOOL_VERSIONS)"

rust-tools-check:
	bash "$(SHARED_TOOLING_ROOT)/scripts/dev/install-rust-tools.sh" --consumer "$(CURDIR)" --versions "$(RUST_TOOL_VERSIONS)" --check

cloc:
	@if [ -n "$(CLOC_MANIFEST)" ]; then \
		bash "$(SHARED_TOOLING_ROOT)/scripts/dev/cloc.sh" --manifest "$(CLOC_MANIFEST)" "$(CURDIR)"; \
	else bash "$(CLOC_REPORT)" "$(CLOC_ROOT)"; fi

cloc-tooling:
	@test -f "$(SHARED_TOOLING_ROOT)/scripts/dev/cloc-tooling.pl" || { \
		printf '%s\n' 'Fleet tooling reports are optional: run make cloc-tooling in Shared Tooling, or explicitly adopt scripts/dev/cloc-tooling.pl in this snapshot.' >&2; \
		exit 2; }
	@perl "$(SHARED_TOOLING_ROOT)/scripts/dev/cloc-tooling.pl" "$(CLOC_PARENT)"

.DEFAULT_GOAL := $(_shared_tooling_default_goal)

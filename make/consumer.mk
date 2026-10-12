# Common consumer entrypoint; set local inputs before including this snapshot.
# Shared companions: make/tools.mk make/release.mk make/rust-format.mk scripts/dev/install-git-hooks.sh scripts/ci/verify-shared-tooling-snapshot.sh
_shared_consumer_default_goal := $(.DEFAULT_GOAL)
_shared_consumer_make_dir := $(dir $(lastword $(MAKEFILE_LIST)))

# Non-Rust repositories still prepare the complete common executable set.
SHARED_TOOLING_RUST ?= 1
ifneq ($(SHARED_TOOLING_RUST),0)
ifneq ($(SHARED_TOOLING_RUST),1)
$(error SHARED_TOOLING_RUST must be 0 or 1)
endif
endif
SHARED_TOOLING_MANIFEST ?= .shared-tooling.snapshot

include $(_shared_consumer_make_dir)tools.mk
include $(_shared_consumer_make_dir)release.mk

.PHONY: shared-tooling-check
shared-tooling-check:
	@bash "$(SHARED_TOOLING_ROOT)/scripts/ci/verify-shared-tooling-snapshot.sh" \
		--consumer "$(CURDIR)" --manifest "$(SHARED_TOOLING_MANIFEST)"

ifeq ($(SHARED_TOOLING_RUST),1)
include $(_shared_consumer_make_dir)rust-format.mk
.PHONY: install-hooks
install-hooks:
	@bash "$(SHARED_TOOLING_ROOT)/scripts/dev/install-git-hooks.sh" --consumer "$(CURDIR)"
endif

.DEFAULT_GOAL := $(_shared_consumer_default_goal)

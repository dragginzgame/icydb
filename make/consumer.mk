# Common consumer entrypoint; set local inputs before including this snapshot.
# Shared companions: scripts/dev/shared-tooling.sh make/tools.mk make/release.mk make/rust-format.mk make/cargo-publication.mk scripts/dev/install-git-hooks.sh scripts/ci/verify-shared-tooling-snapshot.sh
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
# Optional, one-time ownership proof when migrating an existing Git snapshot.
SHARED_TOOLING_SOURCE ?=
SHARED_TOOLING_VERSION ?=

.PHONY: shared-tooling-status shared-tooling-update shared-tooling-install
shared-tooling-status shared-tooling-update shared-tooling-install:
	@bash "$(SHARED_TOOLING_ROOT)/scripts/dev/shared-tooling.sh" $(patsubst shared-tooling-%,%,$@) \
	  --consumer "$(CURDIR)" --manifest "$(SHARED_TOOLING_MANIFEST)" \
	  $(if $(strip $(SHARED_TOOLING_SOURCE)),--source "$(SHARED_TOOLING_SOURCE)",) \
	  $(if $(strip $(SHARED_TOOLING_VERSION)),--version "$(SHARED_TOOLING_VERSION)")

include $(_shared_consumer_make_dir)tools.mk
include $(_shared_consumer_make_dir)release.mk

.PHONY: shared-tooling-check
shared-tooling-check:
	@bash "$(SHARED_TOOLING_ROOT)/scripts/dev/shared-tooling.sh" check \
	  --consumer "$(CURDIR)" --manifest "$(SHARED_TOOLING_MANIFEST)"

ifeq ($(SHARED_TOOLING_RUST),1)
include $(_shared_consumer_make_dir)rust-format.mk
.PHONY: install-hooks
install-hooks:
	@bash "$(SHARED_TOOLING_ROOT)/scripts/dev/install-git-hooks.sh" --consumer "$(CURDIR)"
endif

# Root tooling selection is admitted automatically before CI or release effects.
ifneq ($(SHARED_TOOLING_PRODUCER),1)
ci release-patch release-minor release-major release-resume release-preflight: shared-tooling-check
endif

.DEFAULT_GOAL := $(_shared_consumer_default_goal)

# Opt in with consumer-owned package policy; existing publication adapters stay local.
ifneq ($(strip $(CARGO_PUBLISH_PACKAGES)),)
include $(_shared_consumer_make_dir)cargo-publication.mk
endif

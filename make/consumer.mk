# Common consumer entrypoint; set local inputs before including this snapshot.
# Shared companions: scripts/dev/toko-tooling.sh make/tools.mk make/release.mk make/rust-format.mk make/cargo-publication.mk scripts/dev/install-git-hooks.sh scripts/ci/verify-toko-tooling-snapshot.sh
_shared_consumer_default_goal := $(.DEFAULT_GOAL)
_shared_consumer_make_dir := $(dir $(lastword $(MAKEFILE_LIST)))

# Non-Rust repositories still prepare the complete common executable set.
TOKO_TOOLING_RUST ?= 1
ifneq ($(TOKO_TOOLING_RUST),0)
ifneq ($(TOKO_TOOLING_RUST),1)
$(error TOKO_TOOLING_RUST must be 0 or 1)
endif
endif
TOKO_TOOLING_MANIFEST ?= .toko-tooling.snapshot
# Optional, one-time ownership proof when migrating an existing Git snapshot.
TOKO_TOOLING_SOURCE ?=
TOKO_TOOLING_VERSION ?=
TOKO_TOOLING_INSTALL_TOOLS ?= 1
ifneq ($(TOKO_TOOLING_INSTALL_TOOLS),0)
ifneq ($(TOKO_TOOLING_INSTALL_TOOLS),1)
$(error TOKO_TOOLING_INSTALL_TOOLS must be 0 or 1)
endif
endif

.PHONY: toko-tooling-status toko-tooling-update toko-tooling-install
toko-tooling-status toko-tooling-update toko-tooling-install:
	@bash "$(TOKO_TOOLING_ROOT)/scripts/dev/toko-tooling.sh" $(patsubst toko-tooling-%,%,$@) \
	  --consumer "$(CURDIR)" --manifest "$(TOKO_TOOLING_MANIFEST)" \
	  $(if $(strip $(TOKO_TOOLING_SOURCE)),--source "$(TOKO_TOOLING_SOURCE)",) \
	  $(if $(strip $(TOKO_TOOLING_VERSION)),--version "$(TOKO_TOOLING_VERSION)") \
	  $(if $(and $(filter toko-tooling-install toko-tooling-update,$@),$(filter 0,$(TOKO_TOOLING_INSTALL_TOOLS))),--snapshot-only)

.PHONY: toko-tooling-preview
toko-tooling-preview:
	@bash "$(TOKO_TOOLING_ROOT)/scripts/dev/toko-tooling.sh" update --preview \
	  --consumer "$(CURDIR)" --manifest "$(TOKO_TOOLING_MANIFEST)" \
	  $(if $(strip $(TOKO_TOOLING_SOURCE)),--source "$(TOKO_TOOLING_SOURCE)")

include $(_shared_consumer_make_dir)tools.mk
include $(_shared_consumer_make_dir)release.mk
toko-tooling-install toko-tooling-update install-hooks: | toko-execution-check

.PHONY: toko-tooling-check
toko-tooling-check:
	@bash "$(TOKO_TOOLING_ROOT)/scripts/dev/toko-tooling.sh" check \
	  --consumer "$(CURDIR)" --manifest "$(TOKO_TOOLING_MANIFEST)"

ifeq ($(TOKO_TOOLING_RUST),1)
include $(_shared_consumer_make_dir)rust-format.mk
.PHONY: install-hooks
install-hooks:
	@bash "$(TOKO_TOOLING_ROOT)/scripts/dev/install-git-hooks.sh" --consumer "$(CURDIR)"
endif

# Root tooling selection is admitted automatically before CI or release effects.
ifneq ($(TOKO_TOOLING_PRODUCER),1)
ci release-patch release-minor release-major release-resume release-preflight: toko-tooling-check
endif

.DEFAULT_GOAL := $(_shared_consumer_default_goal)

# Opt in with consumer-owned package policy; existing publication adapters stay local.
ifneq ($(strip $(CARGO_PUBLISH_PACKAGES)),)
include $(_shared_consumer_make_dir)cargo-publication.mk
endif

# Opt-in publication mechanics; consumers declare eligibility, registry and gates.
# Shared companions: make/execution.mk scripts/ci/cargo-publication-adapter.sh
_shared_publication_default_goal := $(.DEFAULT_GOAL)
include $(dir $(lastword $(MAKEFILE_LIST)))execution.mk
CARGO_PUBLISH_EVIDENCE_ROOT ?= $(CURDIR)/target/publication
_shared_publication_quote = '$(subst ','"'"',$(1))'

.PHONY: publish publish-check
publish publish-check:
	+@CARGO_PUBLISH_PACKAGES=$(call _shared_publication_quote,$(CARGO_PUBLISH_PACKAGES)) \
		CARGO_PUBLISH_SELECTION=$(call _shared_publication_quote,$(if $(filter command line,$(origin PACKAGE)),$(PACKAGE),$(CARGO_PUBLISH_PACKAGES))) \
		CARGO_PUBLISH_REGISTRY=$(call _shared_publication_quote,$(CARGO_PUBLISH_REGISTRY)) \
		CARGO_PUBLISH_VERIFY_TARGETS=$(call _shared_publication_quote,$(CARGO_PUBLISH_VERIFY_TARGETS)) \
		CARGO_PUBLISH_EVIDENCE_ROOT=$(call _shared_publication_quote,$(CARGO_PUBLISH_EVIDENCE_ROOT)) \
		CARGO_PUBLISH_MAKE=$(call _shared_publication_quote,$(MAKE_COMMAND)) \
		CARGO_PUBLISH_MANIFEST_PATH=$(call _shared_publication_quote,$(CARGO_PUBLISH_MANIFEST_PATH)) \
		CARGO_PUBLISH_TARGET_DIR=$(call _shared_publication_quote,$(CARGO_PUBLISH_TARGET_DIR)) \
		CARGO_PUBLISH_FEATURES=$(call _shared_publication_quote,$(CARGO_PUBLISH_FEATURES)) \
		CARGO_PUBLISH_ALL_FEATURES=$(call _shared_publication_quote,$(or $(CARGO_PUBLISH_ALL_FEATURES),0)) \
		CARGO_PUBLISH_NO_DEFAULT_FEATURES=$(call _shared_publication_quote,$(or $(CARGO_PUBLISH_NO_DEFAULT_FEATURES),0)) \
		CARGO_PUBLISH_CHECK_ALLOW_DIRTY=$(call _shared_publication_quote,$(or $(CARGO_PUBLISH_CHECK_ALLOW_DIRTY),0)) \
		bash $(call _shared_publication_quote,$(SHARED_TOOLING_ROOT)/scripts/ci/cargo-publication-adapter.sh) $(if $(filter publish-check,$@),check,publish)

.DEFAULT_GOAL := $(_shared_publication_default_goal)

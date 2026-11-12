# Explicit Cargo updates for the root and qualification workspaces.
# Shared companions: make/execution.mk scripts/dev/cargo-dependencies.pl
_dependency_default_goal := $(.DEFAULT_GOAL)
include $(dir $(lastword $(MAKEFILE_LIST)))execution.mk
DEPENDENCY_CONSUMER ?= $(CURDIR)
DEPENDENCY_PACKAGE ?=
DEPENDENCY_PRECISE ?=
DEPENDENCY_MANIFEST ?=
_dependency_quote = '$(subst ','"'"',$(1))'

.PHONY: deps-update
deps-update: | toko-execution-check
deps-update:
	@perl $(call _dependency_quote,$(TOKO_TOOLING_ROOT)/scripts/dev/cargo-dependencies.pl) update \
	  --consumer $(call _dependency_quote,$(DEPENDENCY_CONSUMER)) \
	  $(if $(strip $(DEPENDENCY_PACKAGE)),--package $(call _dependency_quote,$(DEPENDENCY_PACKAGE))) \
	  $(if $(strip $(DEPENDENCY_PRECISE)),--precise $(call _dependency_quote,$(DEPENDENCY_PRECISE))) \
	  $(if $(strip $(DEPENDENCY_MANIFEST)),--manifest $(call _dependency_quote,$(DEPENDENCY_MANIFEST)))

.DEFAULT_GOAL := $(_dependency_default_goal)

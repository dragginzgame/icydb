# Standard entrypoints only; consumer metadata, validation and delivery policy stay local.
# Shared companions: scripts/ci/run-release.sh make/execution.mk
_shared_release_default_goal := $(.DEFAULT_GOAL)
SHARED_TOOLING_ROOT ?= $(CURDIR)
include $(dir $(lastword $(MAKEFILE_LIST)))execution.mk
RELEASE_REMOTE ?= origin
RELEASE_BRANCH ?= main

ifneq ($(word 2,$(filter release-patch release-minor release-major release-resume,$(MAKECMDGOALS))),)
$(error Select exactly one release target)
endif

.PHONY: release-patch release-minor release-major release-resume
release-patch release-minor release-major:
	+@bash "$(SHARED_TOOLING_ROOT)/scripts/ci/run-release.sh" "$(@:release-%=%)" "$(RELEASE_REMOTE)" "$(RELEASE_BRANCH)"

release-resume:
	+@bash "$(SHARED_TOOLING_ROOT)/scripts/ci/run-release.sh" resume "$(VERSION)" "$(RELEASE_REMOTE)" "$(RELEASE_BRANCH)"

.DEFAULT_GOAL := $(_shared_release_default_goal)

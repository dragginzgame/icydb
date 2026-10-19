# Standard entrypoints only; consumer metadata, validation and delivery policy stay local.
# Shared companions: scripts/ci/run-release.sh scripts/ci/check-release-delivery.sh make/execution.mk
_shared_release_default_goal := $(.DEFAULT_GOAL)
SHARED_TOOLING_ROOT ?= $(CURDIR)
_shared_release_delivery_check := $(dir $(lastword $(MAKEFILE_LIST)))../scripts/ci/check-release-delivery.sh
include $(dir $(lastword $(MAKEFILE_LIST)))execution.mk
RELEASE_REMOTE ?= origin
RELEASE_BRANCH ?= main
# Optional consumer-owned verification after a successful exact-version resume.
# This is a check, not a second delivery or publication command.
RELEASE_RESUME_CHECK_COMMAND ?= :
RELEASE_DELIVERY_POLICY ?=

# Consumers can reuse this canonical interface roster without repeating hook names.
override SHARED_RELEASE_TARGETS := release-patch release-minor release-major release-resume release-version release-preflight release-verify release-prepare-version release-prepared-check release-files release-commit-check release-committed-check release-tagged-check release-push-check release-merged-preflight
ifneq ($(filter $(SHARED_RELEASE_TARGETS),$(MAKECMDGOALS)),)
ifneq ($(RELEASE_DELIVERY_POLICY),)
ifneq ($(shell bash '$(subst ','"'"',$(_shared_release_delivery_check))' '$(subst ','"'"',$(RELEASE_DELIVERY_POLICY))' '$(subst ','"'"',$(if $(filter undefined,$(origin RELEASE_DELIVERY)),direct,$(RELEASE_DELIVERY)))' && printf admitted),admitted)
$(error Consumer release delivery policy refused this command)
endif
endif
endif

ifneq ($(word 2,$(filter release-patch release-minor release-major release-resume,$(MAKECMDGOALS))),)
$(error Select exactly one release target)
endif

.PHONY: release-patch release-minor release-major release-resume
release-patch release-minor release-major:
	+@bash "$(SHARED_TOOLING_ROOT)/scripts/ci/run-release.sh" "$(@:release-%=%)" "$(RELEASE_REMOTE)" "$(RELEASE_BRANCH)"

release-resume:
	+@bash "$(SHARED_TOOLING_ROOT)/scripts/ci/run-release.sh" resume "$(VERSION)" "$(RELEASE_REMOTE)" "$(RELEASE_BRANCH)"
	+@$(RELEASE_RESUME_CHECK_COMMAND)

.DEFAULT_GOAL := $(_shared_release_default_goal)

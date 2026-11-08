# Standard entrypoints only; consumer metadata, validation and delivery policy stay local.
# Shared companions: scripts/ci/run-release.sh scripts/ci/run-release-batch.pl scripts/ci/check-release-delivery.sh make/execution.mk scripts/dev/clean-empty-source-dirs.pl
_shared_release_default_goal := $(.DEFAULT_GOAL)
SHARED_TOOLING_ROOT ?= $(CURDIR)
_shared_release_delivery_check := $(dir $(lastword $(MAKEFILE_LIST)))../scripts/ci/check-release-delivery.sh
include $(dir $(lastword $(MAKEFILE_LIST)))execution.mk
# Retired settings must fail visibly, especially a roster that would otherwise
# be ignored and accidentally reduce a requested multi-group release to one.
_shared_retired_release_settings := $(strip $(foreach setting,RELEASE_FROZEN_GROUPS RELEASE_FROZEN_PUBLISH_TARGET RELEASE_FROZEN_SETUP_TARGET RELEASE_FROZEN_SETUP_MODE FROZEN_RELEASE_ACTIVE,$(if $(filter undefined,$(origin $(setting))),,$(setting))))
ifneq ($(_shared_retired_release_settings),)
$(error Retired release settings: $(_shared_retired_release_settings); adopt docs/release-batches.md)
endif
# Clean conventional source roots before capture as well as direct preflight.
.PHONY: clean-empty-source-dirs
clean-empty-source-dirs:
	@perl "$(SHARED_TOOLING_ROOT)/scripts/dev/clean-empty-source-dirs.pl"
release-patch release-minor release-major release-resume release-preflight: clean-empty-source-dirs

RELEASE_REMOTE ?= origin
RELEASE_BRANCH ?= main
# Optional consumer-owned verification after a successful exact-version resume.
# This is a check, not a second delivery or publication command.
RELEASE_RESUME_CHECK_COMMAND ?= :
# Explicit single-group releases capture automatically. Multi-group batches
# require an explicit supplier-first roster; never infer all workspace groups.
RELEASE_GROUPS ?= $(RELEASE_GROUP)
RELEASE_PUBLISH_TARGET ?=
RELEASE_SETUP_TARGET ?=
RELEASE_DELIVERY_POLICY ?=

# Consumers can reuse this canonical interface roster without repeating hook names.
override SHARED_RELEASE_TARGETS := release-patch release-minor release-major release-resume release-version release-preflight release-verify release-prepare-version release-prepared-check release-files release-commit-check release-committed-check release-tagged-check release-push-check release-merged-preflight
ifneq ($(filter $(SHARED_RELEASE_TARGETS) release-batch-resume,$(MAKECMDGOALS)),)
ifneq ($(RELEASE_DELIVERY_POLICY),)
ifneq ($(shell bash '$(subst ','"'"',$(_shared_release_delivery_check))' '$(subst ','"'"',$(RELEASE_DELIVERY_POLICY))' '$(subst ','"'"',$(if $(filter undefined,$(origin RELEASE_DELIVERY)),direct,$(RELEASE_DELIVERY)))' && printf admitted),admitted)
$(error Consumer release delivery policy refused this command)
endif
endif
endif

_shared_batch_targets := release-batch-resume release-batch-status release-batch-reconcile release-batch-retire
ifneq ($(word 2,$(filter release-patch release-minor release-major release-resume $(_shared_batch_targets),$(MAKECMDGOALS))),)
$(error Select exactly one release target)
endif

.PHONY: release-patch release-minor release-major release-resume
ifneq ($(filter release-patch release-minor release-major,$(MAKECMDGOALS)),)
ifneq ($(strip $(RELEASE_GROUP)),)
ifeq ($(strip $(RELEASE_GROUPS)),)
$(error A selected release group requires batch capture; do not clear RELEASE_GROUPS)
endif
endif
endif
release-patch release-minor release-major:
ifneq ($(strip $(RELEASE_GROUPS)),)
ifneq ($(RELEASE_BATCH_ACTIVE),1)
	+@perl "$(SHARED_TOOLING_ROOT)/scripts/ci/run-release-batch.pl" --root "$(CURDIR)" --groups "$(RELEASE_GROUPS)" --kind "$(@:release-%=%)" --remote "$(RELEASE_REMOTE)" --branch "$(RELEASE_BRANCH)" --delivery "$(if $(filter undefined,$(origin RELEASE_DELIVERY)),direct,$(RELEASE_DELIVERY))" $(if $(RELEASE_PUBLISH_TARGET),--publish-target "$(RELEASE_PUBLISH_TARGET)",) $(if $(RELEASE_SETUP_TARGET),--setup-target "$(RELEASE_SETUP_TARGET)",)
else
	+@bash "$(SHARED_TOOLING_ROOT)/scripts/ci/run-release.sh" "$(@:release-%=%)" "$(RELEASE_REMOTE)" "$(RELEASE_BRANCH)"
endif
else
	+@bash "$(SHARED_TOOLING_ROOT)/scripts/ci/run-release.sh" "$(@:release-%=%)" "$(RELEASE_REMOTE)" "$(RELEASE_BRANCH)"

endif

release-resume:
	+@bash "$(SHARED_TOOLING_ROOT)/scripts/ci/run-release.sh" resume "$(VERSION)" "$(RELEASE_REMOTE)" "$(RELEASE_BRANCH)"
	+@$(RELEASE_RESUME_CHECK_COMMAND)

.PHONY: $(_shared_batch_targets)
$(_shared_batch_targets): export SHARED_RELEASE_BATCH = $(BATCH)
$(_shared_batch_targets): export SHARED_PUBLICATION_RESULT = $(PUBLICATION_RESULT)
$(_shared_batch_targets):
	+@test -n "$$SHARED_RELEASE_BATCH" || { echo 'Select BATCH=batch-XXXXXXXX' >&2; exit 2; }
	+@perl "$(SHARED_TOOLING_ROOT)/scripts/ci/run-release-batch.pl" --root "$(CURDIR)" --$(@:release-batch-%=%) "$$SHARED_RELEASE_BATCH" --delivery "$(if $(filter undefined,$(origin RELEASE_DELIVERY)),direct,$(RELEASE_DELIVERY))" $(if $(PUBLICATION_RESULT),--publication-result "$$SHARED_PUBLICATION_RESULT",)

.DEFAULT_GOAL := $(_shared_release_default_goal)

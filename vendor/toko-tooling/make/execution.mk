# Guard selected recipes, without running probes while reading the Makefile.
# Shared companions: scripts/ci/check-make-execution.sh
ifneq ($(origin _toko_make_execution_defined),override)
override _toko_make_execution_defined := yes
_toko_execution_default_goal := $(.DEFAULT_GOAL)
override _toko_make_execution_script := $(dir $(lastword $(MAKEFILE_LIST)))../scripts/ci/check-make-execution.sh

# Explicit previews suppress recursive execution as well as admitting -n. Keep
# MAKE indirect: a literal $(MAKE) in a recipe forces it even without +.
TOKO_TOOLING_DRY_RUN ?= 0
ifneq ($(TOKO_TOOLING_DRY_RUN),0)
ifneq ($(TOKO_TOOLING_DRY_RUN),1)
$(error TOKO_TOOLING_DRY_RUN must be 0 or 1)
endif
endif
override _toko_recursive_prefix = $(if $(filter 1,$(TOKO_TOOLING_DRY_RUN)),,+)
override _toko_recursive_make = $(MAKE)

# Expand only when this prerequisite is reached, including through a consumer
# wrapper. Make errors cannot be swallowed by -i; + also reaches the guard in
# dry-run, touch and question modes before any protected recipe can execute.
override define _toko_require_execution
$(if $(filter environment,$(origin MFLAGS)),,$(error Toko Tooling requires GNU Make's generated MFLAGS; remove command-line or Makefile MFLAGS assignments))
$(if $(shell MAKEFLAGS='$(subst ','"'"',$(MAKEFLAGS))' MFLAGS='$(subst ','"'"',$(MFLAGS))' MAKEOVERRIDES= GNUMAKEFLAGS= bash '$(subst ','"'"',$(_toko_make_execution_script))' '$(subst ','"'"',$(MAKE_COMMAND))' $(if $(filter 1,$(TOKO_TOOLING_DRY_RUN)),--dry-run) >/dev/null 2>&1 && printf admitted),,$(error $(if $(filter 1,$(TOKO_TOOLING_DRY_RUN)),TOKO_TOOLING_DRY_RUN=1 requires make -n in both MAKEFLAGS and MFLAGS,Toko Tooling requires Make recipe execution and failure propagation; remove ignore-errors, dry-run, touch and question modes or preview with make -n TOKO_TOOLING_DRY_RUN=1)))
endef
.PHONY: toko-execution-check
toko-execution-check:
	+@$(_toko_require_execution) :
.DEFAULT_GOAL := $(_toko_execution_default_goal)
endif

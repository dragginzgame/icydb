# Guard selected recipes, without running probes while reading the Makefile.
# Shared companions: scripts/ci/check-make-execution.sh
ifneq ($(origin _toko_make_execution_defined),override)
override _toko_make_execution_defined := yes
_toko_execution_default_goal := $(.DEFAULT_GOAL)
override _toko_make_execution_script := $(dir $(lastword $(MAKEFILE_LIST)))../scripts/ci/check-make-execution.sh

# Expand only when this prerequisite is reached, including through a consumer
# wrapper. Make errors cannot be swallowed by -i; + also reaches the guard in
# dry-run, touch and question modes before any protected recipe can execute.
override define _toko_require_execution
$(if $(filter environment,$(origin MFLAGS)),,$(error Toko Tooling requires GNU Make's generated MFLAGS; remove command-line or Makefile MFLAGS assignments))
$(if $(shell MAKEFLAGS='$(subst ','"'"',$(MAKEFLAGS))' MFLAGS='$(subst ','"'"',$(MFLAGS))' MAKEOVERRIDES= GNUMAKEFLAGS= bash '$(subst ','"'"',$(_toko_make_execution_script))' '$(subst ','"'"',$(MAKE_COMMAND))' >/dev/null 2>&1 && printf admitted),,$(error Toko Tooling requires Make recipe execution and failure propagation; remove ignore-errors, dry-run, touch and question modes))
endef
.PHONY: toko-execution-check
toko-execution-check:
	+@$(_toko_require_execution) :
.DEFAULT_GOAL := $(_toko_execution_default_goal)
endif

# Admit Make itself before recipe failures can be ignored or execution skipped.
# Shared companions: scripts/ci/check-make-execution.sh
ifneq ($(origin _shared_make_execution_checked),override)
override _shared_make_execution_checked := yes
# GNU Make retains actual invocation options in MFLAGS even if MAKEFLAGS is
# replaced. Refuse assignments which erase that independent execution evidence.
ifeq ($(filter environment,$(origin MFLAGS)),)
$(error Shared tooling requires GNU Make's generated MFLAGS; remove command-line or Makefile MFLAGS assignments)
endif
# $(shell) does not reliably export this invocation's computed Make flags.
# Pass both explicitly, shell-quoted, for independent behavioral probes. The
# isolated Makefile never loads consumer recipes. Keep real caller flags intact.
# Resolve the probe beside this selected include, before target-specific runtime
# roots apply. MAKE_COMMAND identifies this Make executable; MAKE may instead
# carry recursive arguments/Makefiles, which must never enter the isolated probe.
ifneq ($(shell MAKEFLAGS='$(subst ','"'"',$(MAKEFLAGS))' MFLAGS='$(subst ','"'"',$(MFLAGS))' MAKEOVERRIDES= GNUMAKEFLAGS= bash '$(subst ','"'"',$(dir $(lastword $(MAKEFILE_LIST)))../scripts/ci/check-make-execution.sh)' '$(subst ','"'"',$(MAKE_COMMAND))' >/dev/null 2>&1 && printf admitted),admitted)
$(error Shared tooling requires Make recipe execution and failure propagation; remove ignore-errors, dry-run, touch and question modes)
endif
endif

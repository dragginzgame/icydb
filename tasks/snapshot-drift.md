# Shared snapshot integrity and adoption

Apply the [common run contract](README.md) and the
[snapshot contract](../docs/consuming-snapshots.md).

Inventory every maintained `.shared-tooling*.snapshot`, including nested bundles
and explicit custom manifests. Resolve each manifest's consumer root from its
documented adoption, not from whichever path produces a passing result. Run the
trusted Shared Tooling verifier with those explicit inputs:

```bash
bash scripts/ci/verify-shared-tooling-snapshot.sh \
  --consumer /absolute/consumer/root --manifest .shared-tooling.snapshot
```

Record revision, selected files, hash/mode drift and missing companions. Compare
recorded entrypoints and derived payloads separately; an older unannotated
manifest is integrity evidence but needs explicit roots for a 0.7 refresh.
Review proposed additions and exact retirements through the exporter's preview;
do not treat a dependency closure as proof of complete local caller coverage. Compare
the recorded upstream revision with reviewed committed releases and relevant
fixes, then inspect the owner's adoption issue and focused CI evidence. Never
offer dirty upstream files as a published snapshot or infer adoption from an
upstream release note.

For 0.10 and later snapshots, trace the selected common catalogs as well as
payload integrity. Record the default snapshot paths and any explicit
`HOST_TOOL_VERSIONS`, `RUST_TOOL_VERSIONS` or `IC_TOOL_PINS` exceptions, including
their documented reason and native qualification. These selections are
independent; a host exception does not select Rust pins. Compare complete
version/digest records, not just filenames or displayed versions.

Inspect Make includes, standalone `--versions`/`--pins` calls, formatting adapters,
CI cache-key inputs and failure-evidence action inputs for stale consumer-local
catalog references. Distinguish unused duplicate defaults from files still needed
for workflow-only tools or approved exceptions. Do not source a consumer catalog
or execute its Makefile merely to inspect selections; report computed/ambiguous
values as unresolved. Propose cleanup at the owning issue without deleting files,
changing pins, installing tools or refreshing a snapshot during this check.

Keep three conclusions distinct: snapshot byte integrity, available upstream
changes, and consumer behavior/host qualification. An older intact snapshot is
not corruption; prioritize applicable corrections rather than automatic churn.
Report absent adoption explicitly; Shared Tooling itself needs no self-snapshot.
Reconcile delivered adoption through the
[issue maintenance procedure](../rules/agent-maintenance.md): verify the consumer's
required checks, close completed owning issues and update linked Shared trackers.
An intact snapshot or a pushed commit alone does not satisfy behavior/native
acceptance, and an upstream tracker with other outstanding consumers stays open.

Return consumer/bundle root, recorded revision, integrity result, applicable
upstream fixes, selected catalogs/exceptions, stale callers and adoption
evidence/issue. Do not refresh or patch snapshots
during this check.

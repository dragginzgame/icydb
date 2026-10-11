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
the recorded upstream revision with reviewed committed releases and relevant
fixes, then inspect the owner's adoption issue and focused CI evidence. Never
offer dirty upstream files as a published snapshot or infer adoption from an
upstream release note.

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
upstream fixes and adoption evidence/issue. Do not refresh or patch snapshots
during this check.

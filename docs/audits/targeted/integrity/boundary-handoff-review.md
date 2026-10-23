# Targeted Review — Boundary Handoffs

Method: `HANDOFF-3`. Apply the reviewed
[Shared focused-handoff method](../../../../audits/flow-convergence-and-duplication.md#focused-boundary-handoff),
[common audit contract](../../../../audits/README.md) and
[IcyDB audit governance](../../README.md). Record the snapshot revision alongside
this overlay identity. Shared owns the handoff table, producer/consumer proof
and report mechanics. Use IcyDB's
[domain ownership map](../../README.md#finding-ownership-and-shared-evidence)
for product obligations. Earlier HANDOFF-1 and HANDOFF-2 reports remain
historical; affected comparisons are `N/A (method change)`.

## IcyDB carried authority

- Accepted schema snapshots own runtime row/index/constraint/identity meaning.
  SQL and generated models provide proposals, reconciliation, convenience or
  tests; consumers must not reconstruct accepted authority from them.
- Preserve entity/store/index identity, accepted revision/fingerprint and
  validity lifetime in persisted/prepared artifacts, caches and continuations.
- For row decoding, identify who proves the expected data-store key matches
  the primary key in the accepted row contract; for
  publication, identify who proves candidate completeness.
- Use the range owner's actual comparator and directional bounds; do not assume
  ASC-only resume or serialized-byte ordering. Include stale accepted identities
  in reachable continuation and recovery paths.

## Local evidence and report destination

Apply IcyDB's [executed-test evidence](../../README.md#executed-test-evidence)
and [verification readout](../../README.md#verification-readout).

Authorized saved investigations use
`docs/reports/investigations/YYYY/MM/DD/boundary-handoff-review/<run>/report.md`.

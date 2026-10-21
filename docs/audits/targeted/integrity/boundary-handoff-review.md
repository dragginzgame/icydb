# Targeted Review — Boundary Handoffs

Method: `HANDOFF-2`. Apply the reviewed
[Shared flow method](../../../../audits/flow-convergence-and-duplication.md),
[common audit contract](../../../../audits/README.md) and
[IcyDB audit governance](../../README.md). Record the snapshot revision alongside
this overlay identity. This bounded investigation adds no recurring baseline,
repair authority or broad gate. Old HANDOFF-1 reports remain historical;
affected comparisons are `N/A (method change)`.

## Select the handoff

Use a concrete changed producer, caller, persisted/prepared artifact or accepted
identity/publication lifetime whose guarantee is unclear at its consumer.
Follow only the reachable handoff. Consult the
[domain ownership map](../../README.md#finding-ownership-and-shared-evidence):
reuse matching proof or route an existing domain obligation to its owner.
A neighbouring PASS alone is insufficient; do not open a second finding.

The local checks below preserve the contract on the currently adopted Shared
revision. Narrow them further only after reviewing a supplying snapshot with
the focused-handoff guidance under
[#396 — Adopt shared focused-handoff guidance](https://github.com/dragginzgame/icydb/issues/396).
A moving or unpublished Shared draft is not adopted.

## IcyDB carried authority

| Invariant / authority | Producer and enforcement | Carried artifact / validity | Consumer and first protected effect | Bypass or stale-input candidate | Proof or gap |
| --- | --- | --- | --- | --- | --- |

- Accepted schema snapshots own runtime row/index/constraint/identity meaning.
  SQL and generated models provide proposals, reconciliation, convenience or
  tests; consumers must not reconstruct accepted authority from them.
- Follow actual calls to the first protected read/write/publication. Preserve
  entity/store/index identity, accepted revision/fingerprint and validity lifetime.
  Inspect alternate callers/conversions and delayed cache/continuation reuse.
- Consumer assumptions must follow from producer guarantees. For row decoding,
  identify who proves the expected key matches the accepted row identity; for
  publication, identify who proves candidate completeness.
- Inspect recovery, continuation and alternate frontends only where reachable.
  Protective checks after decoding, delayed use or changed authority may be
  required at several boundaries. Do not require exactly-once validation.
- Use the range owner's actual comparator and directional bounds; do not assume
  ASC-only resume or serialized-byte ordering.

## Focused proof and result

Apply [executed-test evidence](../../README.md#executed-test-evidence). Select
assertions/features/targets proving valid inputs retain the contract and
invalid, mismatched or stale inputs cannot reach the effect. Complete caller
inspection plus type visibility may prove an unconstructible state; label source
proof. A debug assertion alone is no production guard. Missing behavioral proof
is a gap, not a proven defect or deployed/platform rollback guarantee.

Use the shared report/severity/verdict contract and the local verification
readout, retaining scope, owner, handoff table, evidence and any missing proof.
Authorized saved investigations use
`docs/reports/investigations/YYYY/MM/DD/boundary-handoff-review/<run>/report.md`.
Stop when coverage is sufficient. No extra sweep, backlog or runtime fix follows.

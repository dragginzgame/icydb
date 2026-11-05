# Cross-repository tooling adoption and duplication

Apply the [common run contract](README.md) and the
[flow convergence method](../audits/flow-convergence-and-duplication.md).
Use an explicitly selected projects directory. First apply
[snapshot-drift](snapshot-drift.md), reusing its evidence when still applicable.
Keep absent adoption, byte drift, relevant missing fixes and harmless older
snapshots distinct. Compare against reviewed committed Shared revisions, not
dirty upstream work. No LOC tool or new monitoring service is required.

For each selected repository, trace its actual Make, CI, hooks and deployment
entrypoints to shared helpers or local implementations. Check applicable tool
catalogs, group selection down to leaf commands, nested repeated validation,
qualified evidence reuse, artifact retention and cleanup, and obsolete exports.
Read the selected helper contracts before judging a local exception. A vendored
helper without a caller is not adoption; a matching snapshot does not prove
correct wiring. Record unavailable or computed paths as unresolved.

Compare relevant capabilities with the consumer's needs. Recommend an update
for a concrete defect, missing capability or authorized refresh, not simply a
newer version. Separate product policy from reusable mechanics; identify local
code that duplicates an existing shared owner or supplies a real missing one.

Select one repeated behavior for deeper review. Trace callers, inputs, failure
and recovery semantics, ordering, host requirements and authority boundaries.
First look for an existing shared owner or obsolete code that can be retired.
Keep product policy and identities local. Similar text is a candidate, not proof
of an equivalent contract or justification for a new abstraction.

Return a compact per-repository table: recorded revision, integrity, applicable
missing capability/fix, actual caller evidence, action or justified no-action,
and owner. Rank actionable findings by demonstrated impact and identify the
required focused qualification. Include LOC only when explicitly requested.
Use owning issues only when requested, checking for existing coverage first, and
preserve active work. This task does not
edit siblings, replace snapshots or promise a reduction before implementation.

For repeated test dispatch, apply the
[consumer qualification selection](../docs/consuming-snapshots.md#consumer-qualification-selection).
Distinguish upstream implementation cases from actual consumer wiring and native
acceptance before proposing removal; retain each required proof at its owner.

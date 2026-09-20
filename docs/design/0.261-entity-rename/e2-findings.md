# E2 — Populated rename admission finding

2026-09-20. Initial finding below is historical evidence. The user approved the
narrow companion declaration, subject to design reconciliation. The correction
and native E2 qualification are complete; see [status](0.261-status.md) for the
final evidence and E3's remaining generated-actor scope.

## Frozen probe

Base: published 0.260.0, `0339efcd9fe35d9a27c4d4f2b809f9b8a6c557c6`, plus E1 docs
and the new application-boundary
[fixture](../../../crates/icydb-core/src/db/schema/application/tests/entity_rename.rs).
It reuses the maintained journaled migration-execution registry; no actor or
alternate build/recovery owner is added.

- `Item`: Nat64 `id`, unique indexed Nat64 `key`, Nat64 `label`, nullable
  Nat64 `parent_id` referencing the same entity with restrictive deletion.
- Rows: `(1, 101, 201, null)` and `(2, 102, 202, 1)`.
- Inbound variant adds `Holder(id: Nat64, item_id: Nat64)` with restrictive
  `item_id -> Item.id`, populated with `(10, 2)`.
- Successor: `Item` becomes `CatalogItem`, with source version 1 to 2 and
  `from_name = "Item"`. All relation target spellings follow that name.
  Fields, data, indexes and store assignments are otherwise unchanged.
  `Holder` remains source version 1 with no authored transition in this probe.
- Scalar label values keep the probe about identity rather than text encoding.
  This is a test workload, not a Toko Miner schema or deployment change.

## Result and cause

Without `Holder`, metadata publication succeeds, the entity tag stays equal
and the two returned rows match independent expectations. This is not yet
proof of index/reverse bytes, delete enforcement, stale binding or recovery.

With `Holder`, the same requested success fails at migration admission with
`SCHEMA_MIGRATION_MISSING_MIGRATION` (E230). No later validation was run after
this first focused failure; the two tests are the only fresh semantic results.

The source digest includes relation target names and referenced field meanings
(`icydb-schema/src/source_digest.rs`). Consequently the successor `Holder`
is not unchanged. `migration_planner.rs::validate_unchanged_lineage` correctly
requires a coordinated declaration for its changed meaning. Bumping its source
version alone also takes the missing-migration branch.

The next obvious declaration is a companion transition for `Holder`, advancing
its source version without renaming any Holder-owned field/relation or changing
row values. Source inspection finds that both `EntityMigration::try_new` and
the generated declaration parser reject a transition with no `from_name`,
renames or transforms. There is no demonstrated supported declaration for this
dependency-only transition within the frozen workload. The constructor/parser
restriction was inspected, not bypassed or changed for the probe.

## Approved correction

The explicit affected-entity contract remains coherent: Holder's source meaning
really changes. What drifted was the assumption that every transition must have
a local rename or row transform. The current constructor/parser now admit a
bare companion, and the planner requires its entire source digest, after
reversing only same-plan entity-rename target names, to equal accepted lineage.
Its next source version is still explicit. No implicit lineage, extra executor,
format, publication phase or recovery route was introduced.

The populated tests now pass. Expanded tests cover identity/data,
rejection, stale bindings, replay and compound-marker recovery. Final validation
and the current handoff verdict are recorded in [status](0.261-status.md).

### Original decision rationale

Recommended: allow a narrowly checked, explicitly versioned companion transition
for a relation-target rename explained by another transition in the same plan.
The schema declaration and existing planner remain its canonical owners.
Meaningless version bumps and unrelated unexplained changes must still reject.
No new opcode, lifecycle phase, cursor, storage version or implicit lineage
advance is proposed. Exact admission checks must be designed before removing
the current empty-operation guard; simply accepting all empty declarations
would be a broader change than this finding establishes.

This admits a previously rejected public declaration shape, exceeding E2's
initial zero-new-surface qualification assumption; the explicit scope decision
was obtained before implementation. The simpler alternative is to narrow qualification to entities with
no inbound references; that does not satisfy the originally selected workload.
Artificial field/relation renames, no-op copies that force rewriting, or silently
advancing a dependent entity's lineage are not acceptable workarounds.

## Initial blocked handoff (historical)

The focused run compiled and returned **1 passed, 1 failed, 0 ignored**.
The failing test is an active WIP reproducer, not release-ready coverage.
Formatting ran; whitespace and documentation links are checked at handoff.
Clippy, broader tests, populated rename recovery and actor qualification are
not run. Raw Wasm, IC cycles and instructions are unmeasured. No network lifecycle,
dependency/version changes, sibling edits, commits or pushes.

Six files change in this E2 attempt: two test-support Rust files and four
documentation files, approximately +345 net lines. Runtime implementation and
state space are unchanged. E1's pre-existing documentation changes remain.
This handoff paused E2 for the scope decision; the approved correction remains
inside E2 and does not start E3.

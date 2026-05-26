# NAMING.md

## Purpose

This document defines architectural naming policy for IcyDB.

It is authoritative for names that describe ownership role, cross-layer
contracts, planner/executor payloads, and helper intent. Domain vocabulary for
schema, runtime values, planner classification, and storage encodings remains
owned by `docs/architecture/TERMINOLOGY.md`.

Names should encode architectural role before historical lineage.

They should also preserve established database vocabulary. IcyDB-specific role
families are guardrails for local architecture; they are not replacements for
standard database terms.

---

## Core Rule

If two adjacent concepts play the same architectural role, they should use the
same name family.

If two concepts play different architectural roles, they should not share a
family name casually.

This applies to:

- public and internal types
- module and file names
- helper verbs
- tests that lock a named concept
- active design docs and examples

Archived design docs may keep historical vocabulary when they describe the
state of the code at that time.

## Database Idiom Guardrails

Prefer conventional database and query-engine terms when they are precise:

- `Catalog`, `Schema`, `Relation`, `Field`, `Index`, `Key`, and `Constraint`
  describe data-model and metadata concepts.
- `Predicate`, `Projection`, `Aggregate`, `Grouping`, `Ordering`, `Limit`,
  and `Offset` describe query semantics.
- `LogicalPlan`, `PhysicalPlan`, `ExecutionPlan`, `AccessPath`, and `Executor`
  describe planner/executor phases.
- `Cursor`, `Continuation`, `Page`, and `Window` describe pagination and
  resumable execution.
- `Transaction`, `Commit`, `Snapshot`, `Replay`, and `Recovery` describe
  durability and atomicity boundaries.

Do not rename conventional database terms into local role names when the
database term already communicates the concept. For example, a query
`Projection` should not become a generic `Shape`; an `Index` should not become
a generic `Capability`; and a persisted schema `Constraint` should not become
an IcyDB `Contract`.

Use IcyDB role families to clarify the phase and ownership of those database
concepts. Examples:

- `GroupedOrderContract` can describe an admission proof for an order.
- `CoveringReadPlan` can describe a planner-selected covering read payload.
- `ProjectionFacts` can describe derived projection inputs reused by execution.
- `QueryIdentity` can describe cache-key equivalence for query reuse.

When a standard term and a role family both seem plausible, prefer the standard
term for user-visible, SQL-facing, and persisted concepts. Prefer the role
family for internal phase artifacts whose main purpose is ownership,
admission, or handoff between subsystems.

---

## Type Families

### `*Plan`

Use `*Plan` for planner-selected or executable payloads consumed by a later
phase.

Use explicit database phase names when the distinction matters:

- `LogicalPlan` for schema-validated query intent before physical access
  selection.
- `PhysicalPlan` or `AccessPlan` for selected access strategy.
- `ExecutionPlan` for executor-ready payloads.

Do not use `*Plan` for local walk bundles, proof-only surfaces, or lightweight
classification snapshots.

### `*Contract`

Use `*Contract` for proof or admission surfaces, cross-layer guarantees, and
reusable shape agreements.

Do not use `*Contract` for final chosen outcomes when the value also carries
fallback state or decision reason.

Do not use `*Contract` as a replacement for conventional persisted database
constraints. Schema-level uniqueness, relation, primary-key, and field rules
should keep database vocabulary such as `Constraint`, `PrimaryKey`, `Relation`,
or `FieldKind` when those are the actual domain concepts.

### `*Decision`

Use `*Decision` for final chosen outcome objects, especially structures that
pair a selected mode, route, or payload with its reason.

### `*Facts`

Use `*Facts` for frozen classification snapshots and derived supporting inputs
reused by more than one downstream helper.

### `*Context`

Use `*Context` for local traversal bundles and short-lived owner-local
execution or derivation inputs.

Do not use `*Context` for reusable classification records when `*Facts` would
state the role more clearly.

### `*Shape`

Use `*Shape` for compact structural families, usually enum-like or
admitted/rejected forms.

Do not use `*Shape` for phase outputs that are already execution payloads.

### `*Identity`

Use `*Identity` for canonical equivalence keys, hashing/fingerprinting/cache
keys, deduplication authority, and normalized representations where two
syntactic forms intentionally compare equal.

Do not use `*Identity` for runtime behavior, evaluator policy, reducer state,
SQL truth/null semantics, or broad descriptions of what an operation does.

### `*Semantics`

Use `*Semantics` only for behavior rules:

- truth and null behavior
- value comparison or arithmetic behavior
- reducer behavior such as empty-window policy
- SQL-visible meaning that changes query results

Do not use `*Semantics` for identity surfaces whose main job is to decide when
two shapes are the same for planning, hashing, deduplication, or cache reuse.

### `*Analysis`

Use `*Analysis` for richer descriptive results with multiple derived fields or
structural answers that are more than one enum or bool.

---

## Helper Verbs

### `classify_*`

Use `classify_*` when returning a discrete family, enum, or compact result
category.

### `analyze_*`

Use `analyze_*` when returning a richer multi-field struct or composite
descriptive result.

### `derive_*`

Use `derive_*` when computing a new fact or output from inputs.

### `resolve_*`

Use `resolve_*` when looking up or selecting one already-existing thing, or
when mapping an input onto an existing slot, symbol, path, or component.

Do not use `resolve_*` for broad multi-step derivation pipelines.

### `prepare_*`

Use `prepare_*` when freezing reusable state for a later phase or precomputing
a reusable bundle for subsequent execution or planning.

Do not use `prepare_*` for final public outputs when `derive_*` would state the
role more clearly.

### `canonicalize_*`

Use `canonicalize_*` only for normalization to an equivalent stable form.

Do not use it for arbitrary validation, lookup, or shaping.

---

## Rename Discipline

Rename by concept, not by isolated symbol.

When an accepted rename changes architectural vocabulary, update the owning
surface consistently:

1. Primary types, enum variants, traits, and aliases.
2. Module files and directory names that own the renamed concept.
3. `mod ...;`, `pub use ...`, and import paths.
4. Helper functions and methods that expose the same concept.
5. Tests that lock the concept.
6. Active design docs, examples, and comments that teach the concept.

After the patch, scan for the old stem, plural, and compound forms. Remaining
hits should be intentional behavior vocabulary, archive text, generated
artifact text, or explicitly accepted residual vocabulary.

Do not rename stable public names just because they are slightly awkward. Public
surface renames require a higher bar: the new name must remove real ambiguity
or prevent likely misuse.

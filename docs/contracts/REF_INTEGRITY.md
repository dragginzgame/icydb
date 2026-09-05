# Referential Integrity (RI)

## Status

IcyDB enforces referential integrity for every schema-declared relation.

Accepted relation edges are the sole live relation authority. References are
stored as **typed primary-key values**. Declared relations trigger save-time
target-existence checks and delete-time source-reference checks.
Each accepted edge owns a non-zero entity-local `RelationId`; that exact ID,
not a source field slot, identifies its reverse domain.
The surrounding row strictness and ingress rules are defined in
`docs/contracts/WRITE_ADMISSION.md`.

This document is **normative**. It defines:

* what guarantees exist,
* what is explicitly *not* guaranteed,
* and where future extensions may occur.

It is not a roadmap.

This specification reflects the current shipped contract; the baseline
originated in the `0.10` line.

---

## 1. Scope and intent

Referential integrity (RI) in IcyDB is a **bounded pre-commit validation
rule**.

It exists to ensure that certain schema-declared references point to existing entities **at the moment of mutation**, without introducing relational database semantics.

IcyDB is **not** a relational system. It does not support:

* joins
* cascades
* public reverse traversal queries
* query-time relation semantics

RI is intentionally narrow, schema-driven, and enabled by default for declared
relations.

---

## 2. What a reference is

A scalar reference is a **typed primary-key value** identifying another entity:

```rust
Id<T>
```

A reference:

* identifies an entity by key
* is a public, non-secret identifier value
* does **not** imply ownership
* does **not** imply authorization
* does **not** imply lifecycle coupling
* does **not** imply traversal, joins, or relational semantics

References are **identity values**, not relationships in the relational sense.

A direct composite relation uses an explicitly declared ordered set of
top-level local fields that exactly matches the target's accepted composite
primary-key components. A nested relation terminates at one scalar key;
collection relations to composite targets are not part of the current
contract.

`Id<T>` is a *boundary type* used for entity-kind correctness. It is **not** automatically validated for existence.
`Id<T>` values may be deserialized from untrusted input; validation is explicit and contextual.

Existence validation occurs wherever the schema declares a relation. A field
that may contain a missing or stale identifier must be declared as an ordinary
key-typed field rather than a relation.

---

## 3. Schema-driven discovery

Referential integrity is **schema-driven and field-scoped**.

Only accepted relation edges explicitly admitted from schema declarations
participate in RI enforcement. Generated relation metadata is proposal input;
runtime save, reverse-index, and delete paths do not infer a missing edge from
raw generated field kinds.

The current accepted source locator is explicitly tagged `Direct` or `Nested`.
A direct locator carries ordered accepted field IDs. A nested locator carries
one accepted root-field ID plus an identity-based path through named wrappers,
optional boundaries, record members, enum-variant payloads, list items, set
items, or map values. Source shape is persisted authority, not an
interpretation of names or an untagged field list.

Generated direct scalar-relation write inputs expose `Id<Target>` so the Rust
facade cannot silently substitute another entity's equal-shaped key. The
adapter lowers it to the declared primitive key before accepted relation
validation. This facade typing does not replace the runtime existence check;
composite relation components remain their declared component types.

There is no inference from an identifier's type, name, or cardinality. Model
lowering discovers only explicit `rel` annotations, once per reachable entity
root. Runtime traversal follows only the accepted locator and exact accepted
enum/composite catalogs; generated models are never runtime fallback
authority.

RI applies to accepted direct relation fields and to explicitly annotated
scalar relation leaves reached through the supported nested source shapes.

---

## 4. Declared relations

The presence of `rel` is the complete enforcement declaration:

```text
item(rel = "EntityA", prim = "Ulid")
item(prim = "Ulid") // ordinary identifier with no relation guarantee
```

There is no weak, unchecked, or non-enforcing relation mode. Target metadata is
retained only for fields that accept the full relation contract.

---

### 4.1 Relation guarantees

Relations are validated on both save and delete paths.

Rules:

* Declaring `rel` opts into the complete relation contract
* Validation runs **before commit**
* The referenced entity **must exist**
* Any failure aborts the mutation
* No partial state is written
* No cascading inserts or deletes occur

For one same-store structural batch, all participating entities share one
complete entity-qualified final overlay. A relation source may therefore
precede a caller-keyed target insert in request order, and a target delete may
be paired with every source deletion or update-away in the same batch. Delete
protection runs for each target entity that has deleted keys; retained final
sources still block the whole batch.

Supported relation shapes in the current contract:

* `Id<T>`
* `Option<Id<T>>`
* Collections of `Id<T>`
* explicitly declared ordered top-level fields matching a composite target key
* scalar relation leaves below required or optional named records
* scalar enum-variant payloads
* scalar list items, set items, and map values, including bounded combinations

Supported collection forms:

* relation lists (`many` list cardinality)
* relation sets (`many` set cardinality, e.g. `IdSet<T>`)

Collection validation is **aggregate**:

* every referenced target must exist
* empty collections are valid
* a single missing target fails the save

Map keys, tuple positions, opaque payloads, recursive value cycles, and nested
leaves that would need to assemble a composite target key are not relation
sources. Nested traversal and raw occurrences are subject to the fixed
per-image and per-atomic-batch relation-work budgets.

### 4.2 Relation publication and migration

Initial schema publication accepts declared relations before source rows are
inserted. Ordinary schema application does not add or redefine relations on
an existing entity; adding an annotation does not start a historical validation
job. There is no public standalone relation-activation workflow.

An explicit supported physical migration may add a relation while transforming
an existing entity. The current planner requires a supported physical transform;
a relation addition alone is not such a migration. Bounded validation checks
candidate row images and target existence, then stages isolated reverse keys.
From validation onward, the migration gate blocks ordinary database work until
completion or pre-rewrite abort. Rewrite and final validation must finish before
marker-bound publication makes the candidate schema and reverse generation authoritative.
A pre-rewrite abort leaves accepted rows unchanged and removes staged candidate
keys through bounded cleanup.

Transforms of an existing relation's source root also require an isolated
candidate reverse generation, even when its accepted path and relation ID stay
unchanged. The migration planner resolves transform targets to accepted field
IDs; affected direct components and nested roots get a fresh physical generation,
while unaffected relations retain theirs. Rewrite and final validation use that
candidate generation before publication switches accepted authority. Retired
physical generations are not live reverse-edge authority.

Accepted candidate-relation metadata and defensive write/delete checks do not
by themselves provide a workflow that starts, advances, promotes, or aborts a
standalone relation activation. Physical migration uses its own progress record
and publication lifecycle, not a relation Forward/Verify job.

### 4.3 Relation identity and reverse domains

`RelationId` is local to one source entity and is allocated monotonically from
that entity's persisted high-water. Removing the highest live relation does
not lower the high-water or permit its ID to be reused. Exhaustion fails schema
admission before candidate publication.

All direct relations use one reserved relation-system index namespace. A
reverse key carries the source entity, physical generation, exact four-byte
big-endian `RelationId`, canonical target key, and canonical source primary
key. Physical generation distinguishes rebuilds of the same semantic relation;
it does not decide whether two definitions are the same.

---

## 5. Enforcement model

### 5.1 When enforcement runs

RI enforcement:

* is part of the write-admission pre-commit defined in
  `docs/contracts/WRITE_ADMISSION.md`
* is synchronous and bounded
* does not rely on traps or recovery
* applies to both save-time target existence checks and delete-time source checks

### 5.2 What is enforced

Every schema-declared relation receives RI guarantees.

For collections, validation is element-wise and bounded.

RI enforcement is skipped when:

* the value is explicitly absent (`None`)
* the field is not a schema-declared relation
* an accepted enum value selects a different variant from the annotated payload
* an accepted repeated source contains no terminal values

There is no runtime relation discovery from decoded value shape. Unsupported
nested source shapes reject during schema admission rather than becoming
unchecked references.

Historical findings during physical-migration validation describe rejected
candidate state, not accepted-state corruption. A candidate relation is not a
weak relation mode and cannot become authoritative before migration publication.

### 5.3 What is not enforced

IcyDB explicitly does **not** enforce:

* cascading deletes or updates
* query-time reverse traversal semantics
* read-time validation
* deferred checking of a newly authored relation value
* cross-mutation or cross-message constraints

### 5.4 Integrity inspection scope

Write admission remains the sole authority that permits a new relation
after-image. Quick checks the bounded accepted relation declaration and its
accepted source/target control closure. Deep verifies each selected source
row's target and expected reverse witness, then scans the active source-owned
reverse generation for orphaned or divergent entries.

Inbound relations are not silently folded into the target entity's claim.
They belong to their source entity scan; the direct target store participates
in that job's proof vector so target mutation invalidates the sweep. Pending
candidate generations remain owned by constraint activation and are excluded
from accepted integrity claims until promotion.

### 5.5 Pre-1.0 recreation boundary

The 0.253 relation snapshot, allocator, source locator, database boot identity,
and reverse-key form replace their predecessors in place as current version
`1`. A 0.253 binary rejects 0.252 stable state during database-format admission.
Rows that must survive require old-binary export followed by a fresh 0.253
installation and ordinary current-form import. There is no predecessor
decoder, reverse-key fallback, in-place upgrade, or reverse-only regeneration
route.

---

## 6. Atomicity compatibility

Referential integrity is designed to preserve IcyDB’s atomicity model.

* Relation validation completes under the write-admission contract
* The apply phase follows `docs/contracts/ATOMICITY.md`

RI enforcement does **not** depend on traps, recovery timing, or read behavior.

---

## 7. Error classification

Relation failures surface as **write-time validation errors**.

They are reported as:

* `ErrorClass::InvariantViolation`
* `ErrorOrigin::Executor`

They indicate invalid input, **not** corruption.

Accepted and pending relations retain their stable constraint ID and name in
typed runtime or validation diagnostics. Historical activation findings are
reported through the bounded validation response; they are not collapsed into
an ordinary write error.

---

## 8. Explicit non-goals

The following are out of scope for the current RI contract:

* implicit junction-table or relational many-to-many traversal
* recursive existence validation
* cascading behavior
* deferred constraint checking
* `ON DELETE SET NULL` or `ON DELETE SET DEFAULT`
* query-time relation semantics
* joins or relational algebra

Any addition requires a new RI specification.

---

## 9. Reserved extension points (non-binding)

The following extensions are explicitly reserved:

* collection relations to composite target keys
* stronger static guarantees for entity–store locality
* tooling for reference diagnostics and visualization

Any extension must preserve:

* bounded pre-commit validation
* single-message atomicity
* executor simplicity
* the distinction between relations and ordinary identifier fields

---

## 10. Summary

IcyDB’s referential integrity model is:

* **schema-driven**
* **always enforced for declared relations**
* **save-time and delete-time**
* **bounded**
* **non-relational**

Ordinary key-typed fields remain available when dangling identifiers are an
intentional part of the domain model. The difference is explicit and
foundational: a relation always carries referential-integrity guarantees.

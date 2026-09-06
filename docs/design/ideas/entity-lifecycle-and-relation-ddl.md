# Entity Lifecycle And Relation DDL

Status: unpromoted workload questions; no implementation authority

Reviewed: 2026-09-06

## Maintained Starting Point

Coordinated migration preparation, candidate validation, bounded physical work,
and publication already have a schema-owned lifecycle. Start from the
[source-migration design](../archive/0.218-explicit-versioned-schema-migrations/0.218-design.md)
and the maintained
[command contract](../../../crates/icydb-core/src/db/schema/migration_api.rs).
The existence of a multi-entity transition does not establish a need for a
second catalog epoch, draft registry, fingerprint family, or publication runner.

Accepted relation identities, activation, reverse indexes, and nested relation
paths also have maintained owners. Reconcile the
[accepted-constraint design](../archive/0.211-accepted-catalog-constraints/0.211-design.md)
and [nested-relation design](../0.253-nested-relations/0.253-design.md) with the
current code before proposing relation DDL. SQL remains a frontend to
catalog-native mutation semantics.

## Independent Questions

These are separate possible outcomes, not one implementation programme. Promote
only one when a concrete application workload demonstrates the missing behavior.

| Outcome | Question that must be answered before design |
| --- | --- |
| Entity creation | Can existing generated proposal/admission meet the need, or must an application create an entity without deploying a declaration? Define absence checks, accepted identity allocation, and runtime access. |
| Entity removal | Does the workload require removing accepted identity, deleting rows, or retiring physical storage? Define relation, index, journal, recovery, and active-migration consequences separately. |
| Entity rename | Which source/display identity changes, and which accepted entity/tag/store identities must remain stable? Compare explicit source-lineage support before treating a rename as replacement or data movement. |
| Relation DDL | Which exact relation change cannot already be delivered through maintained source migration and activation? Define accepted source/target checks and bounded activation through the existing relation owner. |

The current migration rename vocabulary includes fields, named types, and local
relations; that does not establish support for entity rename. Audit the exact
requested transition rather than inferring capability from the command name.

## Ownership And Boundaries

- Accepted catalog snapshots own entity, field, relation, and index semantics.
  Generated models may propose changes but cannot reconstruct runtime authority.
- Reuse existing migration admission, physical-work execution, and publication
  wherever their contract covers the requested outcome. Identify a concrete
  missing invariant before extending an owner.
- Relation changes must preserve accepted target-key compatibility, historical
  validation, write/delete admission, reverse-generation consistency, and
  recovery. No SQL-only relation representation or second execution route.
- Entity removal does not authorize memory-slot reuse. Store retirement and
  physical allocation history belong to the separate
  [logical-memory ownership investigation](logical-memory-identity.md).
- Source renames must not leave old-name aliases or compatibility lookup paths.
  Persisted/runtime representation changes follow the pre-1.0 version-1 hard cut.

No CREATE/DROP/FOREIGN KEY syntax, activation mode, durable tombstone, ownership
transfer policy, or entity-tag behavior is selected here. Those choices depend
on the independently demonstrated outcome and the current owner audit.

## Promotion Gate

1. Capture one concrete schema, requested transition, and application reason;
   compare the maintained source-migration or application workflow first.
2. Trace admission, accepted identity, physical work, publication, invalidation,
   and recovery for that outcome. Record reusable owners and the precise gap.
3. Establish the smallest end-to-end change and its state-space delta before
   adding a format, lifecycle state, execution route, or widely consumed variant.
4. Define direct success, rejection, interruption/recovery, and stale-authority
   evidence, plus raw Wasm, instruction, and implementation-complexity gates.
5. Promote only that bounded outcome into an explicitly authorized design and
   substantive landing-slice tracker. Parser reservations, unused classifiers,
   and rejection-only scaffolding are not standalone implementation outcomes.

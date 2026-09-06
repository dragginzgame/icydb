# IcyDB Design Note - Schema Ownership Adoption

## Status

Historical, unpromoted owner-transfer question; not implementation authority.

The maintained source-migration `Adopt` command establishes exact generated
schema lineage; it is not an arbitrary generated-to-DDL ownership transfer.
Require a concrete transfer workload before adding any new policy or state.

## Purpose

This note sketches a future policy for transferring schema facts between
generated, DDL-owned, and managed ownership classes.

0.178 keeps ownership fail-closed: DDL may mutate DDL-owned accepted facts only
when schema-owned admission says the target is safe.

## Problem

Accepted schemas can contain facts with different owners:

- generated facts proposed by Rust models;
- DDL-owned facts authored after deployment;
- managed facts controlled by IcyDB internals or future policy surfaces;

DDL must not accidentally take control of generated or managed facts. But future
users may need explicit adoption workflows, such as taking over a generated
field with DDL or converting a DDL index into generated model ownership.

## Ownership Model

Each mutable schema fact should carry:

- owner kind;
- owner provenance;
- adoption eligibility;
- last accepted schema identity that changed ownership;
- optional policy reason.

Candidate owner kinds:

```text
Generated
Ddl
Managed
```

Missing ownership metadata must fail current-format validation. It is not an
additional ownership class or an implicit adoption route.

## Adoption Rules

Adoption is a schema-owned transition, not a SQL binder shortcut.

An adoption request must:

- declare expected accepted identity;
- declare current owner;
- declare target owner;
- prove the fact identity is unchanged unless the adoption operation explicitly
  permits a paired metadata edit;
- bump schema version if accepted fingerprint changes;
- publish only through the accepted snapshot path.

Generated-to-DDL adoption should require explicit syntax or API shape. DDL must
not infer adoption from a matching field or index name.

DDL-to-generated adoption should require generated model reconciliation to
prove the generated fact is semantically equivalent to the accepted fact.

Managed facts should remain non-adoptable unless a future managed policy module
defines a transfer.

## Fail-Closed Rules

- Missing ownership metadata rejects through current-format validation.
- Generated-owned fields reject DDL drop/rename/default/nullability changes.
- Generated-owned indexes reject DDL drop unless explicitly adopted.
- Managed-owned facts reject all DDL mutation by default.
- Ownership transfer must not allocate field/index identities in SQL DDL.

## Open Questions

- Should adoption be SQL syntax, admin API, or generated reconciliation policy?
- Does adoption itself change accepted schema fingerprint?
- How should diagnostics expose provenance without leaking internal details?
- Can ownership transfer be reversed?
- Should adoption require an empty runtime data proof?

## First Safe Slice

Add a schema-owned adoption classifier with no live adoption path.

Tests should prove:

- missing ownership metadata fails current-format validation;
- generated and managed facts reject normal DDL mutations;
- adoption requests fail closed until a concrete owner-transfer policy exists.

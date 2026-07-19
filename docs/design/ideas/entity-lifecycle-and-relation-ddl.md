# IcyDB Design Note - Entity Lifecycle and Relation DDL

## Status

Tentative follow-up after [0.211 accepted-catalog
constraints](../0.211-accepted-catalog-constraints/0.211-design.md) and [0.212
bounded integrity
checking](../0.212-bounded-resumable-integrity-check/0.212-design.md). Not part
of either implementation line.

## Purpose

This note sketches a future catalog-level DDL model for entity birth/death,
entity rename, and relation changes.

Existing schema-mutation work handles accepted-schema-affecting transitions for
one live entity. Entity lifecycle and relation DDL need a wider catalog identity
model because the accepted-before or accepted-after entity snapshot may be
absent, or because one relation transition coordinates source, target, and
reverse-generation authority.

0.211 supplies stable relation/constraint identity and the bounded activation
state machine. This follow-up may reuse those owners; it must not invent a
SQL-only relation representation or execution path.

## Problem

Existing-entity DDL can use a simple entity-local rule:

```text
accepted entity version N
accepted-after entity version N + 1
accepted-before identity must still be live at publication
```

That does not fit:

- `CREATE TABLE`, where accepted-before entity identity is absent;
- `DROP TABLE`, where accepted-after entity identity is absent;
- entity rename, where path/tag/store identity may change;
- relation DDL, where more than one accepted entity may be affected.

## Design Direction

Introduce catalog-level transition admission beside entity-level admission.

Catalog transition identity should include:

- catalog epoch or equivalent publication identity;
- affected entity identities before publication;
- affected entity identities after publication;
- store paths;
- relation edge identities;
- schema fingerprint method versions for every affected accepted snapshot;
- a transition fingerprint over the whole catalog candidate.

Entity-level DDL remains the common case. Catalog-level DDL is used only when
the transition cannot be expressed as a single existing entity version bump.

## Candidate Rules

`CREATE TABLE`:

- accepted-before entity identity is absent;
- generated model proposal or DDL table definition creates candidate snapshot;
- schema-owned allocation assigns entity tag/path/store contracts;
- publication must recheck absence under catalog publication lock.

`DROP TABLE`:

- accepted-before entity identity must be live;
- accepted-after entity identity is absent;
- publication must preflight data/index/relation consequences;
- runtime state must self-invalidate by catalog publication identity.

Entity rename:

- should be treated as catalog-level identity replacement, not just metadata;
- must define whether entity tag is stable or replaced;
- must define old path lookup behavior.

Relation DDL:

- must validate both source and target accepted contracts;
- accepts only local fields whose exact accepted kinds match the target's
  accepted primary-key fields;
- creates an enforced relation with immediate checking and
  `ON DELETE RESTRICT`;
- lowers to 0.211's candidate relation, write gate, target-existence scan,
  staged reverse generation, stable-revision verification, and atomic
  promotion; and
- must update delete/save preflight authority atomically with accepted
  publication.

The reserved SQL shape is conceptually:

~~~sql
ALTER TABLE Order
ADD CONSTRAINT order_user_fk
FOREIGN KEY (user_id)
REFERENCES User (id)
ON DELETE RESTRICT
NOT VALID
~~~

The syntax is illustrative and unsupported until this design is completed.
`NOT VALID` would expose the same temporary `EnforcingNewWrites` activation as
0.211, not a permanent partially enforced relation. A future plain form must
either prove complete historical validity in one bounded call or change
nothing.

## Fail-Closed Rules

Until explicitly supported:

- relation mutation rejects before mutation planning;
- primary-key mutation rejects;
- entity create/drop/rename rejects before publication;
- relation changes never fall back to generated model metadata;
- relation validation consumes accepted row contracts only;
- `FOREIGN KEY` does not imply an unchecked, alternate-key, deferred, cascade,
  `SET NULL`, or `SET DEFAULT` variant; and
- storing an ordinary typed primary-key value without declaring a relation
  remains the intentional non-relational representation.

## Open Questions

- Is the catalog version global, store-local, or entity-set-local?
- Can relation DDL be single-entity if only source metadata changes?
- How does SQL-DDL ownership coexist with later generated reconciliation of the
  same canonical relation identity?
- What exact catalog publication lock covers source metadata, target identity,
  the temporary delete barrier, and reverse-generation promotion?
- Should entity tags be user-visible and stable across rename?
- What is the durable tombstone model for dropped entities?
- How does journal replay order catalog-level and entity-level transitions?

## First Safe Slice

Add explicit parser/binder rejection tests and source guards proving unsupported
catalog-level DDL cannot reach schema mutation planning or publication.

The first supported implementation should choose exactly one narrow case, such
as `CREATE TABLE` for an empty generated entity declaration, and define catalog
absence/recheck semantics before any broader relation work begins.

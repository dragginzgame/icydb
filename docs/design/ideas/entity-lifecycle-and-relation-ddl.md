# IcyDB Design Note - Entity Lifecycle and Relation DDL

## Status

Tentative follow-up design. Not scoped to 0.178.x.

## Purpose

This note sketches a future catalog-level DDL model for entity birth/death,
entity rename, and relation changes.

0.178 handles accepted-schema-affecting transitions for existing accepted
entities. Entity lifecycle and relation DDL need a wider catalog identity model
because the accepted-before or accepted-after entity snapshot may be absent.

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
- must define cascade/restrict behavior;
- must update relation indexes and delete/save preflight authority before
  accepted publication.

## Fail-Closed Rules

Until explicitly supported:

- relation mutation rejects before mutation planning;
- primary-key mutation rejects;
- entity create/drop/rename rejects before publication;
- relation changes never fall back to generated model metadata;
- relation validation consumes accepted row contracts only.

## Open Questions

- Is the catalog version global, store-local, or entity-set-local?
- Can relation DDL be single-entity if only source metadata changes?
- Should entity tags be user-visible and stable across rename?
- What is the durable tombstone model for dropped entities?
- How does journal replay order catalog-level and entity-level transitions?

## First Safe Slice

Add explicit parser/binder rejection tests and source guards proving unsupported
catalog-level DDL cannot reach schema mutation planning or publication.

The first supported implementation should choose exactly one narrow case, such
as `CREATE TABLE` for an empty generated entity declaration, and define catalog
absence/recheck semantics before any broader relation work begins.

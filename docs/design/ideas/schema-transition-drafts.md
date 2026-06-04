# IcyDB Design Note - Schema Transition Drafts

## Status

Tentative follow-up design. Not scoped to 0.178.x.

## Purpose

This note sketches a future draft-based migration surface for schema changes
that need more than one SQL DDL statement before publication.

0.178 closes single-statement DDL transition admission. Drafts should preserve
that boundary: DDL authors intent, while schema-owned code derives candidates,
admits transitions, plans physical work, validates, publishes, and invalidates.

## Problem

Some migrations are not naturally a single statement:

- add a column, backfill it, then make it required;
- add indexes that require physical validation before visibility;
- stage multiple metadata edits that should publish atomically;
- prepare a migration in one call and publish in a later call.

Without an explicit draft model, these workflows can drift into partial
accepted-schema publication, ad hoc session state, or SQL-owned mutation
semantics.

## Design Direction

Drafts are unpublished schema transition candidates. They may be edited and
validated, but they do not mutate accepted schema identity or planner-visible
runtime state until a schema-owned publish operation succeeds.

Each draft should carry:

- draft id;
- entity identity;
- accepted-before identity captured at draft creation;
- expected current schema version;
- requested accepted-after schema version;
- ordered DDL intent list;
- schema-owned accepted-after candidate;
- schema-owned mutation request and execution plan;
- validation and runner readiness state;
- expiration or explicit abort policy.

## Admission Rules

Draft creation must bind to the live accepted schema and capture its accepted
identity.

Every edit must recheck that the draft's accepted-before identity still matches
the live accepted identity. If not, the draft becomes stale and cannot publish.

Draft edits may update only the unpublished candidate. They must not:

- write accepted schema snapshots;
- run irreversible physical work;
- invalidate query/runtime state;
- expose indexes or columns as accepted runtime authority.

## Publication Rule

Publishing a draft is a single schema-owned operation:

```text
draft accepted-before identity
  == live accepted identity under publication lock
```

If the identity differs, publish rejects as stale and leaves accepted schema
unchanged. If validation or runner preflight fails, publish also leaves accepted
schema unchanged.

## Open Questions

- Is draft state durable, session-local, or both?
- Should drafts survive canister upgrade?
- Should draft ids be user-visible or only returned by an admin API?
- Can a draft span multiple entities, or is that a separate catalog-level
  migration design?
- What is the authorization surface for create/edit/validate/publish/abort?

## Non-Goals

- No relation DDL.
- No entity create/drop.
- No version-gap import.
- No online rewrite runner semantics beyond the runner design that publishes
  this draft.

## First Safe Slice

Implement parser reservation only if it rejects before binding and planning.

A real first implementation should support:

- creating one draft for one existing entity;
- appending one otherwise-supported DDL intent;
- validating the draft candidate through existing schema-owned admission;
- aborting the draft;
- no publication until final publish semantics are fully implemented.

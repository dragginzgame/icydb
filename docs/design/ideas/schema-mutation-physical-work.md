# IcyDB Design Note - Schema Mutation Physical Work

## Status

Tentative follow-up design. Not scoped to 0.178.x.

## Purpose

This note sketches a future physical-work lifecycle for schema mutations that
cannot publish accepted metadata safely without rebuild, backfill, row rewrite,
or validation work.

0.178 establishes that runner preflight is schema/execution-owned and that
unsupported physical work fails before accepted snapshot publication.

## Problem

Some transitions require durable physical work:

- secondary index rebuild or backfill;
- uniqueness validation;
- default materialization;
- nullability proof;
- type rewrite;
- row layout compaction;
- cleanup after retired slots or dropped indexes.

If accepted metadata becomes planner-visible before physical validation, reads
can observe indexes or row contracts that storage has not actually built.

## Design Direction

Physical work should be a staged, validated, publish-or-abort pipeline:

```text
accepted-before snapshot
  -> accepted-after candidate
  -> schema-owned execution plan
  -> runner input from accepted before/after
  -> staged physical writes
  -> validation gate
  -> publication identity handoff
  -> accepted snapshot publication
  -> runtime invalidation
  -> staged state promotion or cleanup
```

No planner-visible accepted metadata should publish until required validation
passes.

## Required Contracts

Runner input:

- accepted-before snapshot;
- accepted-after candidate;
- execution plan;
- target store path;
- target index/field identities;
- runtime publication identity to hand off only after publish.

Runner output:

- rows scanned;
- keys written;
- staged physical store identity;
- validation proof;
- rollback plan;
- runtime invalidation handoff.

Publication gate:

- rejects missing runner capabilities;
- rejects validation failure;
- rejects publication race;
- rejects staged-state drift;
- publishes accepted schema only after all pre-publication physical guarantees
  hold.

## Visibility Rule

Planner/runtime visibility is tied to accepted publication identity, not staged
physical writes. Staged indexes are invisible to query planning until accepted
metadata publishes. Accepted metadata does not publish until staged physical
work is validated.

## Failure Rules

Before publication:

- failed validation leaves accepted schema unchanged;
- staged writes may be rolled back or left isolated and unreachable;
- runtime caches/cursors remain tied to the old accepted identity.

After publication:

- runtime state must invalidate or self-invalidate by accepted publication
  identity;
- cleanup can be best-effort only if the new accepted runtime state is already
  correct without it.

## Open Questions

- Are staged physical writes durable across canister upgrade?
- How large can one runner step be before it must yield?
- Is online work resumable by an explicit migration job id?
- What metrics prove no planner-visible gap exists?
- Which storage backends support isolated staging natively?

## First Safe Slice

Extend the existing field-path index runner path with durable staged identity
and explicit publish handoff tests, without adding new DDL syntax.

Tests should prove:

- staged physical writes are not planner-visible;
- validation failure leaves accepted schema unchanged;
- publication race leaves staged work unpublished;
- successful publication advances runtime identity and invalidates stale plans;
- cleanup failures do not corrupt accepted runtime authority.

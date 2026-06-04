# IcyDB Design Note - Version-Gap Import and Migration Scripts

## Status

Tentative follow-up design. Not scoped to 0.178.x.

## Purpose

This note sketches future support for intentional schema-version gaps, external
schema import, and public migration scripts.

0.178 deliberately rejects version gaps for normal single-statement DDL. That
rule should remain the default.

## Problem

Normal DDL should advance schema version by exactly one accepted transition.
But future operational workflows may need:

- importing a schema snapshot from another environment;
- replaying a migration script that spans many versions;
- restoring from backup where intermediate snapshots are absent;
- applying generated hard-cut migrations before 1.0;
- proving that skipped versions were intentionally bridged.

If version gaps are accepted through ordinary DDL, stale writes and accidental
schema skips become too easy.

## Design Direction

Version-gap import should be a separate privileged migration/import surface, not
an extension of ordinary SQL DDL admission.

The import surface should require:

- explicit source provenance;
- expected live accepted identity;
- imported accepted-after identity;
- fingerprint method version;
- migration script id or import id;
- validation proof over the imported snapshot;
- policy reason for the version gap;
- journal/replay record that preserves the import boundary.

Ordinary DDL continues to reject `N -> N + k` where `k > 1`.

## Migration Script Model

A migration script is a declarative or generated sequence of schema-owned
transition intents. It may contain DDL-like text, but SQL parsing is not the
source of authority.

Script execution should produce:

- ordered transition records;
- per-step expected accepted identity;
- per-step accepted-after identity;
- runner requirements;
- validation proofs;
- final publication identity.

The script runner must not hide failed intermediate transitions by publishing a
later snapshot directly unless the import policy explicitly authorizes a
hard-cut.

## Import Modes

Strict replay:

- every intermediate transition is admitted and published in order;
- no version gap remains.

Hard-cut import:

- one imported accepted snapshot replaces the live snapshot after validation;
- requires explicit policy and provenance;
- before 1.0, internal formats may hard-cut without compatibility fallbacks.

Read-only audit:

- validates imported snapshots and reports planned changes;
- publishes nothing.

## Fail-Closed Rules

- Ordinary SQL DDL never accepts version gaps.
- Import cannot compare raw fingerprint bytes without method-version awareness.
- Import cannot bypass accepted identity publication recheck.
- Import cannot make physical metadata planner-visible before validation.
- Import cannot reconstruct runtime authority from generated models.

## Open Questions

- What authorization is required for import/hard-cut?
- Should imported snapshots be signed or checksummed externally?
- How are failed scripts resumed?
- How much provenance belongs in stable storage?
- Should migration scripts be Rust-generated, SQL text, JSON, or a separate DSL?

## First Safe Slice

Add a read-only import validator that accepts an imported snapshot plus expected
live identity and reports whether it would be a strict replay, hard-cut import,
or rejection. It must publish nothing.

Tests should prove ordinary DDL still rejects version gaps and the import
validator rejects method-version mismatches, stale live identity, invalid
snapshots, and physical-work requirements without runner proof.

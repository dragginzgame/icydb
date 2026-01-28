# IcyDB Post-0.5 Status and Roadmap

## Executive Summary

IcyDB 0.5 completes Query Engine v2 stabilization and locks in a typed, intent-first query model.
Execution safety now relies on validated, entity-bound plans rather than executor-side validation.
Persistence is stable-memory backed; recovery is deterministic via commit markers and replayed ops.
0.5 is correctness- and invariants-focused, not a feature expansion release.

## Current State Assessment

### Correctness and Invariants

- Query intent is typed (`Query<E>`) and planning produces an executor-only `ExecutablePlan<E>`.
- Executors accept only validated plans; executor-invalid plans are mechanically unrepresentable.
- Ordering semantics are deterministic; incomparable values preserve input order.
- Persisted bytes are bounded and decoded fallibly; corruption is surfaced, not ignored.
- Unique index reads re-validate stored values to catch hash collisions.

### Architecture and Layering

- Clear separation: planner selects access paths; executors execute without planning.
- Commit markers define mutation authority; recovery replays recorded index ops then data ops.
- Indexing is explicitly hash + equality based; index keys are fixed-size and canonical.

### Operational Characteristics

- Write path cost is dominated by index fingerprinting and index/data mutations.
- Index maintenance is explicit; changes to index definitions require rebuilds.
- Error taxonomy is explicit (`Unsupported`, `Corruption`, `Internal`, etc.).
- Storage uses stable memory (not heap), backed by CanIC B-tree structures.

## Non-Goals (Explicit)

The following are deliberately out of scope as of 0.5:

- Multi-entity transactions or cross-entity atomicity.
- Snapshot isolation / MVCC or historical reads.
- Cost-based or statistics-driven planning.
- Storage format versioning, compaction, or migration tooling.
- Schema evolution automation or compatibility tooling.

These are not gaps in 0.5; they are deferred by design.

## Roadmap

### Tier 1 — Low-Risk, High-ROI Hardening

Localized improvements that do not change storage formats or public APIs:

- Expand planner/executor diagnostics and trace output (composite plans, row counts).
- Improve corruption context for index/data decode errors.
- Add targeted recovery and commit-marker regression tests.
- Tighten documentation on atomicity and indexing semantics.

### Tier 2 — Medium-Scope Capability Expansions

Improvements that require careful design and may touch planner/executor logic:

- Heuristic planner improvements that stay deterministic (no cost model).
- Executor batching and memory-conscious execution for large scans.
- Richer predicate ergonomics with explicit, documented semantics.

### Tier 3 — Foundational Changes

Large efforts that should remain isolated from incremental hardening:

- Storage format versioning and compaction.
- MVCC or snapshot isolation.
- Schema evolution and migration tooling.
- Cross-entity transaction support.
- Cost-based planning and statistics collection.

## Design Principles Going Forward

- No panics beyond validation boundaries; persisted input is always fallible.
- Deterministic recovery via commit markers and replayed operations.
- Hash-based indexing with explicit equality semantics.
- Schema-first validation gates all execution paths.
- Planner and executor responsibilities remain strictly separated.

## Stability and Compatibility Expectations

- Backward compatibility is not guaranteed pre-1.0; breaking changes are documented.
- Persisted formats may change until versioned storage is introduced.
- Production users should pin to tags for reproducible builds.

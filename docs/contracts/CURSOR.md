# Cursor Pagination Checkpoint (Historical Note)

Date: 2026-02-12

This file captures a point-in-time audit checkpoint.
Normative cursor guarantees are defined in `docs/QUERY_CONTRACT.md`.
Release tracking for cursor-related work lives in `docs/status/0.9-status.md`
and `docs/status/0.10-status.md`.

## Done

- Completed a full cursor-pagination audit across planner, executor, and session/facade APIs.
- Documented the concurrency contract explicitly in public/core docs as:
  `best-effort and forward-only over live state`, with no snapshot/version pinning.
- Updated doc comments in:
  - `crates/icydb-core/src/db/query/session/load.rs`
  - `crates/icydb/src/db/session/load.rs`
  - `crates/icydb/src/db/response/paged.rs`
- Confirmed execution behavior: cursor boundary and pagination are applied in post-access phase after rows are loaded/materialized and ordered.
- Confirmed there is no cursor pushdown into index continuation seek/range in the current implementation.

## Current Behavior (As Implemented)

- Cursor token is opaque by API contract, but not confidential:
  - Internal token is CBOR bytes containing continuation signature + boundary slots.
  - Public token is hex-encoded bytes.
- Boundary slots reflect canonical index ordering (ordered components + PK tie-break), consistent with IndexKey v2 encoding.
- Pagination continuity is deterministic per request, but not snapshot-isolated across requests.
- Continuation is strict forward-only over the canonical boundary.
- For a fixed query shape, rows are not duplicated across pages when ordered keys remain stable between requests.
- Under concurrent writes, continuation reflects live state; updates that reorder rows can cause logical drift (skips/re-observation).

## Decision For This Checkpoint

- We are **not** implementing a cursor data-leakage fix in this pass.
- No encryption/HMAC or server-side cursor-state tokenization is planned in this checkpoint.

## Follow-up Ideas At The Time (Non-Leakage)

- Decide whether to optimize performance with cursor pushdown/range-seek semantics for large paged scans.
- Add explicit tests for public hex cursor decode failures at API boundary.
- Add explicit tests/docs for mutation-between-pages behavior (insert/update/delete drift expectations).
- If needed later, evaluate secure cursor envelopes as a separate scoped change.

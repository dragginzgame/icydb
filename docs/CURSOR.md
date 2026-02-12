# Cursor Pagination Checkpoint

Date: 2026-02-12

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
- Boundary slots follow canonical order (user order fields + PK tie-break), so non-PK ordered fields can be present in token payload.
- Pagination continuity is deterministic per request, but not snapshot-isolated across requests.
- Under concurrent writes, continuation reflects live state (normal stateless cursor semantics).

## Decision For This Checkpoint

- We are **not** implementing a cursor data-leakage fix in this pass.
- No encryption/HMAC or server-side cursor-state tokenization is planned in this checkpoint.

## Left To Do (Non-Leakage)

- Decide whether to optimize performance with cursor pushdown/range-seek semantics for large paged scans.
- Add explicit tests for public hex cursor decode failures at API boundary.
- Add explicit tests/docs for mutation-between-pages behavior (insert/update/delete drift expectations).
- If needed later, evaluate secure cursor envelopes as a separate scoped change.

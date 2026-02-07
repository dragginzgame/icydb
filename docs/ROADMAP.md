# IcyDB Roadmap

This document records **where IcyDB is headed**.

It is intentionally separate from the 0.7 contract docs (`docs/atomicity.md`, `docs/REF_INTEGRITY.md`), which define current guarantees and limits.

## Current State (0.7.x)

- Single-entity save/delete operations are atomic.
- Batch write helpers are fail-fast and non-atomic.
- Atomicity and recovery guarantees are scoped to the current architecture and contract.

## Direction

The project direction remains:

- typed-entity-first APIs
- deterministic planning and execution
- explicit invariants with mechanical enforcement
- clear boundaries between stable public API and internal engine details

## Explicit Goals

### Transactions are a goal

**Transactions are a project goal.**

Specifically, support for stronger multi-entity transactional semantics is a planned direction for future releases.

This does **not** change the current 0.7 contract: today’s batch helpers remain non-atomic, and transaction semantics beyond current guarantees are not yet implemented.

Any transaction feature work must ship with:

- a clear semantics spec
- atomicity/recovery updates
- explicit API naming and migration guidance
- tests covering partial-failure and replay behavior

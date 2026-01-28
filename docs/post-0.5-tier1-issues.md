# Tier 1 Hardening Issues (Post-0.5)

This list converts Tier 1 items into testable, non-refactor issues.
Each issue is scoped to localized changes with explicit acceptance criteria.

## Issue 1: Commit Recovery Idempotence Tests

**Invariant strengthened**
Recovery must be deterministic and idempotent: replaying the same commit marker
must not duplicate index entries or data rows, and the marker must clear after replay.

**Signal improved**
Recovery correctness is provable with explicit tests; regressions surface as test failures
instead of silent data duplication or lingering commit markers.

**Test (fails today, passes after)**
Add a test that simulates a forced failure after `begin_commit` with staged index/data ops,
invokes `ensure_recovered`, and asserts:
- commit marker is cleared after recovery
- index entry key set matches expected values (no duplication)
- data row count is stable across repeated recovery calls

**Acceptance criteria**
- New regression test in `crates/icydb-core/src/db/commit.rs` or
  `crates/icydb-core/src/db/executor/tests.rs` that fails on current behavior
  (no recovery idempotence assertions) and passes after implementation.
- Test covers at least one index op and one data op.

## Issue 2: Trace Access for Composite Plans

**Invariant strengthened**
Diagnostics must expose the access shape even for composite plans; `TraceAccess` should not
be `None` solely because the plan is a union/intersection.

**Signal improved**
Execution traces become actionable for query tuning by showing access topology rather than
opaque `None` entries.

**Test (fails today, passes after)**
Add a test that builds a predicate producing a composite access plan and asserts that
trace events include a non-`None` access descriptor (e.g., union/intersection with child count).

**Acceptance criteria**
- New trace variant or structured access descriptor for composite access plans.
- New test in `crates/icydb-core/src/db/executor/tests.rs` verifies trace access is populated.

## Issue 3: Per-Phase Row Counts in Executor Trace

**Invariant strengthened**
Trace events must reflect executor phases deterministically, with row counts that
match the actual execution pipeline (access -> filter -> order -> page/delete).

**Signal improved**
Performance debugging gains concrete, phase-level row counts instead of only start/finish.

**Test (fails today, passes after)**
Add a test that executes a query with filtering and pagination and asserts that trace
events report the expected row counts after each phase.

**Acceptance criteria**
- New trace events or fields that capture per-phase counts.
- A deterministic test that fails prior to the change due to missing counts.

## Issue 4: Corruption Error Context for Index Decode

**Invariant strengthened**
Corruption diagnostics must include index identity and decode stage so that operators can
locate the exact failing entry and why it failed.

**Signal improved**
Error messages become actionable without additional logging or instrumentation.

**Test (fails today, passes after)**
Add a test that injects a corrupted index entry and asserts the error message contains:
index name, key fingerprint (or raw key bytes), and the decode stage (key vs entry).

**Acceptance criteria**
- Error paths in `crates/icydb-core/src/db/index/store.rs` and/or
  `crates/icydb-core/src/db/index/entry.rs` include index name and decode stage.
- A regression test asserts on error content and fails on current generic messages.

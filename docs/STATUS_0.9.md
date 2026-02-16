# 0.9 Status (as of 2026-02-15)

## 0.9 Ship Checklist (Strengthening Release)

### Must Finish Before 0.9.0

- [x] Lock weak-relation delete semantics (`no existence validation`) with focused regressions for single, optional, and collection relation fields.  
  Owner: `Codex`
- [x] Close remaining deterministic pre-commit validation gaps for delete/save planning so all fallible work is complete before commit-window open.  
  Owner: `Codex`
- [x] Add final replay regressions for reverse-index correctness across mixed marker sequences (`save -> save -> delete`, retarget updates, and rollback path).  
  Owner: `Codex`
- [x] Standardize error classification at RI boundaries (`Unsupported`, `Corruption`, `Internal`) and verify emitted classes in tests.  
  Owner: `Codex`
- [x] Finalize operator-facing diagnostics for blocked deletes (include enough context to identify source entity/field quickly).  
  Owner: `Codex`
- [x] Verify reverse-index and relation-validation metrics deltas are emitted consistently on success and failure paths.  
  Owner: `Codex`

### Target for 0.9.x (After 0.9.0 Cut if Needed)

- [x] Advance explicit transaction semantics surface (opt-in only; no behavior change for existing batch helpers).  
  Owner: `Codex`
  Progress (2026-02-15): shipped opt-in `*_many_atomic` APIs for single-entity-type batch saves, kept `*_many_non_atomic` semantics unchanged, and added explicit transaction semantics docs plus focused regressions.
  Progress (2026-02-15): added interrupted-marker replay coverage for atomic batch row-op markers and verified replay idempotency.
  Progress (2026-02-15): added API-level hardening tests for atomic/non-atomic `insert_many`/`update_many`/`replace_many` conflict behavior and relation-validation failure paths, plus clearer user-facing lane docs.
- [x] Continue pagination performance work without changing cursor semantics, ordering guarantees, or continuation validation rules.  
  Owner: `Codex`
  Progress (2026-02-15): added bounded top-k ordering for first-page ordered load pagination (`offset=0`, `limit` set, no cursor) to avoid full in-memory sort while preserving output and continuation semantics.
  Progress (2026-02-15): added a PK-ordered full-scan streaming fast path for `order_by(primary_key ASC)` loads (including cursor pagination) with early stop at `offset + limit + 1`.
  Progress (2026-02-15): extended PK-ordered streaming to `AccessPath::KeyRange { start, end }`, preserving key-range bounds plus cursor continuation semantics with early stop.
  Progress (2026-02-15): extended bounded top-k ordering from first-page-only to all non-cursor offset pages via `offset + limit + 1` keep-count, preserving continuation boundary semantics.

### Release Gate

- [ ] `make fmt-check && make clippy && make check && make test` passes on release branch.  
  Owner: `Codex`
- [ ] `docs/old/PLAN_0.9.md`, `docs/ROADMAP.md`, and `CHANGELOG.md` reflect final shipped 0.9 scope.  
  Owner: `Codex`

## 1. Strong Referential Integrity - Delete-Time Validation (~100%)

* Reject deletes that would leave dangling strong references: **100%**
* Perform validation deterministically before commit: **100%**
* Introduce reverse indexes for strong-relation delete validation to avoid full source-store scans: **100%**
* Preserve explicit weak-relation behavior (no existence validation): **100%**
* Keep semantics validation-only (no implicit cascades): **100%**

## 2. Explicit Transaction Semantics (Opt-In Surface) (~100%)

* Keep transactional behavior explicit and opt-in: **100%**
* Preserve existing fail-fast, non-atomic batch helper semantics unless users adopt explicit transaction APIs: **100%**
* Ship formal semantics, recovery behavior, and failure-mode tests alongside any transactional surface: **100%**

## 3. Pagination Efficiency Without Semantic Drift (~100%)

* Reduce full candidate-set work for large ordered paged scans: **100%**
* Preserve canonical ordering and continuation-signature compatibility checks: **100%**
* Keep forward-only, live-state continuation semantics unchanged: **100%**

## 4. Contract Hardening and Diagnostics (~100%)

* Expand structural regression coverage around post-access execution phases: **100%**
* Keep error classification explicit (`Unsupported`, `Corruption`, `Internal`) at execution boundaries: **100%**
* Improve diagnostics where contract violations or corruption are detected: **100%**
* Emit and aggregate reverse-index/relation-validation observability metrics with operation-level deltas: **100%**
* Distinguish system vs user index entries in storage snapshots and enforce reserved `~` namespace constraints in schema validation: **100%**

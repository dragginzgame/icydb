# 0.9 Status (as of 2026-02-14)

## 0.9 Ship Checklist (Referential Integrity Release)

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

- [ ] Advance explicit transaction semantics surface (opt-in only; no behavior change for existing batch helpers).  
  Owner: `Codex`
- [ ] Continue pagination performance work without changing cursor semantics, ordering guarantees, or continuation validation rules.  
  Owner: `Codex`

### Release Gate

- [ ] `make fmt-check && make clippy && make check && make test` passes on release branch.  
  Owner: `Codex`
- [ ] `docs/PLAN_0.9.md`, `docs/ROADMAP.md`, and `CHANGELOG.md` reflect final shipped 0.9 scope.  
  Owner: `Codex`

## 1. Strong Referential Integrity - Delete-Time Validation (~100%)

* Reject deletes that would leave dangling strong references: **100%**
* Perform validation deterministically before commit: **100%**
* Introduce reverse indexes for strong-relation delete validation to avoid full source-store scans: **100%**
* Preserve explicit weak-relation behavior (no existence validation): **100%**
* Keep semantics validation-only (no implicit cascades): **100%**

## 2. Explicit Transaction Semantics (Opt-In Surface) (~15%)

* Keep transactional behavior explicit and opt-in: **25%**
* Preserve existing fail-fast, non-atomic batch helper semantics unless users adopt explicit transaction APIs: **20%**
* Ship formal semantics, recovery behavior, and failure-mode tests alongside any transactional surface: **0%**

## 3. Pagination Efficiency Without Semantic Drift (~35%)

* Reduce full candidate-set work for large ordered paged scans: **15%**
* Preserve canonical ordering and continuation-signature compatibility checks: **55%**
* Keep forward-only, live-state continuation semantics unchanged: **35%**

## 4. Contract Hardening and Diagnostics (~88%)

* Expand structural regression coverage around post-access execution phases: **88%**
* Keep error classification explicit (`Unsupported`, `Corruption`, `Internal`) at execution boundaries: **100%**
* Improve diagnostics where contract violations or corruption are detected: **90%**
* Emit and aggregate reverse-index/relation-validation observability metrics with operation-level deltas: **100%**
* Distinguish system vs user index entries in storage snapshots and enforce reserved `~` namespace constraints in schema validation: **95%**

# Index Integrity Audit - 2026-03-09

## Report Preamble

- scope: index ordering, namespace isolation, unique enforcement parity, and replay integrity
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-08/index-integrity.md`
- code snapshot identifier: `b29df45d`
- method tag/version: `Method V3`
- comparability status: `comparable`

## Invariant Registry

| Invariant | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Key ordering authority remains index-owned | `scripts/ci/check-layer-authority-invariants.sh` (`Comparator definitions outside index: 0`) | PASS | Low |
| Range execution remains byte-oriented in executor paths | `scripts/ci/check-index-range-spec-invariants.sh` | PASS | Low |
| Continuation anchor must remain inside envelope | `crates/icydb-core/src/db/index/scan.rs` (`ensure_anchor_within_envelope`) | PASS | Low |
| Resume must strictly advance | `crates/icydb-core/src/db/index/scan.rs` (`ensure_continuation_advanced`) and `crates/icydb-core/src/db/index/envelope.rs` (`continuation_advanced`) | PASS | Low |
| Index-range plan/runtime spec alignment enforced | `db::executor::context::tests::access_plan_rejects_misaligned_index_range_spec` | PASS | Low-Medium |

## Encoding, Namespace, and Mutation Checks

| Area | Evidence | Result |
| ---- | ---- | ---- |
| Namespace/arity/index-id checks for cursor anchors | `crates/icydb-core/src/db/cursor/anchor.rs` (`validate_index_range_anchor`) | PASS |
| Bound rewrite keeps strict exclusion semantics | `crates/icydb-core/src/db/index/envelope.rs` (`resume_bounds_from_refs`) | PASS |
| Apply sequence is guarded by commit-window protocol | `crates/icydb-core/src/db/executor/mutation/commit_window.rs` (`open_commit_window`) | PASS |
| Recovery replay path remains deterministic | `crates/icydb-core/src/db/commit/recovery.rs` + `replay_commit_marker_row_ops` | PASS |

## Overall Index Integrity Risk Index

**3/10**

## Follow-Up Actions

- None required for this run.

## Verification Readout

- `bash scripts/ci/check-index-range-spec-invariants.sh` -> PASS
- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `cargo test -p icydb-core access_plan_rejects_misaligned_index_range_spec -- --nocapture` -> PASS
- `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture` -> PASS

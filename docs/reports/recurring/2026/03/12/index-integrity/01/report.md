# Index Integrity Audit - 2026-03-12

## Report Preamble

- scope: index ordering, namespace isolation, unique enforcement parity, and replay integrity
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-10/index-integrity-2.md`
- code snapshot identifier: `f12b0b74`
- method tag/version: `Method V3`
- comparability status: `comparable`

## Findings

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Range-plan/runtime spec alignment invariants hold | `bash scripts/ci/check-index-range-spec-invariants.sh` | PASS | Low |
| Comparator and layer authority remain index-owned | `bash scripts/ci/check-layer-authority-invariants.sh` | PASS | Low |
| Misaligned index-range specs are rejected at plan construction | `cargo test -p icydb-core access_plan_rejects_misaligned_index_range_spec -- --nocapture` | PASS | Low-Medium |
| Continuation anchor containment guard rejects invalid anchors | `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture` | PASS | Low-Medium |
| Unique-conflict class parity between live apply and replay remains locked | `cargo test -p icydb-core unique_conflict_classification_parity_holds_between_live_apply_and_replay -- --nocapture` | PASS | Low-Medium |
| Replay of interrupted conflicting unique batch fails closed | `cargo test -p icydb-core recovery_replay_interrupted_conflicting_unique_batch_fails_closed -- --nocapture` | PASS | Low-Medium |

## Overall Index Integrity Risk Index

**3/10**

## Follow-Up Actions

- None required for this run.

## Verification Readout

- `bash scripts/ci/check-index-range-spec-invariants.sh` -> PASS
- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `cargo test -p icydb-core access_plan_rejects_misaligned_index_range_spec -- --nocapture` -> PASS
- `cargo test -p icydb-core anchor_containment_guard_rejects_out_of_envelope_anchor -- --nocapture` -> PASS
- `cargo test -p icydb-core unique_conflict_classification_parity_holds_between_live_apply_and_replay -- --nocapture` -> PASS
- `cargo test -p icydb-core recovery_replay_interrupted_conflicting_unique_batch_fails_closed -- --nocapture` -> PASS

# State Machine & Transition Integrity Audit - 2026-03-08

Scope: planner -> execution -> commit -> recovery transition correctness.

## Transition Integrity

| Transition | Validation Before Mutation | Result | Risk |
| ---- | ---- | ---- | ---- |
| Plan validation -> executable plan | yes (`query/plan/validate/*`) | PASS | Low |
| Commit-window open -> apply | yes (`open_commit_window` preflight before `begin_commit`) | PASS | Medium |
| Apply success/failure marker semantics | success clears marker; failure preserves marker | PASS | Low |
| Startup/write recovery gate | `ensure_recovered` invoked before write paths | PASS | Low |

## Attack Scenarios

| Scenario | Expected Safety Behavior | Current Outcome | Risk |
| ---- | ---- | ---- | ---- |
| Failure during index update | marker retained, replay authority restores consistency | PASS | Medium |
| Failure after marker persistence but before full apply | durable marker remains and replay continues | PASS | Medium |
| Recovery replay retry | idempotent replay behavior | PASS (`recovery_replay_is_idempotent`) | Low-Medium |

## Drift Sensitivity

- The highest coordination-risk seam remains continuation/grouped routing interactions.
- No state-machine break was identified in this run.

## Verification Readout

- `cargo test -p icydb-core recovery_replay_is_idempotent -- --nocapture` -> PASS
- `cargo check -p icydb-core` -> PASS

## Overall State-Machine Risk Index

**4/10**

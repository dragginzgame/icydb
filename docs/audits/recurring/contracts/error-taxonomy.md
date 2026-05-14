# WEEKLY AUDIT - Strict Error Taxonomy

`icydb-core` plus the public `icydb` facade.

---

## Purpose

Verify that error classifications:

- preserve class and origin across internal layers
- preserve public facade mappings
- never downgrade corruption or invariant failures
- never escalate user/query policy failures into corruption
- keep cursor, planner, executor, store, recovery, schema, and facade
  boundaries explicit

This is a classification audit only.

Do not:

- suggest renaming
- propose refactors
- discuss style
- discuss performance
- propose architecture changes

Only verify classification correctness and list violations.

---

## Current Taxonomy

Use the current `icydb-core::error::ErrorClass` taxonomy:

| ErrorClass | Applies When | Must Preserve |
| ---------- | ------------ | ------------- |
| `Corruption` | trusted persisted bytes or structural storage/index/row payloads are invalid | never downgraded to query/user policy |
| `IncompatiblePersistedFormat` | persisted bytes are well-formed enough to identify an unsupported stored format/version | serialize/store origin fidelity |
| `NotFound` | operation expectation did not find required data | not escalated to corruption |
| `Internal` | runtime failure not represented by a narrower class | not disguised as query validation |
| `Conflict` | operation expectation conflicts with current state | not escalated to corruption |
| `Unsupported` | feature, query shape, cursor contract, schema transition, or value policy is intentionally unsupported | not reclassified as invariant unless an internal boundary was violated |
| `InvariantViolation` | internal logical assumption or layer contract is violated despite well-formed bytes | never downgraded to unsupported/query policy |

Use the current `icydb-core::error::ErrorOrigin` taxonomy:

- `Serialize`
- `Store`
- `Index`
- `Identity`
- `Query`
- `Planner`
- `Cursor`
- `Recovery`
- `Response`
- `Executor`
- `Interface`

The public facade maps internal classes to `icydb::RuntimeErrorKind` and
internal origins to `icydb::ErrorOrigin`. Query validation/intent/plan/response
errors map to `icydb::QueryErrorKind`.

Cursor malformed payloads, signature mismatches, window mismatches, and boundary
type mismatches are currently `Unsupported` with `Cursor` origin. Cursor
executor contract failures are `InvariantViolation` with `Cursor` origin.

---

## Required Scope

Enumerate or sample with explicit coverage for:

- `InternalError`
- `ErrorClass`
- `ErrorOrigin`
- `ErrorDetail`
- `StoreError`
- `QueryErrorDetail`
- `PlanError`
- `QueryError`
- `QueryExecutionError`
- `ResponseError`
- `CursorDecodeError`
- `CursorPlanError`
- `IdentityDecodeError`
- serialize/storage key/index key/row decode errors
- store registry errors
- commit marker and recovery errors
- schema reconciliation and schema mutation runner errors
- public `icydb::Error`, `RuntimeErrorKind`, `QueryErrorKind`, and
  `icydb::ErrorOrigin`

No audit run needs to paste every enum variant when command-backed matrices
already freeze the variant surface, but the report must state which surfaces
were checked and which were sampled.

---

## Required Checks

### 1. Class Mapping

Verify:

- every `ErrorClass` maps to the corresponding public runtime kind
- `QueryExecutionError` preserves the wrapped `InternalError` class
- helper methods such as `with_message` and `with_origin` preserve class
- `ErrorDetail` payloads do not contradict class/origin

Produce:

| Source | Internal Class | Public Class | Preserved? | Risk |

### 2. Origin Fidelity

Verify:

- store-origin errors are not reported as planner/executor errors
- index-origin errors stay index-origin
- serialize-origin corruption and incompatible formats stay serialize-origin
- cursor-origin errors stay cursor-origin
- recovery-origin errors are not collapsed into generic executor/query origin
- facade mappings preserve internal origin

Produce:

| Source | Internal Origin | Public Origin | Preserved? | Risk |

### 3. Corruption Containment

Verify:

- trusted persisted-byte decode failures stay `Corruption`
- commit marker structural decode failures stay corruption or incompatible
  persisted format as appropriate
- row/index/storage key corruption is not exposed as query validation
- user/input parsing does not manufacture `Corruption`

Produce:

| Corruption Surface | Final Classification | Origin | Correct? | Risk |

### 4. Unsupported and Policy Containment

Verify:

- unsupported SQL/query/schema-transition features stay `Unsupported`
- cursor contract mismatches stay `Unsupported` unless they are internal
  executor cursor invariants
- indexability/value policy failures stay `Unsupported`
- unsupported paths do not become corruption or invariant violations unless a
  layer conversion received the wrong domain

Produce:

| Unsupported Surface | Final Classification | Origin | Correct? | Risk |

### 5. Invariant Violation Preservation

Verify:

- planner/executor disagreement remains `InvariantViolation`
- index/row logical mismatch with well-formed bytes remains
  `InvariantViolation`
- store invariant details stay store-origin
- schema mutation publication/runtime contract failures stay fail-closed and do
  not become generic unsupported unless they are intentionally policy fences

Produce:

| Invariant Surface | Propagation Path | Classification Preserved? | Risk |

### 6. Query and Facade Mapping

Verify:

- `QueryError::Validate`, `Intent`, and `Plan` map to query kinds
- `ResponseError::NotFound` and `NotUnique` map to response origin
- execute errors preserve runtime class and origin through public facade
- public Candid-facing error enums still contain all mapped variants

Produce:

| Query/Facade Surface | Public Kind | Public Origin | Correct? | Risk |

### 7. Cross-Path Consistency

Verify classification consistency between:

- normal write/apply and recovery replay
- unique conflicts in live apply and replay
- commit marker decode and startup recovery
- save/replace/delete expectation failures
- cursor decode, cursor planning, and grouped cursor revalidation

Produce:

| Scenario | Normal Classification | Replay/Alternate Classification | Consistent? | Risk |

### 8. Layer Violation Detection

Detect:

- lower-layer errors inspecting higher-layer types
- planner wrapping executor errors improperly
- executor reclassifying planner errors
- recovery reinterpreting planner/query errors
- schema mutation startup converting accepted schema authority failures into
  user input errors

Produce:

| Violation | Location | Classification Impact | Risk |

### 9. Mixed-Domain Pressure

Identify enums or conversion helpers that mix:

- corruption and unsupported policy
- invariant violation and unsupported policy
- public query kinds and runtime classes
- recovery/store failures and query planner errors

Produce:

| Enum / Helper | Mixed Domains? | Guarded By | Risk |

### 10. Incorrect Classification List

List:

- misclassifications
- downgrade risks
- escalation risks
- origin mismatches
- stale audit assumptions

If none are found, state that explicitly.

### 11. Overall Taxonomy Risk Index

Taxonomy Risk Index:

- `1-3` = low risk / structurally healthy
- `4-6` = moderate risk / manageable pressure
- `7-8` = high risk / requires monitoring
- `9-10` = critical risk / structural instability

---

## Output Contract

Write one dated result file for each run:

- `docs/audits/reports/YYYY-MM/YYYY-MM-DD/error-taxonomy*.md`

Report sections:

1. Run Metadata + Comparability Note
2. Method Changes, when method changed
3. Class Mapping
4. Origin Fidelity
5. Corruption Containment
6. Unsupported and Policy Containment
7. Invariant Violation Preservation
8. Query and Facade Mapping
9. Cross-Path Consistency
10. Layer Violation Detection
11. Mixed-Domain Pressure
12. Incorrect Classification List
13. Overall Taxonomy Risk Index
14. Verification Readout
15. Follow-Up Actions

Run metadata must include:

- compared baseline report path
- code snapshot identifier
- method tag/version
- comparability status (`comparable` or `non-comparable` with reason)

Do not overwrite prior dated results.

---

## Baseline Verification Commands

Start with:

- `cargo test -p icydb-core error::tests -- --nocapture`
- `cargo test -p icydb error::tests -- --nocapture`
- `cargo test -p icydb-core db::query::intent::errors::tests -- --nocapture`
- `cargo test -p icydb-core db::cursor::tests -- --nocapture`
- `cargo test -p icydb-core db::commit::store::tests::commit_marker -- --nocapture`
- `cargo test -p icydb-core recovery_rejects_corrupt_marker_data_key_decode -- --nocapture`
- `cargo test -p icydb-core recovery_rejects_incompatible_marker_format_version_fail_closed -- --nocapture`
- `cargo test -p icydb-core unique_conflict_classification_parity_holds_between_live_apply_and_replay -- --nocapture`
- `cargo test -p icydb-core conditional_unique_conflict_classification_parity_holds_between_live_update_and_replay -- --nocapture`
- `cargo test -p icydb-core db::schema::mutation::tests::planning::runner_outcome_classifies_missing_capabilities_and_unsupported_requirements -- --nocapture`
- `cargo test -p icydb-core schema_mutation_publication_boundary_uses_runner_preflight --features sql -- --nocapture`
- `bash scripts/ci/check-layer-authority-invariants.sh`

Add targeted tests for any new public error kind, internal error class,
origin, schema mutation runner failure, persisted decode path, recovery replay
path, or facade mapping.

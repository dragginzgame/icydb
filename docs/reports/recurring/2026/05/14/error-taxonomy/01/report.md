# Error Taxonomy Audit - 2026-05-14

## Run Metadata + Comparability Note

- scope: internal and public error class/origin preservation across core,
  facade, cursor, query intent, recovery, commit marker, schema mutation, and
  layer authority boundaries
- compared baseline report path:
  `docs/audits/reports/2026-03/2026-03-24/error-taxonomy.md`
- code snapshot identifier: `499a8478a` plus local uncommitted audit/design and
  schema-reconcile split changes
- method tag/version: `Method V4`
- comparability status: `non-comparable`
- non-comparable reason: Method V4 replaces the stale five-domain audit frame
  with the current seven-class `ErrorClass` taxonomy and adds facade,
  recovery, commit-marker, and schema-mutation coverage.

## Method Changes

- Replaced stale semantic domains with current `ErrorClass` variants:
  `Corruption`, `IncompatiblePersistedFormat`, `NotFound`, `Internal`,
  `Conflict`, `Unsupported`, and `InvariantViolation`.
- Updated cursor taxonomy: malformed payloads, signature mismatches, window
  mismatches, and boundary type mismatches are `Unsupported` with `Cursor`
  origin; cursor executor contract failures are `InvariantViolation`.
- Added public facade mapping checks for `icydb::Error`, runtime kinds, query
  kinds, and public origins.
- Added commit-marker/recovery and schema-mutation classification surfaces.
- Added explicit baseline commands so future runs cannot collapse into broad
  smoke coverage.

## Class Mapping

| Source | Internal Class | Public Class | Preserved? | Risk |
| ------ | -------------- | ------------ | ---------- | ---- |
| `InternalError` class matrix | all seven current `ErrorClass` variants | matching `RuntimeErrorKind` variants | yes | low |
| `QueryExecutionError` wrappers | wrapped `InternalError` class | matching public runtime kind | yes | low |
| `with_message` / `with_origin` helpers | original class | original class | yes | low |
| cursor invalid payload/signature/window/type mismatch | `Unsupported` | `Unsupported` when surfaced as runtime error | yes | low |
| cursor executor invariant | `InvariantViolation` | `InvariantViolation` | yes | low |
| schema mutation missing capability / unsupported requirement | `Unsupported` policy result | remains unsupported/fail-closed | yes | low |

## Origin Fidelity

| Source | Internal Origin | Public Origin | Preserved? | Risk |
| ------ | --------------- | ------------- | ---------- | ---- |
| index corruption constructor | `Index` | `Index` | yes | low |
| store corruption constructor | `Store` | `Store` | yes | low |
| serialize corruption / incompatible format | `Serialize` | `Serialize` | yes | low |
| cursor plan/runtime mapping | `Cursor` | `Cursor` | yes | low |
| query unsupported constructors | `Query` | `Query` | yes | low |
| query execution storage/index errors | `Store` / `Index` | `Store` / `Index` | yes | low |
| response not-found / not-unique | `Response` | `Response` | yes | low |

## Corruption Containment

| Corruption Surface | Final Classification | Origin | Correct? | Risk |
| ------------------ | -------------------- | ------ | -------- | ---- |
| index key/payload corruption | `Corruption` | `Index` | yes | low |
| store row/key corruption | `Corruption` | `Store` | yes | low |
| serialize persisted payload corruption | `Corruption` | `Serialize` | yes | low |
| incompatible persisted marker format | `IncompatiblePersistedFormat` | recovery/serialize path remains fail-closed | yes | low |
| corrupt marker data-key decode during recovery | corruption/fail-closed recovery classification | recovery path | yes | low |
| commit marker oversized stored payload | `Corruption` | store/commit marker path | yes | low |

## Unsupported and Policy Containment

| Unsupported Surface | Final Classification | Origin | Correct? | Risk |
| ------------------- | -------------------- | ------ | -------- | ---- |
| unsupported SQL feature | `Unsupported` | `Query` | yes | low |
| unknown aggregate target field execution boundary | `Unsupported` | `Query` | yes | low |
| cursor invalid payload and signature mismatch | `Unsupported` | `Cursor` | yes | low |
| grouped scalar DISTINCT policy violation before scan | unsupported/fail-closed query policy | query/executor boundary | yes | low |
| schema mutation unsupported plan / missing capability | unsupported/fail-closed schema mutation report | schema/query policy boundary | yes | low |
| unsupported entity path in recovery | unsupported/fail-closed recovery path | recovery path | yes | low |

## Invariant Violation Preservation

| Invariant Surface | Propagation Path | Classification Preserved? | Risk |
| ----------------- | ---------------- | ------------------------- | ---- |
| access plan conversion invariant | access plan -> internal error | yes | low |
| planner policy invariant | planner policy -> internal planner invariant | yes | low |
| group-plan wrong-domain conversion | group-plan conversion guard -> planner invariant | yes | low |
| cursor executor invariant | cursor plan error -> internal cursor invariant | yes | low |
| query executor invariant | internal query error -> execution/facade runtime kind | yes | low |
| schema mutation publication preflight guard | source guard -> test boundary | yes | low |

## Query and Facade Mapping

| Query/Facade Surface | Public Kind | Public Origin | Correct? | Risk |
| -------------------- | ----------- | ------------- | -------- | ---- |
| `QueryError::Validate` | `Query(Validate)` | `Query` | yes | low |
| `QueryError::Intent` | `Query(Intent)` | `Query` | yes | low |
| `QueryError::Plan` | `Query(Plan)` or `Query(UnorderedPagination)` | `Query` | yes | low |
| `ResponseError::NotFound` / `NotUnique` | `Query(NotFound)` / `Query(NotUnique)` | `Response` | yes | low |
| `QueryError::Execute` | matching `RuntimeErrorKind` | mapped internal origin | yes | low |
| Candid-facing facade shape | stable runtime/query/origin labels | n/a | yes | low |

## Cross-Path Consistency

| Scenario | Normal Classification | Replay / Alternate Classification | Consistent? | Risk |
| -------- | --------------------- | --------------------------------- | ----------- | ---- |
| unique conflict live apply vs replay | `Conflict` | `Conflict` | yes | low |
| conditional unique conflict live update vs replay | `Conflict` | `Conflict` | yes | low |
| commit marker current/future/older decode | accepted or fail-closed class | same recovery/commit marker class | yes | low |
| corrupt marker key decode | corruption/fail-closed | recovery fail-closed | yes | low |
| cursor decode vs grouped cursor revalidation | unsupported policy or cursor invariant as appropriate | same cursor class family | yes | low |
| schema mutation missing capabilities vs publication preflight | unsupported/fail-closed policy | fail-closed guard | yes | low |

## Layer Violation Detection

| Violation | Location | Classification Impact | Risk |
| --------- | -------- | --------------------- | ---- |
| lower layer imports higher layer taxonomy | `check-layer-authority-invariants.sh` | none found | low |
| planner/executor error ownership drift | route/layer authority snapshot | no tracked authority violation | low-medium |
| schema mutation startup converts accepted-schema authority failure into user input | schema publication guard | not observed | low |

## Mixed-Domain Pressure

| Enum / Helper | Mixed Domains? | Guarded By | Risk |
| ------------- | -------------- | ---------- | ---- |
| `CursorPlanError` | yes, unsupported cursor contracts plus cursor invariants | explicit conversion matrix | low |
| `PlanError` | yes, user/policy/cursor planning families | grouped/domain conversion guards | low-medium |
| `QueryExecutionError` | yes, wraps runtime class families | class-preserving matrix | low |
| `InternalError` constructors | yes, all runtime classes | constructor and helper tests | low |
| public `icydb::Error` | yes, query and runtime categories | facade Candid shape and mapping tests | low |

## Incorrect Classification List

No current implementation misclassification was found.

The stale audit assumption that malformed cursors are `Invalid Input` has been
removed from the recurring definition. Current implementation and tests treat
cursor contract mismatches as `Unsupported` with `Cursor` origin unless the
cursor path detects an executor/runtime contract invariant.

## Overall Taxonomy Risk Index

**2/10**

The taxonomy is structurally healthy. The main risk was audit drift, not
runtime behavior. Method V4 now tracks the current class/origin model and the
public facade mapping.

## Verification Readout

- `cargo test -p icydb-core error::tests -- --nocapture` -> PASS
- `cargo test -p icydb error::tests -- --nocapture` -> PASS
- `cargo test -p icydb-core db::query::intent::errors::tests -- --nocapture` -> PASS
- `cargo test -p icydb-core db::cursor::tests -- --nocapture` -> PASS
- `cargo test -p icydb-core db::commit::store::tests::commit_marker -- --nocapture` -> PASS
- `cargo test -p icydb-core recovery_rejects_corrupt_marker_data_key_decode -- --nocapture` -> PASS
- `cargo test -p icydb-core recovery_rejects_incompatible_marker_format_version_fail_closed -- --nocapture` -> PASS
- `cargo test -p icydb-core unique_conflict_classification_parity_holds_between_live_apply_and_replay -- --nocapture` -> PASS
- `cargo test -p icydb-core conditional_unique_conflict_classification_parity_holds_between_live_update_and_replay -- --nocapture` -> PASS
- `cargo test -p icydb-core db::schema::mutation::tests::planning::runner_outcome_classifies_missing_capabilities_and_unsupported_requirements -- --nocapture` -> PASS
- `cargo test -p icydb-core schema_mutation_publication_boundary_uses_runner_preflight --features sql -- --nocapture` -> PASS
- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS

## Follow-Up Actions

No runtime follow-up is required. Keep Method V4 as the next comparable
baseline, and add targeted checks whenever a new public error kind, internal
error class, origin, schema mutation runner failure, persisted decode path,
recovery replay path, or facade mapping is introduced.

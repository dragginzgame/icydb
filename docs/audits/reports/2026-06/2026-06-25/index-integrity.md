# Index Integrity Audit - 2026-06-25

## Run Metadata + Comparability Note

- scope: `index-integrity`
- recurring definition:
  `docs/audits/recurring/access/access-index-integrity.md`
- compared baseline report path:
  `docs/audits/reports/2026-05/2026-05-11/index-integrity.md`
- code snapshot identifier: `d389eec3bd` (`dirty` working tree)
- run timestamp: `2026-06-25T10:45:53+02:00`
- method tag/version: `Method V5`
- comparability status: `non-comparable`
  - Method V5 refreshes Method V4 for the current SQL DDL index publication
    surface
  - the stale standalone `write_boundary_guards` target no longer exists and
    was replaced with focused accepted-authority filters
  - future V5 runs should compare directly to this report
- run mode: same-day follow-up implementation plus validation
  - initial Method V5 audit was read-only
  - follow-up changed expression-index DDL failure handling and added regression
    proof
  - generated artifacts, package manifests, lockfiles, and release metadata were
    not modified for this follow-up
  - no external service was started or stopped

The working tree was already dirty and changed further during the run in areas
outside this audit. This report treats those files only as current snapshot
context and relies on passing focused checks for evidence.

## Method Changes

- Added explicit `Method V5` identity to the recurring definition.
- Updated catalog mutation readiness for the current split between startup
  reconciliation and SQL DDL execution.
- Replaced the removed `write_boundary_guards` integration target with focused
  accepted-index, accepted-expression, accepted-relation, transition,
  reconciliation, and SQL DDL frontend authority filters.
- Added SQL DDL field-path, expression-index, unique-index, drop, generated-drop
  rejection, and cache-invalidation probes to the baseline.
- Added expression-index DDL rollback proof for post-insert validation failure.
- Added read-only run-mode and normalized verification statuses.

## Index Invariant Registry

| Invariant | Category | Enforced Where |
| --- | --- | --- |
| Raw index keys sort lexicographically in traversal order | Ordering | `IndexStore`, `IndexKey`, canonical ordering tests |
| Index id and namespace are encoded in index keys | Namespace | `IndexId`, `IndexKey::try_from_raw`, cursor/index guard tests |
| Key encode/decode rejects malformed or partial keys | Structural | index codec and route guard coverage |
| Forward index mutations are coupled to row writes | Mutation | commit preflight, accepted schema write contracts |
| Reverse relation indexes use accepted relation contracts | Mutation | accepted schema info relation authority and reverse rebuild paths |
| Unique validation is equivalent live and on replay | Recovery | unique conflict parity and interrupted replay tests |
| Runtime visible indexes are accepted-schema-backed | Accepted Authority | `SchemaInfo`, `VisibleIndexes`, planner/explain/cache paths |
| DDL-published indexes remain accepted metadata | Catalog Mutation | SQL DDL execution, schema transition, reconcile preservation |
| Unsupported/generated index mutations fail closed | Catalog Mutation | SQL DDL generated-drop rejection and mutation planning gates |

## Encode / Decode Symmetry Table

| Key Type | Symmetric? | Failure Mode | Risk |
| --- | --- | --- | --- |
| Raw index key | Yes | malformed bytes reject during decode | Low |
| Accepted field-path index key | Yes | invalid component shape fails closed | Low |
| Accepted expression index key | Yes for supported deterministic subset | unsupported expression shape rejects | Low |
| Cursor raw anchor key | Yes | index-id or canonical re-encode mismatch rejects | Low |

## Ordering Stability Analysis

| Case | Lexicographically Stable? | Why | Risk |
| --- | --- | --- | --- |
| Negative/positive scalar components | Yes | canonical encoded value ordering remains guarded | Low |
| Equal boundary with exclusive edge | Yes | empty envelopes are rejected before scan | Low |
| Composite key prefix ordering | Yes | accepted key item order drives construction | Low |
| DESC scan continuation | Yes | direction-owned continuation remains guarded | Low |
| SQL `DESC` index publication | Out of scope | DDL rejects unsupported index order before publication | Low |

## Namespace Isolation Table

| Scenario | Can Cross-Decode? | Prevented Where | Risk |
| --- | --- | --- | --- |
| Key from index A used as index B anchor | No | index-id validation | Low |
| User/data key decoded as index key | No | key namespace validation | Low |
| Wrong component arity | No | accepted key contract checks | Low |
| DDL-published index confused with generated index | No | origin/generation checks and generated-drop rejection | Low |

## Entry Layout Analysis

| Entry Component | Layout Stable? | Decode Safe? | Risk |
| --- | --- | --- | --- |
| Raw key bytes | Yes | bounded decode and canonical key validation | Low |
| Entry payload | Yes | store-owned raw entry structure | Low |
| Index id | Yes | encoded into key namespace | Low |
| Primary key payload | Yes | accepted contract key construction and replay checks | Low |

## Reverse Relation Integrity

| Flow | Reverse Mutation Symmetric? | Orphan Risk | Replay Risk |
| --- | --- | --- | --- |
| Save relation row | Yes | Low | Low |
| Delete relation row | Yes | Low | Low |
| Replace relation row | Yes | Low | Low |
| Recovery/startup rebuild | Yes, with accepted relation authority | Low | Low |

## Unique Enforcement Equivalence

| Scenario | Unique Enforced? | Recovery Enforced? | Live/Replay Class Parity? | Risk |
| --- | --- | --- | --- | --- |
| Duplicate unique insert | Yes | Yes | Yes | Low |
| Interrupted conflicting unique batch | Yes | Yes | Yes | Low |
| DDL unique field-path index publication | Yes | N/A for publication path | N/A | Low |
| DDL unique expression index publication | Yes | N/A for publication path | N/A | Low |
| Duplicate unique DDL values | Yes, rejects before publication | N/A | N/A | Low |

## Partial-Update Membership Transitions

| Transition Case | Prepared Ops Minimal? | Symmetric? | Risk |
| --- | --- | --- | --- |
| old indexed value -> new indexed value | Yes | Yes | Low |
| old `null` -> new value | Yes | Yes | Low |
| old value -> `null` | Yes | Yes | Low |
| membership moves across indexes | Yes | Yes | Low |
| unchanged indexed field | Expected no-op | Yes | Low |

## Mixed Unique+Reverse+Secondary Ordering Check

| Scenario | Uniqueness Verdict Happens First? | Any Side Effects Before Verdict? | Risk |
| --- | --- | --- | --- |
| conflicting unique plus reverse and secondary changes | Yes | No evidence of side-effectful mutation before verdict | Low |
| duplicate unique DDL values | Yes | no accepted schema or physical target keys published | Low |

## Accepted Runtime Index Authority

| Runtime Lane | Authority Source | Generated Metadata Present? | Allowed? | Risk |
| --- | --- | --- | --- | --- |
| Visible index selection | `SchemaInfo` from accepted snapshots | No on accepted runtime lane | Yes | Low |
| Planner candidate construction | accepted semantic index contracts | model-only lane remains separate | Yes | Low |
| Predicate/index membership | accepted field-path/expression contracts | No fallback found in validated path | Yes | Low |
| Key shape and encoding | accepted index metadata | generated convenience remains model-only | Yes | Low |
| Forward writes | accepted row/index contracts | No generated-only write fallback in evidence | Yes | Low |
| Reverse writes | accepted relation contracts | generated proposal remains out of runtime lane | Yes | Low |
| Uniqueness validation | accepted field-path/expression authority | generated split remains named separately | Yes | Low |
| Explain/cache identity | accepted visible indexes and fingerprints | no generated cache identity in proof | Yes | Low |

Required fail-closed checks:

| Scenario | Expected Behavior | Evidence | Risk |
| --- | --- | --- | --- |
| Accepted index metadata missing after reconciliation | reject or keep invisible | schema transition/reconcile filters passed | Low |
| Stale accepted index fingerprint | distinct planner/cache identity | DDL cache invalidation probe passed | Low |
| Dropped DDL secondary index still in store | clean target namespace before metadata removal | DDL drop probe passed | Low |
| Runtime write sees generated-only index metadata | unreachable/rejected outside model-only lane | accepted-authority filters passed | Low |

## Catalog Mutation Readiness

| Mutation | Compatibility | Rebuild Requirement | Runtime Visibility Boundary | Risk |
| --- | --- | --- | --- | --- |
| Add nullable/defaulted field | metadata-only | none | publishable after schema gate | Low |
| Startup field-path index addition | requires index rebuild | physical field-path build | supported through startup runtime-store gate | Low |
| SQL DDL field-path index addition | requires index rebuild | physical field-path build | supported after accepted-before identity and physical validation | Low |
| SQL DDL expression index addition | requires index rebuild | physical expression build | supported after accepted-before identity and physical validation | Low |
| SQL DDL unique index addition | requires index rebuild | physical build plus uniqueness validation | supported; duplicate values reject before publication | Low |
| SQL DDL DDL-owned index drop | requires cleanup | target namespace cleanup | supported before accepted metadata removal | Low |
| Generated index drop | unsupported | none | rejected before execution/publication | Low |
| SQL `DESC` index order | unsupported | none | rejected before execution/publication | Low |
| Full data rewrite / key-shape change | incompatible | full rewrite | blocked | Low |

## Row/Index Coupling Analysis

| Failure Point | Divergence Possible? | Prevented? | Risk |
| --- | --- | --- | --- |
| Failure after index insert before row write | Low | commit/replay and accepted preflight | Low |
| Failure after row write before index insert | Low | commit/replay and index planning | Low |
| Failure during DDL field-path build | Low | staged build, validation, and publication gate | Low |
| Failure during DDL expression build | Low | rollback removes staged keys and restores ready state before returning the validation error | Low |
| Failure during DDL index drop | Low | target cleanup validation before metadata removal | Low |

## Replay Equivalence Table

| Phase | Normal | Replay | Equivalent? | Risk |
| --- | --- | --- | --- | --- |
| Save | accepted row/index preflight | accepted commit preflight | Yes | Low |
| Delete | accepted row/relation contract | accepted delete/recovery path | Yes | Low |
| Replace | before/after accepted contracts | replay uses same accepted mutation facts | Yes | Low |
| Unique conflict | `Conflict`, index-owned origin | `Conflict`, recovery-owned origin | Yes | Low |

## Cross-Layer Continuation Stability

| Scenario | Resurrection? | Skip? | Duplicate? | Risk |
| --- | --- | --- | --- | --- |
| Delete inside active cursor window | No | No | No | Low |
| Inverted key range | No scan | No | No | Low |
| Composite range pagination | No | No | No | Low |

## High Risk Mutation Paths

| Path | Complexity | Divergence Risk | Risk Level |
| --- | --- | --- | --- |
| SQL DDL expression-index build | High | guarded by accepted target, physical validation, and rollback on post-insert validation failure | Low |
| SQL DDL index drop cleanup | Medium | guarded by target namespace cleanup before publication | Moderate |
| Startup field-path index rebuild | Medium | guarded by row/schema fingerprints and final store validation | Low |
| Generated index drop attempt | Low | rejected before execution | Low |
| Planner/cache invalidation after accepted fingerprint change | Medium | DDL invalidates schema-scoped cache key | Low |

## Storage-Layer Assumptions

| Assumption | Required For | Violation Impact |
| --- | --- | --- |
| Key comparison is byte-wise deterministic | index traversal order | order drift / cursor errors |
| Store iteration remains deterministic | replay, rebuild, and pagination | non-deterministic pages |
| Commit replay sees durable marker state | row/index atomicity | divergence after interruption |
| No external mutation of raw index store | namespace and coupling | orphan or stale entries |
| No concurrent writes during DDL publication | DDL physical/schema coupling | stale physical/schema publication |

## Overall Index Risk Index

**3/10**

Runtime index integrity remains structurally healthy. The score drops below
`4/10` because the main moderate expression-index build risk now has explicit
rollback on post-insert validation failure. SQL DDL still publishes a broader
field-path, expression, unique, and drop surface than the May V4 report, but the
remaining risk is bounded by accepted-before identity checks, physical
validation before accepted schema publication, generated-index rejection,
rollback proof, and passing cache/reconcile checks.

## Verification Readout

| Check | Status | Result |
| --- | --- | --- |
| Audit definition fitness | PASS | Definition updated to Method V5 for current SQL DDL index mutation lanes |
| Stale `write_boundary_guards` target | BLOCKED | Target no longer exists; V5 replaces it with focused accepted-authority filters |
| Inverted primary-key range cursor guard | PASS | 1 test passed |
| Aggregate fast-path non-exact range arity guard | PASS | 1 test passed |
| Aggregate fast-path prefix-spec guard | PASS | 1 test passed |
| Cross-layer canonical ordering | PASS | 1 test passed |
| Unique live/replay conflict parity | PASS | 1 test passed |
| Interrupted conflicting unique replay | PASS | 1 test passed |
| Live cursor delete between pages | PASS | 1 test passed |
| Accepted field-path index metadata authority | PASS | 1 test passed |
| Accepted expression index metadata authority | PASS | 1 test passed |
| Accepted strong relation authority | PASS | 1 test passed |
| Supported DDL indexes absent from generated model | PASS | 1 test passed |
| Reconcile preserves DDL indexes during generated rename | PASS | 1 test passed |
| SQL DDL frontend does not take schema-store/generated-index authority | PASS | 1 test passed |
| Expression-index post-insert validation rollback | PASS | 1 test passed |
| SQL DDL field-path index publication | PASS | 1 test passed |
| SQL DDL expression-index publication | PASS | 1 test passed |
| SQL DDL unique field-path publication | PASS | 1 test passed |
| SQL DDL unique expression publication | PASS | 1 test passed |
| SQL DDL duplicate unique field-path rejection | PASS | 1 test passed |
| SQL DDL duplicate unique expression rejection | PASS | 1 test passed |
| SQL DDL DDL-owned index drop | PASS | 1 test passed |
| SQL DDL generated-index drop rejection | PASS | 1 test passed |
| SQL DDL cache invalidation | PASS | 1 test passed |
| `schema::mutation` focused filter | PASS | 65 tests passed |
| `schema::reconcile` focused filter | PASS | 26 tests passed |
| `make check-invariants` | PASS | All invariant scripts passed |
| `git diff --check` | PASS | No whitespace errors reported |

## Follow-Up Actions

- Keep future `index-integrity` runs on Method V5 so DDL-published field-path,
  expression, unique, drop, and rollback behavior remains comparable.
- Treat the removed `write_boundary_guards` integration target as retired; do
  not reintroduce it into reports unless a new target is actually added.
- Watch the dirty prepared/explain/cache work in the next run because accepted
  index fingerprints and shared plan cache identity are part of this audit.

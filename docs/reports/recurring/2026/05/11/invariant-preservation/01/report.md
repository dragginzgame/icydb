# Invariant Preservation Audit - 2026-05-11

## Run Metadata + Comparability Note

- scope: `invariant-preservation`
- recurring definition: `docs/audits/recurring/integrity/invariant-preservation.md`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-12/invariant-preservation.md`
- code snapshot identifier: `a4ed38245`
- method tag/version: `Method V4`
- comparability status: `non-comparable` - Method V4 adds accepted runtime authority, catalog mutation publication, and recovery-before-rebuild checks after the schema/index authority migration. The 2026-03-12 report only covered memory-id, field-projection, and replay-idempotence checks.

## Invariant Registry

| Invariant | Category | Subsystem(s) Impacted |
| --- | --- | --- |
| Entity primary key matches storage key | Identity | row decode, cursor boundary, recovery replay |
| Index id consistency | Identity | index key codec, cursor anchors, access planning |
| Key namespace consistency | Identity | index store, cursor anchors, recovery rebuild |
| Component arity stability | Identity | index key encoding, planner range specs, cursor anchors |
| Raw index key ordering is canonical | Ordering | index store traversal, range scans, cursor resume |
| Logical ordering matches raw ordering | Ordering | value encoding, access planning, pagination |
| Cursor resume is strictly monotonic | Ordering | cursor spine, index envelope, executor pagination |
| Bound inclusivity semantics are preserved | Ordering | range planner, index envelope, access validation |
| Access path shape is stable after planning | Structural | query planner, executor route validation, explain |
| Unique constraints are equivalent live/replay | Structural | commit preflight, recovery replay, unique index validation |
| Reverse relation symmetry is preserved | Structural | relation writes, deletes, recovery replay |
| Save/delete mutate row and index state consistently | Mutation | save executor, commit window, recovery |
| Recovery replay is idempotent | Recovery | commit marker replay, index rebuild |
| Recovery rejects incompatible persisted formats | Recovery | row decode, startup rebuild, commit marker retention |
| Accepted row/schema/index contracts own runtime authority | Accepted Authority | planner, executor, writes, explain, cache, cursor, recovery |
| Accepted schema fingerprints own runtime identity | Accepted Authority | commit markers, query cache, planner invalidation |
| Mutation publication is fail-closed | Catalog Mutation | schema transition, reconciliation, snapshot publication |
| Rebuild-required mutation plans do not publish | Catalog Mutation | schema mutation, index add/drop, expression index add |

## Boundary Map

| Boundary | Input Assumptions | Output Guarantees |
| --- | --- | --- |
| serialize -> deserialize | persisted bytes may be malformed or old/future format | bounded decode or fail-closed error |
| RawIndexKey encode -> decode | raw key bytes may be forged | canonical decode, id/namespace/arity validation |
| identity types -> storage key | runtime value may not match key type | typed key decode rejects mismatch |
| planner -> executable plan | logical plan may have unresolved shape pressure | runtime invariants validated before execution |
| executable plan -> executor | executor receives frozen plan | accepted `SchemaInfo` validates access/runtime invariants |
| save executor -> commit | staged rows may contain invalid field values | accepted row/index contracts validate before commit apply |
| commit -> recovery replay | commit marker may represent interrupted mutation | replay is idempotent and reuses accepted preflight |
| cursor decode -> cursor planning | token may be malformed or stale | cursor spine validates signature, shape, offset, boundary, anchor |
| reverse relation mutation | relation fields may change on save/delete | reverse entries update symmetrically with row mutation |
| accepted snapshot -> `SchemaInfo` | snapshot is catalog authority | runtime schema contracts expose accepted fields/indexes |
| generated proposal -> accepted reconciliation | generated metadata is proposal input only | transition policy accepts exact/metadata-safe or rejects |
| mutation plan -> publication status | delta may require rebuild or rewrite | only metadata-safe/no-rebuild plans publish |
| accepted schema fingerprint -> runtime identity | schema may change | commit/cache fingerprints derive from accepted snapshot |

## Enforcement Mapping Table

| Invariant | Assumed At | Enforced At | Exactly Once? | Narrowest Boundary? | Correct Error Class? | Risk |
| --- | --- | --- | --- | --- | --- | --- |
| Memory-id uniqueness | stable memory config | `check-memory-id-invariants.sh` | Yes | CI/static boundary | Yes | Low |
| Field projection slot discipline | terminal/projection execution | `check-field-projection-invariants.sh`, accepted slot guards | Mostly | runtime/static guard boundary | Yes | Low |
| Index-range spec shape | access planning/execution | `check-index-range-spec-invariants.sh`, access validation | Mostly | planner/executor boundary | Yes | Low |
| Recovery idempotence | startup replay | recovery replay tests | Yes | recovery boundary | Yes | Low |
| Schema-before-rebuild | startup recovery | `recovery_reconciles_schema_before_rebuilding_indexes_from_rows` | Yes | recovery startup boundary | Yes | Low |
| Future row format rejection | startup rebuild | `recovery_startup_rebuild_rejects_future_row_format_fail_closed` | Yes | decode/rebuild boundary | Yes | Low |
| Accepted runtime authority | session/executor runtime | `write_boundary_guards` | Distributed but guard-owned | runtime entry boundaries | Yes | Low |
| Mutation publication safety | reconciliation | `MutationPlan::publication_status` and reconcile tests | Yes | schema publication boundary | Yes | Low |
| Accepted fingerprint identity | cache/commit identity | fingerprint guards | Yes | schema fingerprint boundary | Yes | Low |

## Recovery Symmetry Table

| Invariant | Normal Exec | Recovery | Cursor | Reverse Index | Risk |
| --- | --- | --- | --- | --- | --- |
| row/index coupling | accepted commit preflight | replay reuses commit/rebuild preflight | N/A | reverse path guarded | Low |
| unique validation | live apply validates conflicts | replay conflict parity covered | N/A | N/A | Low |
| expression index key construction | accepted expression contracts | accepted rebuild preflight guarded | cursor uses accepted access contract | N/A | Low |
| cursor envelope containment | runtime cursor spine | N/A | anchor identity/envelope validation | N/A | Low |
| reverse relation symmetry | save/delete relation contracts | relation recovery guard covered | N/A | accepted contract guard | Low |
| schema compatibility | session write/read gates | recovery reconciles first | cursor consumes accepted `SchemaInfo` | N/A | Low |
| mutation publication | reconciliation gate | recovery sees accepted snapshot only after gate | future stale cursor case deferred | N/A | Moderate |

## Accepted Authority Preservation Table

| Surface | Runtime Authority | Generated Fallback Possible? | Evidence | Risk |
| --- | --- | --- | --- | --- |
| row decode/emission | accepted row decode contracts | no production fallback found | `write_boundary_guards` | Low |
| field projection | accepted `SchemaInfo` slots | test/model-only generated wrappers only | field projection script and guards | Low |
| access-plan validation | accepted `SchemaInfo` | generated validation removed from runtime | `executor_plan_validation_uses_accepted_schema_info` | Low |
| cursor boundary validation | accepted `SchemaInfo` | fail-closed if absent | `cursor_boundary_validation_uses_authority_schema_info` | Low |
| visible indexes | accepted persisted index contracts | model-only helpers explicitly named | visible-index guards | Low |
| forward index writes | accepted field/expression contracts | no production generated expression fallback | forward-index guards | Low |
| uniqueness | accepted unique key authority | generated split named separately | unique guard | Low |
| recovery rebuild | accepted commit preflight | no generated expression rebuild lane | recovery guard | Low |
| cache/commit fingerprint | accepted snapshot fingerprint | generated model fingerprint APIs absent | fingerprint guard | Low |

## Catalog Mutation Publication Table

| Mutation Shape | Compatibility | Publication Status | Evidence | Risk |
| --- | --- | --- | --- | --- |
| exact accepted snapshot match | metadata-safe/no rebuild | publishable | schema reconcile tests | Low |
| append-only nullable/default-backed fields | metadata-safe/no rebuild | publishable | schema mutation/reconcile tests | Low |
| add field-path index | requires rebuild | blocked | mutation publication tests | Moderate |
| add deterministic expression index | requires rebuild | blocked | mutation publication tests | Moderate |
| drop secondary index | requires rebuild | blocked | mutation publication tests | Moderate |
| alter nullability/type/key | unsupported/incompatible | blocked/fail-closed | mutation and transition tests | Low |
| malformed append-only default payload | field-contract rejection | blocked/fail-closed | transition tests in source inspection | Low |

## High Risk Invariants

None found in current runtime behavior.

The highest remaining pressure is not a current invariant violation: rebuild-required catalog mutations are intentionally blocked before publication. Risk will rise if 0.152 starts publishing add/drop-index plans without rebuild orchestration tests.

## Redundant Enforcement

| Invariant | Redundant Sites | Drift Pressure |
| --- | --- | --- |
| cursor anchor containment | cursor anchor validation plus index scan continuation envelope | intentional defense in depth; low drift |
| access range shape | planner lowering, access runtime validation, index-range script | acceptable because each boundary protects a different representation |
| accepted runtime authority | source guards plus runtime fail-closed accessors | acceptable during migration closure; keep guards current |
| row/index recovery coupling | commit preflight plus recovery replay/rebuild tests | acceptable; recovery must prove parity independently |

## Missing Enforcement

- No current missing enforcement found for audited runtime paths.
- Coverage gap: there is no end-to-end stale cursor rejection test after accepted schema mutation publication because rebuild-required mutation publication is not enabled yet.
- Coverage gap: rebuild-required index mutation plans are blocked, but rebuild orchestration invariants are design-only until the next 0.152 slice implements rebuild planning.

## Drift Sensitivity Summary

| Invariant | Sensitive To | Drift Risk |
| --- | --- | --- |
| cursor monotonicity | new grouped/expression cursor paths | Moderate |
| accepted runtime authority | new model-only convenience APIs | Moderate |
| mutation publication safety | enabling index add/drop publication | Moderate |
| recovery idempotence | new commit marker variants | Moderate |
| index key ordering | new value encodings | Moderate |
| schema fingerprint identity | new persisted contract fields | Moderate |
| field projection discipline | new terminal/projection surfaces | Low |
| memory-id uniqueness | new stable memory declarations | Low |

## Overall Invariant Risk Index

**4/10**

Core runtime invariants are healthy. The score stays moderate because catalog-native schema mutation is intentionally in a staged state: metadata-safe publication is guarded, while rebuild-required mutation publication is blocked until rebuild orchestration exists.

## Verification Readout

- `bash scripts/ci/check-memory-id-invariants.sh` -> PASS
- `bash scripts/ci/check-field-projection-invariants.sh` -> PASS
- `bash scripts/ci/check-index-range-spec-invariants.sh` -> PASS
- `cargo test -p icydb-core recovery_replay_is_idempotent --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core recovery_reconciles_schema_before_rebuilding_indexes_from_rows --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core recovery_startup_rebuild_rejects_future_row_format_fail_closed --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core schema::mutation --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core schema::reconcile --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core --test write_boundary_guards -- --nocapture` -> PASS

## Follow-Up Actions

- owner boundary: schema mutation/rebuild layer
- action: add rebuild orchestration invariant tests before allowing add/drop-index mutation plans to publish accepted schema snapshots
- target report date/run: next `invariant-preservation` run after rebuild planning work lands

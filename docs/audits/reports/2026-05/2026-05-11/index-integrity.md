# Index Integrity Audit - 2026-05-11

## Run Metadata + Comparability Note

- scope: `index-integrity`
- recurring definition: `docs/audits/recurring/access/access-index-integrity.md`
- compared baseline report path: `docs/audits/reports/2026-03/2026-03-12/index-integrity.md`
- code snapshot identifier: `082b7d142`
- method tag/version: `Method V4`
- comparability status: `non-comparable` - Method V4 adds accepted runtime index authority, catalog mutation readiness, and live verification commands after the schema/index authority migration.

## Method Changes

- Added accepted-runtime index authority checks for planner, explain, cache, writes, uniqueness, recovery, and reverse indexes.
- Added catalog mutation readiness checks for index add/drop/rebuild classification.
- Replaced stale historical verification filter `access_plan_rejects_misaligned_index_range_spec`, which matched zero tests, with live invalid-range and route-guard checks.

## Index Invariant Registry

| Invariant | Category | Enforced Where |
| --- | --- | --- |
| Raw index keys sort lexicographically in traversal order | Ordering | `IndexStore::visit_raw_entries_in_range`, `RawIndexKey` ordering |
| Canonical value encoding preserves component order | Ordering | index key codec/property tests |
| Index id and namespace are encoded into index keys | Namespace | `IndexKey`, `IndexId`, cursor anchor validation |
| Decode rejects malformed or partial keys | Structural | index key codec tests and cursor anchor canonical re-encode check |
| Forward index mutations are coupled to row writes | Mutation | `db/index/plan`, commit preflight, write-boundary guards |
| Reverse relation indexes use accepted row contracts | Mutation | `db/relation/reverse_index.rs`, `reverse_relation_runtime_paths_use_accepted_contracts` |
| Unique validation is equivalent for live apply and replay | Recovery | commit unique parity tests |
| Runtime visible indexes are accepted-schema-backed | Accepted Authority | `SchemaInfo`, `VisibleIndexes`, accepted planner index contracts |
| Runtime index key construction uses accepted contracts | Accepted Authority | `IndexKey::new_from_slots_with_accepted_*`, write-boundary guards |
| Index mutations are explicitly rebuild-gated | Catalog Mutation | `SchemaMutation`, `MutationPlan`, schema mutation tests |

## Encode / Decode Symmetry Table

| Key Type | Symmetric? | Failure Mode | Risk |
| --- | --- | --- | --- |
| Raw index key | Yes | malformed bytes reject during decode | Low |
| Composite accepted field-path key | Yes | non-indexable component yields no key | Low |
| Accepted expression key | Yes for deterministic subset | unsupported/mismatched expression source rejects | Low |
| Cursor raw anchor key | Yes | non-canonical re-encode mismatch rejects | Low |

## Ordering Stability Analysis

| Case | Lexicographically Stable? | Why | Risk |
| --- | --- | --- | --- |
| Negative vs positive numeric values | Yes | canonical encoded value ordering covered by property test | Low |
| Equal boundary with exclusive edge | Yes | `envelope_is_empty` collapses empty equal-exclusive envelopes | Low |
| Composite key prefix ordering | Yes | key construction uses accepted field/key item order | Low |
| DESC continuation | Yes | strict direction-owned bound rewrite and DESC empty-envelope regression | Low |
| Future value encodings | Unknown until added | must extend ordering property coverage | Moderate |

## Namespace Isolation Table

| Scenario | Can Cross-Decode? | Prevented Where | Risk |
| --- | --- | --- | --- |
| Key from index A used as index B anchor | No | cursor anchor index-id validation | Low |
| User key decoded as internal namespace | No | key namespace validation | Low |
| Wrong component arity | No | component count checks | Low |
| Prefix confusion across indexes | No | `IndexId` includes entity tag and ordinal | Low |

## Entry Layout Analysis

| Entry Component | Layout Stable? | Decode Safe? | Risk |
| --- | --- | --- | --- |
| Raw key bytes | Yes | bounded decode and canonical checks | Low |
| Entry payload | Yes | store-owned raw entry structure | Low |
| Index id | Yes | encoded into key | Low |
| Primary key payload | Yes | equivalence checks fail closed on decode error | Low |

## Reverse Relation Integrity

| Flow | Reverse Mutation Symmetric? | Orphan Risk | Replay Risk |
| --- | --- | --- | --- |
| Save relation row | Yes | Low | Low |
| Delete relation row | Yes | Low | Low |
| Replace relation row | Yes | Low | Low |
| Recovery replay | Yes, guarded by accepted preflight | Low | Low |

Evidence:

- `reverse_relation_runtime_paths_use_accepted_contracts` passed in `write_boundary_guards`.

## Unique Enforcement Equivalence

| Scenario | Unique Enforced? | Recovery Enforced? | Live/Replay Class Parity? | Risk |
| --- | --- | --- | --- | --- |
| Duplicate unique insert | Yes | Yes | Yes | Low |
| Interrupted conflicting unique batch | Yes | Yes | Yes | Low |
| Same-value replace | Yes | Expected stable from unique authority split | Low |
| Accepted expression unique key | Yes for accepted deterministic subset | Yes through accepted preflight | Low |

Evidence:

- `unique_conflict_classification_parity_holds_between_live_apply_and_replay` passed.
- `recovery_replay_interrupted_conflicting_unique_batch_fails_closed` passed.
- `unique_index_validation_splits_accepted_and_generated_authority` passed in `write_boundary_guards`.

## Partial-Update Membership Transitions

| Transition Case | Prepared Ops Minimal? | Symmetric? | Risk |
| --- | --- | --- | --- |
| old indexed value -> new indexed value | Yes | Yes | Low |
| old null -> new value | Yes | Yes | Low |
| old value -> null | Yes | Yes | Low |
| membership moves across indexes | Yes, through accepted index iteration | Yes | Low |
| unchanged indexed field | Expected no-op | Yes | Low |

Evidence:

- Forward index write guards verify accepted field-path and expression key construction.
- Preflight reader guards verify reduced accepted index facts, not generated `IndexModel`.

## Mixed Unique+Reverse+Secondary Ordering Check

| Scenario | Uniqueness Verdict Happens First? | Any Side Effects Before Verdict? | Risk |
| --- | --- | --- | --- |
| conflicting unique plus reverse and secondary changes | Yes, preflight authority validates before commit apply | No evidence of side-effectful write before verdict | Low |

## Accepted Runtime Index Authority

| Runtime Lane | Authority Source | Generated Metadata Present? | Allowed? | Risk |
| --- | --- | --- | --- | --- |
| Visible index selection | `VisibleIndexes::accepted_schema_visible(schema_info)` | No on accepted runtime lane | Yes | Low |
| Planner candidate construction | accepted planner indexes from `SchemaInfo` | model-only helpers remain for standalone generated lane | Yes, named model-only | Low |
| Predicate/index membership | accepted field-path/expression contracts | No runtime fallback found | Yes | Low |
| Key shape and encoding | `SchemaIndexInfo` / `SchemaExpressionIndexInfo` | No accepted-runtime `IndexModel` key builder | Yes | Low |
| Forward writes | accepted index contracts | No generated expression write fallback | Yes | Low |
| Reverse writes | accepted row relation contracts | No generated relation fallback | Yes | Low |
| Uniqueness validation | `UniqueKeyAuthority::Accepted*` | generated split remains named separately | Yes | Low |
| Recovery/rebuild | accepted commit preflight | No production generated expression fallback | Yes | Low |
| Explain/cache identity | accepted `VisibleIndexes` and fingerprints | model-only surfaces remain | Yes | Low |

Required fail-closed checks:

| Scenario | Expected Behavior | Evidence | Risk |
| --- | --- | --- | --- |
| Accepted index metadata missing after reconciliation | incompatible accepted-schema error | guards require accepted persisted index contracts before runtime exposure | Low |
| Stale accepted index fingerprint | planner/cache identity derives from accepted contracts | schema fingerprint guards passed | Low |
| Dropped secondary index still present in store | not yet runtime-visible without accepted contract; cleanup belongs to rebuild plan | deferred orchestration | Moderate |
| Runtime write sees generated-only index metadata | unreachable outside model-only/test lanes | `write_boundary_guards` passed | Low |

## Catalog Mutation Readiness

| Mutation | Compatibility | Rebuild Requirement | Runtime Visibility Boundary | Risk |
| --- | --- | --- | --- | --- |
| Add nullable/defaulted field | `MetadataOnlySafe` | `NoRebuildRequired` | publish allowed after mutation-plan guard | Low |
| Add non-unique field-path index | `RequiresRebuild` | `IndexRebuildRequired` | publication blocked until rebuild orchestration exists | Moderate |
| Add deterministic expression index | `RequiresRebuild` | `IndexRebuildRequired` | publication blocked until rebuild orchestration exists | Moderate |
| Drop non-required secondary index | `RequiresRebuild` | `IndexRebuildRequired` | publication blocked until rebuild orchestration exists | Moderate |
| Alter nullability | `UnsupportedPreOne` | `Unsupported` | fail closed | Low |
| Change field/type/key shape | `Incompatible` | `FullDataRewriteRequired` | fail closed | Low |

Evidence:

- `schema::mutation` tests passed.
- `schema::reconcile` tests passed.
- Reconcile publication guard rejects non-`MetadataOnlySafe` or rebuild-required plans before accepted snapshot publication.

## Row / Index Coupling Analysis

| Failure Point | Divergence Possible? | Prevented? | Risk |
| --- | --- | --- | --- |
| Failure after index insert before row write | Low | commit/recovery replay and preflight contracts | Low |
| Failure after row write before index insert | Low | commit/recovery replay | Low |
| Failure during reverse index update | Low | reverse path accepted-contract guard | Low |
| Replay after partial commit | Low | unique/recovery tests | Low |

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
| Index add/drop with rebuild | High | blocked before publication today | Moderate |
| Expression index rebuild | High | blocked before publication today | Moderate |
| Replace with unique + secondary + reverse changes | Medium | guarded by preflight/replay parity | Low |
| Recovery rebuild using accepted contracts | Medium | guard-covered | Low |
| Planner/cache invalidation after fingerprint change | Medium | accepted fingerprint-owned; broader rebuild orchestration deferred | Moderate |

## Storage-Layer Assumptions

| Assumption | Required For | Violation Impact |
| --- | --- | --- |
| Key comparison is byte-wise deterministic | index traversal order | order drift / cursor errors |
| Store iteration remains deterministic | replay and pagination | non-deterministic pages |
| Commit replay sees durable marker state | row/index atomicity | divergence after interruption |
| No external mutation of raw index store | namespace and coupling | orphan or stale index entries |
| No concurrent writes | uniqueness and reverse symmetry | conflict races |

## Overall Index Risk Index

**4/10**

Runtime index integrity is low risk. The score is above the previous `3/10`
because Method V4 includes catalog mutation readiness, and index add/drop/rebuild
orchestration is intentionally not implemented yet. Current behavior is fail-closed
for rebuild-required index mutations.

## Verification Readout

- `cargo test -p icydb-core access_plan_rejects_misaligned_index_range_spec --features sql -- --nocapture` -> BLOCKED: stale historical filter matched zero tests; Method V4 replaced it with current invalid-range and route-guard checks.
- `cargo test -p icydb-core load_cursor_pagination_pk_order_inverted_key_range_returns_empty_without_scan --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core index_range_aggregate_fast_path_specs_reject_non_exact_range_arity --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core index_range_aggregate_fast_path_specs_reject_prefix_spec_presence --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core cross_layer_canonical_ordering_is_consistent --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core unique_conflict_classification_parity_holds_between_live_apply_and_replay --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core recovery_replay_interrupted_conflicting_unique_batch_fails_closed --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core load_cursor_live_state_delete_between_pages_can_shrink_remaining_results --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core --test write_boundary_guards -- --nocapture` -> PASS
- `cargo test -p icydb-core schema::mutation --features sql -- --nocapture` -> PASS
- `cargo test -p icydb-core schema::reconcile --features sql -- --nocapture` -> PASS

## Follow-Up Actions

- owner boundary: schema mutation/rebuild layer
- action: add explicit rebuild-orchestration design/tests before allowing accepted snapshot publication for add/drop index mutation plans
- target report date/run: next `index-integrity` run after 0.152 rebuild planning work

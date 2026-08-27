# Invariant Preservation Audit

## Run Metadata + Comparability Note

- scope: `invariant-preservation`
- recurring definition:
  `docs/audits/recurring/integrity/invariant-preservation.md`
- compared baseline report path:
  `docs/reports/recurring/2026/05/11/invariant-preservation/01/report.md`
- code snapshot identifier: `630cc9c974294631ff77a77d69a2704581ccb2dd`
  (`v0.245.1`), tree `4b1c4464679e836bfb22da61b72080db1133b7bd`
- worktree relevance: source inspection included two unrelated concurrent
  test-feature gating edits in `compile_cache.rs` and `parser/mod.rs`; neither
  changes production runtime behavior. The 0.246 no-build documentation is
  outside this correctness scope. This audit adds documentation only.
- method tag/version: `Method V5`
- comparability status: `non-comparable (method change)` - V5 retains the V4
  identity, ordering, recovery and accepted-authority anchors, but expands the
  mutation-publication proof to current SQL DDL expression-index and physical
  migration routes. Three V4 test selectors now execute zero tests and are
  replaced explicitly below. Numerical risk deltas are therefore `N/A`.

## Method Changes

Method V5 makes two evidence corrections without changing runtime policy:

1. The mutation registry covers both field-path and expression-index domain
   replacement plus feature-gated physical schema migration. The recurring
   definition still describes only the earlier single field-path startup
   runner.
2. The zero-test selectors `recovery_replay_is_idempotent`,
   `recovery_reconciles_schema_before_rebuilding_indexes_from_rows`,
   `recovery_startup_rebuild_rejects_future_row_format_fail_closed`,
   `schema::reconcile`, and `structural_guards` are not treated as successful
   proofs. Current named replacements are recorded in the verification table.

Stable anchors retained from V4 are accepted-root authority, raw/logical index
ordering equivalence, cursor-envelope monotonicity, row/index commit symmetry,
schema replay idempotence, and fail-closed persisted decoding.

## Verdict

`PASS WITH FINDING`

No runtime invariant violation was found. Accepted schema remains the singular
runtime authority; access and cursor shapes are validated against accepted
contracts; row, forward-index and reverse-index effects share one prepared
commit; recovery reconstructs and verifies those effects through the same
accepted contracts; and schema/index publication is marker-backed and
fail-closed.

`INV-001` is a proof-maintenance issue, not a runtime defect. The recurring
definition's stale surface description and zero-test selectors could let a
future audit report false confidence unless the auditor notices and replaces
them as this run did.

## Invariant Registry

| Invariant | Category | Subsystem(s) impacted |
| --- | --- | --- |
| Persisted row primary-key fields equal the decoded data-store key | Identity | structural row decode, reads, commit preflight, recovery |
| Index key kind, index id and generation remain in one physical namespace | Identity | index codec, scan, reverse indexes, recovery verification |
| Encoded index component arity matches the accepted index contract | Identity | key codec, access lowering, scan, cursor |
| Entity path, tag and store path agree with accepted catalog identity | Identity | runtime root, commit preparation, recovery |
| Raw index-key order equals canonical logical component order | Ordering | index codec/store, query ordering, pagination |
| Continuation candidates advance strictly in the selected direction | Ordering | index envelope, cursor runtime, scan |
| Inclusive and exclusive bounds survive lowering unchanged | Ordering | access lowering, index envelope, range scan |
| A continuation anchor cannot widen or escape its original envelope | Ordering | cursor decode, index envelope, executor continuation |
| Access paths are immutable after accepted-schema validation | Structural | planner-to-executor boundary, explain, cache |
| Runtime execution consumes lowered raw specs rather than rebuilding semantic bounds | Structural | access lowering, executor streams, index owner |
| Unique membership is equivalent in live commit, replay and rebuild | Structural | commit preflight, index mutation, recovery |
| Reverse relation membership is symmetric for insert, update and delete | Structural | relation projection, commit, recovery |
| Row and all forward/reverse index effects publish as one commit | Mutation | prepared commit, marker/journal, data/index stores |
| Batch relation and uniqueness checks observe one final-row overlay | Mutation | mutation scheduler, relation validation, uniqueness |
| Schema metadata and any staged physical index domain publish atomically | Mutation | SQL DDL, schema publication, journal recovery |
| Marker and journal replay are idempotent | Recovery | commit recovery, journal/schema stores |
| Recovery verifies final row and index effects before clearing the marker | Recovery | recovery effect verification, commit guard |
| Malformed, predecessor or future persisted forms fail closed | Recovery | row/schema/index/marker codecs, startup |
| Startup recovery completes before generated proposal reconciliation claims readiness | Recovery | startup driver, schema application |
| Accepted snapshots own row decode, planning, execution, writes and recovery | Accepted authority | runtime root, `SchemaInfo`, `EntityAuthority`, inspection plan |
| Generated models remain proposal, reconciliation, model-only or test inputs | Accepted authority | startup/schema application, facade, runtime |
| Runtime and commit fingerprints derive from accepted persisted snapshots | Accepted authority | cache, cursor, commit marker, integrity jobs |
| One request retains one immutable accepted runtime root | Accepted authority | session request, relation lookup, SQL execution |
| Schema transition policy alone classifies accepted versus rejected deltas | Catalog mutation | transition, mutation plan, SQL DDL |
| Metadata-only plans may publish immediately; index changes require physical-domain proof | Catalog mutation | mutation preflight, SQL DDL, publication |
| Physical migration validates, rewrites, final-validates and publishes one complete candidate | Catalog mutation | migration application, recovery, publication |
| Unsupported or mismatched mutation targets fail before accepted visibility | Catalog mutation | transition, DDL admission, publication race checks |

## Boundary Map

| Boundary | Input assumptions | Output guarantees |
| --- | --- | --- |
| persisted bytes -> row/schema/index/marker values | bytes may be malformed, oversized or noncurrent | bounded current-form decode or typed fail-closed error |
| accepted root selection -> runtime root | store roots may be absent or change during construction | verified selections compiled once; roots re-observed before publication |
| accepted entity runtime -> `SchemaInfo` / `EntityAuthority` | selection is pinned to one root identity | derivatives share accepted fingerprint, row contract, entity and store identity |
| planner -> access lowering | semantic path is accepted-schema validated | executor receives raw range/prefix specs and cannot reclassify eligibility |
| cursor token -> resumed scan | token and anchor may be stale or forged | root/plan identity, containment and strict directional advancement are checked |
| raw index entry -> row materialization | index key may be malformed or orphaned | key decodes under index authority; missing row is typed store corruption |
| row operation -> prepared commit | before/after rows and marker identity may be corrupt | expected key, accepted fingerprint, constraints and all index effects are preflighted |
| prepared commit -> durable publication | process may stop at any marker phase | marker-backed atomic row/index state with replayable deterministic effects |
| commit marker -> recovery replay/fold | marker may describe partially applied work | accepted-authority preparation, idempotent application and effect verification |
| generated proposal -> transition plan | generated material has no runtime authority | schema-owned accepted/rejected decision and physical-work preflight |
| staged index/migration domain -> accepted publication | candidate may race, drift or be incomplete | expected-head match and one marker-backed domain + schema publication |
| publication -> later request | old requests may still hold an immutable root | new root observation rejects stale cache identity; old root cannot become current |

## Enforcement Mapping Table

| Invariant | Assumed at | Enforced at | Exactly once? | Narrowest boundary? | Error class | Risk |
| --- | --- | --- | --- | --- | --- | --- |
| row key equals decoded PK | row readers, commit/recovery | `persisted_row/reader/primary_key.rs` via accepted row contract | Yes per decode | Yes | store corruption / invariant | Low |
| index namespace/id/arity | scans and cursor | `index/key/codec/mod.rs`, accepted access validation | Once per representation | Yes | bounded decode / query invariant | Low |
| raw/logical order equivalence | all ordered scans | index-owned ordered component encoding and raw-key comparator | Yes, plus property tests | Yes | fail before traversal | Low |
| continuation containment and monotonicity | paged execution | `index/envelope/mod.rs`; cursor validates root and plan identity | Intentional two-boundary defense | Yes | typed continuation invariant | Low |
| accepted access shape | executor route | `access/validate.rs` before lowering; static byte-only executor guards | Once semantically | Yes | query executor invariant | Low |
| row/index commit coupling | durable mutation | `commit/prepare.rs` produces one `PreparedRowCommitOp` | Yes | Yes | store/invariant error | Low |
| relation target and reverse symmetry | mutation commit | `relation/reverse_index.rs::prepare_source_transition` against final overlay | Yes, replay reuses preparation | Yes | relation violation / corruption | Low |
| uniqueness parity | commit and replay | accepted constraint schedule and prepared index mutation | Once per preparation mode | Yes | conflict / invariant | Low |
| accepted runtime authority | every runtime entry | `session/accepted_schema.rs` root selection, compilation and root re-observation | Singular owner, multiple consumer checks | Yes | store corruption / stale accepted revision | Low |
| accepted fingerprint identity | cache, commit, recovery | accepted snapshot fingerprint and selection identity checks | Intentional trust-boundary repeats | Yes | stale/invariant/recovery failure | Low |
| schema mutation classification | reconciliation and DDL | `schema/transition.rs` -> `MutationPlan::publication_preflight` | Yes | Yes | typed rejection / unsupported | Low |
| schema + physical domain atomicity | DDL/migration publication | `commit/schema_publication.rs` marker and staged-domain application/replay | Yes | Yes | invariant / recovery failure | Low |
| recovery idempotence/effect equality | startup | journal/schema compare-and-replace, replay preparation and effect verification | Independent recovery proof | Yes | recovery effect verification | Low |
| audit verification selectors name live tests | recurring audit execution | definition's required command list | No: five selectors match zero tests | No | process-level false green | Medium |

## Recovery Symmetry Table

| Invariant | Normal execution | Recovery | Cursor | Reverse index | Risk |
| --- | --- | --- | --- | --- | --- |
| row/index coupling | one prepared commit owns row and index ops | replay rebuilds prepared ops and verifies final effects | N/A | reverse ops are included in the same prepared commit | Low |
| key/row identity | structural decode validates expected data key | replay and rebuild use the same accepted row contract and expected key | decoded index PK locates the row; missing row fails closed | source and target identities are encoded into reverse keys | Low |
| unique membership | final overlay + accepted index contracts | replay preparation and effect validation | N/A | N/A | Low |
| range/envelope safety | lowered raw specs and index-owned envelope | replay does not plan queries | root/plan binding, containment and strict advancement | N/A | Low |
| reverse relations | old/new accepted projections merge exact membership deltas | mixed-entity interruption tests recover retained reverse protection | N/A | insert/update/delete share one merge owner | Low |
| accepted authority | request captures one runtime root | recovery selects accepted runtime entities and fingerprints | cursor binds accepted root identity | relation lookup remains within the captured root | Low |
| schema publication | preflight + marker + candidate/domain apply | accepted schema records and index-domain records replay/fold together | old cursor cannot authorize a new root | physical reverse/index state is verified before marker clear | Low |
| persisted format | current encoders only | malformed/noncurrent bytes fail closed before application | malformed tokens fail closed | malformed keys fail before interpretation | Low |

## Accepted Authority Preservation Table

| Surface | Runtime authority | Generated fallback possible? | Evidence | Risk |
| --- | --- | --- | --- | --- |
| runtime root | accepted store roots and verified catalog selections | No | `AcceptedSchemaRuntimeRoot::compile` and post-build root re-observation | Low |
| row decode/emission | accepted inspection-plan row contract | No | structural reader and primary-key validation | Low |
| field/index metadata | root-owned `SchemaInfo` | No | access validation and executor authority use captured context | Low |
| planning/execution | accepted `SchemaInfo`, lowered access specs, `EntityAuthority` | No | layer and range-spec invariant gates | Low |
| relation target lookup | same captured database-wide runtime root | No | `AcceptedSchemaCatalogContext::for_entity_*` | Low |
| writes/constraints | accepted commit authority and constraint schedule | No | `prepare_commit_context` / `prepare_row_commit_with_context` | Low |
| cache/cursor identity | accepted root and schema fingerprints | No | root matching, fingerprint checks, cursor proof | Low |
| recovery/rebuild | accepted runtime entity, row contract and commit fingerprint | No | recovery prepare/rebuild and effect verification | Low |
| schema proposal | generated model may propose and reconcile only | Not a runtime fallback | transition/application boundaries | Low |

## Catalog Mutation Publication Table

| Mutation shape | Admission / physical requirement | Publication boundary | Current proof | Risk |
| --- | --- | --- | --- | --- |
| exact or accepted-generated compatible metadata | metadata-only | accepted candidate marker publication | transition and mutation tests | Low |
| supported additive/default-backed field metadata | metadata-only after historical-fill proof | accepted candidate marker publication | mutation planning tests | Low |
| field-path index addition | complete staged user-index domain | schema + domain under one marker | mutation domain tests and publication source inspection | Low |
| deterministic expression-index addition | complete staged user-index domain | same schema + domain owner as field-path indexes | expression admission/domain tests and source inspection | Low |
| index drop / ordinal compaction | complete accepted-before/after domain replacement | schema + replacement under one marker | mutation domain/planning tests | Low |
| constraint activation | validation job or candidate-domain proof as required | schema + validation state through commit publication | source inspection; not re-executed as a broad family here | Low |
| feature-gated physical schema migration | bounded validate, rewrite, final validation and cleanup | application record + complete candidate publication | recovery-and-publication focused test | Low |
| unsupported shape, stale head or target mismatch | rejected before visibility | none | fail-closed planning and publication-race checks | Low |

The V4 statement that rebuild-required plans simply remain blocked is no longer
the whole current contract. Index DDL can publish after a complete staged
physical-domain proof, and the migration feature can publish after its bounded
validation/rewrite/final-validation protocol. Neither route gives SQL or
generated models independent mutation authority.

## High Risk Invariants

None found.

No row, index, relation, recovery or accepted-authority invariant was observed
missing or late enough to create a `HIGH` runtime risk.

## Redundant Enforcement

| Invariant | Sites | Disposition |
| --- | --- | --- |
| cursor identity and containment | cursor/root proof plus index-envelope validation | Retain: separate untrusted-token and physical-scan boundaries |
| access/index shape | accepted planner validation plus lowered-spec executor alignment checks | Retain: semantic and byte-level representations differ |
| accepted fingerprint | runtime-root/cache, commit marker and recovery effect checks | Retain: independent publication and recovery trust boundaries |
| row/index equality after recovery | prepared replay plus final effect verification | Retain: preparation proves intent; verification proves materialized state |
| relation symmetry | final-overlay target validation plus reverse-membership delta projection | Retain: referential existence and physical reverse state are distinct invariants |

No duplicated enforcement was found that should be consolidated by this
correctness-only audit.

## Missing Enforcement

No current runtime enforcement gap was found in the inspected paths.

One audit enforcement gap is active:

| ID | Risk | Owner boundary | Present friction | Disposition | Action trigger |
| --- | --- | --- | --- | --- | --- |
| INV-001 | `MEDIUM` | recurring invariant audit definition | five required selectors exit successfully while executing zero tests, and the mutation registry omits current expression-index and physical-migration publication | revise the definition; this run uses explicit live replacements and remains valid | before the next `invariant-preservation` run |

## Drift Sensitivity Summary

| Invariant | Sensitive to | Drift risk |
| --- | --- | --- |
| raw/logical ordering | new scalar encodings, DESC semantics, composite key shapes | Moderate |
| cursor containment | new grouped/aggregate cursor payloads or new physical seek primitives | Moderate |
| accepted-root singularity | cache reuse across publications or generated fallback convenience | Moderate |
| commit/recovery parity | new journal record or staged physical-domain variants | Moderate |
| reverse relation symmetry | composite relations, cross-store mutation changes | Moderate |
| mutation publication | another DDL shape, migration phase or publication owner | Moderate |
| persisted fail-closed decode | new current-form codec fields or relaxed bounds | Moderate |
| verification validity | renamed/moved tests without zero-test rejection | High until INV-001 is corrected |
| memory-id uniqueness | new stable allocations | Low; static gate is current |

## Overall Invariant Risk Index

**4/10 — moderate proof-maintenance pressure; low observed runtime risk.**

The runtime score would otherwise be low. It remains 4 because schema
publication now spans more supported physical routes than Method V4 covered,
while the recurring definition's executable proof list has drifted. This is
not numerically comparable with V4's 4/10.

## Verification Readout

| Verification | Status | Result |
| --- | --- | --- |
| memory-id invariant gate | `PASS` | current stable allocation contracts verified |
| layer-authority invariant gate | `PASS` | accepted authority, envelope ownership and layer edges verified |
| index-range-spec invariant gate | `PASS` | planner-owned lowering and byte-only executor contracts verified |
| V4 replay-idempotence selector | `FAIL` | command exited zero but executed 0 tests; not accepted as evidence |
| live row/reverse recovery replacement | `PASS` | 5 marker/journal/row/state interruption points recovered exact mixed-entity and reverse-relation state |
| exact journal control replay/reopen replacement | `PASS` | 1 exact control replay/retire/reopen test |
| exact schema replay/fold replacement | `PASS` | 1 journaled schema candidate idempotence test |
| V4 schema-before-rebuild selector | `FAIL` | command exited zero but executed 0 tests; not accepted as evidence |
| live recovery-before-reconciliation replacement | `PASS` | 1 startup state-machine test retains recovery until exact schema receipt |
| V4 future-row-format selector | `FAIL` | command exited zero but executed 0 tests; not accepted as evidence |
| live persisted-row fail-closed replacement | `PASS` | 1 malformed corpus test covers bounded row-envelope decoding |
| schema mutation family | `PASS` | 34 mutation planning, domain, identity, uniqueness and budget tests |
| V4 `schema::reconcile` selector | `FAIL` | command exited zero but executed 0 tests; not accepted as evidence |
| physical migration recovery/publication replacement | `PASS` | 1 migration-feature test validates recovery and complete candidate publication |
| V4 `structural_guards` selector | `FAIL` | command exited zero but executed 0 tests; not accepted as evidence |
| accepted-index orphan detection replacement | `PASS` | 1 diagnostics-enabled test returns typed store corruption for an accessed missing row |
| index envelope / continuation family | `PASS` | 14 containment, monotonicity, inclusivity and cross-layer ordering tests |
| index semantic/raw ordering family | `PASS` | 4 stable, Cartesian and randomized composite ordering tests |
| final-overlay relation validation | `PASS` | 1 mixed insert/update/delete relation-symmetry test |
| production changes by this audit | `PASS` | none |
| full repository suite | `BLOCKED` | prohibited and user-owned under `AGENTS.md` |

The repeated dead-code warnings emitted by SQL-only focused builds belong to
feature-shape hygiene, not this correctness audit; the migration-enabled proof
compiled and passed. No test failure was observed after replacing zero-test
selectors.

## Follow-Up Actions

- Revise `docs/audits/recurring/integrity/invariant-preservation.md` before its
  next execution: replace the five stale selectors with live named proofs,
  require a nonzero executed-test count, and describe the current expression
  index, staged domain and physical migration publication contracts.
- Do not change runtime code from this audit. INV-001 is audit-governance work
  and does not authorize a 0.246 implementation patch.

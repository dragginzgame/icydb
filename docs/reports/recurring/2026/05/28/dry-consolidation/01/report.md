# DRY Consolidation Audit - 2026-05-28

## Report Preamble

| Field | Value |
| --- | --- |
| Scope | Crosscutting DRY and redundancy scan over `crates/icydb-core/src`; tests, benches, examples, and generated artifacts excluded except where noted as supporting evidence. |
| Audit definition | `docs/audits/recurring/crosscutting/crosscutting-dry-consolidation.md` |
| Compared baseline | `docs/audits/reports/2026-05/2026-05-14/dry-consolidation.md` |
| Code snapshot | `b144c6bca` with a dirty worktree; dirty changes were mostly release/tooling/docs and were not treated as runtime evidence unless in the audited root. |
| Method tag/version | `DRY-1.2` |
| Comparability status | comparable; scope, taxonomy, and scoring model match the 2026-05-14 baseline. |

Method manifest:

```text
method_version = DRY-1.2
duplication_taxonomy = DT-1
owner_layer_taxonomy = OL-1
invariant_role_model = IR-1
facade_inclusion_rule = FI-1
consolidation_safety_model = CS-1
```

## Executive Summary

The current tree remains structurally healthy for DRY purposes: no high-risk divergence-prone duplication pattern was found. The notable change since the 2026-05-14 baseline is that recent schema, SQL-surface, and relation work moved more invariant-carrying code into two dense owners:

- `crates/icydb-core/src/db/schema/reconcile.rs`
- `crates/icydb-core/src/db/relation/reverse_index.rs`

That is not a correctness failure. It does mean the best next consolidation work is no longer broad cleanup; it is narrow, owner-local extraction of shared admission/publication descriptors where the same authority checks are repeated.

Verdict: **PASS with moderate cleanup pressure**.

DRY risk index: **5 / 10**.

## Baseline Delta

| Metric | 2026-05-14 baseline | 2026-05-28 current | Delta |
| --- | ---: | ---: | --- |
| In-scope Rust files | not recorded | 983 | N/A |
| Total duplication patterns | 11 | 12 | +1 |
| High-risk divergence-prone patterns | 0 | 0 | unchanged |
| Same-layer accidental patterns | 0 | 1 | +1 |
| Cross-layer intentional patterns | 3 | 4 | +1 |
| Defensive duplication patterns | 1 | 1 | unchanged |
| Boundary-protected patterns | 8 | 9 | +1 |
| Invariants repeated in more than 3 places | 7 | 8 | +1 |
| Error-construction families repeated more than 3 times | 2 | 3 | +1 |
| Safe LoC reduction estimate | 10-20 | 35-75 | increased |

The increase is concentrated in two places:

1. SQL DDL schema-publication gates repeat the same transition/admission/store shape across several field and index operations.
2. Relation-edge target identity lowering is repeated across save validation and reverse-index mutation preparation.

## Pattern Inventory

| ID | Pattern | Primary locations | Taxonomy | Risk | Classification |
| --- | --- | --- | --- | --- | --- |
| D1 | SQL DDL accepted-schema publication gate | `crates/icydb-core/src/db/schema/reconcile.rs:60`, `:111`, `:153`, `:205`, `:314`, `:394`, `:552`, `:651` | same-owner evolution drift | medium | MIGRATE CALLS THEN EXTRACT LOCALLY |
| D2 | Accepted relation-edge target contract lowering | `crates/icydb-core/src/db/relation/save_validate.rs:225`, `crates/icydb-core/src/db/relation/reverse_index.rs:490` | same-owner duplicated invariant | medium | MIGRATE CALLS THEN EXTRACT LOCALLY |
| D3 | Relation local component kind normalization | `crates/icydb-core/src/db/relation/save_validate.rs:546`, `crates/icydb-core/src/db/relation/reverse_index.rs:627` | same-owner helper duplication | medium-low | DELETE DUPLICATE HELPER |
| D4 | Relation full target identity read/build paths | `crates/icydb-core/src/db/relation/save_validate.rs`, `crates/icydb-core/src/db/relation/reverse_index.rs`, `crates/icydb-core/src/db/relation/validate.rs` | boundary-protected | medium-low | KEEP BOUNDARY, SHARE DESCRIPTOR ONLY |
| D5 | Route capability facts forwarded through load/aggregate hint consumers | `crates/icydb-core/src/db/executor/planning/route/capability_facts.rs`, `crates/icydb-core/src/db/executor/planning/route/hints/load.rs`, `crates/icydb-core/src/db/executor/planning/route/hints/aggregate.rs` | boundary-protected | low-medium | KEEP |
| D6 | Cursor contract transport across boundary, continuation, and runtime modules | `crates/icydb-core/src/db/cursor/boundary.rs`, `continuation.rs`, `runtime.rs`, `spine.rs` | boundary-protected | low-medium | KEEP |
| D7 | SQL statement/query result shell mapping | `crates/icydb-core/src/db/session/sql/execute/mod.rs`, `crates/icydb-core/src/db/session/sql/execute/write_returning.rs` | facade shell repetition | low-medium | OPTIONAL LOCAL HELPER |
| D8 | Accepted generated-compatible row-decode proof propagation | `crates/icydb-core/src/db/executor/authority/entity.rs`, accepted-schema consumers | defensive boundary | low-medium | KEEP |
| D9 | Commit marker envelope and replay guard duplication | `crates/icydb-core/src/db/commit/*` | defensive boundary | low-medium | KEEP |
| D10 | Startup field-path and expression schema mutation adapters | `crates/icydb-core/src/db/schema/reconcile/startup_field_path.rs`, `startup_expression.rs` | phase-separated boundary | medium-low | KEEP |
| D11 | Index reader/preflight overlay | `crates/icydb-core/src/db/index/*`, access/preflight users | boundary-protected | low-medium | KEEP |
| D12 | Schema DDL error construction via repeated `store_unsupported(format!(...))` | `crates/icydb-core/src/db/schema/reconcile.rs` | error-family drift | medium-low | CONSOLIDATE WHEN TOUCHING D1 |

## Detailed Findings

### D1 - SQL DDL Accepted-Schema Publication Gate

Several SQL DDL execution functions follow the same ownership sequence:

1. read accepted `before`;
2. build or receive proposed `after`;
3. run `decide_schema_transition(before, after)`;
4. enforce a narrow expected `SchemaTransitionPlanKind`;
5. reject unsupported physical/runtime drift with `store_unsupported`;
6. write `insert_persisted_snapshot`.

Observed sites include field-path index addition, expression-index addition, field addition, field drop, default change, nullability change, rename, and secondary-index drop.

This repetition is now large enough to make a local helper worth considering. The helper should not merge SQL DDL semantics into the catalog mutation runner. It should only own the common transition/admission/publication envelope for metadata-only or narrowly-typed SQL DDL publication.

Recommended action: extract an owner-local publication gate in `db::schema::reconcile` when the next DDL slice touches this file.

### D2 / D3 - Accepted Relation-Edge Contract Lowering

The save-validation path and reverse-index path both lower persisted accepted relation-edge metadata into runtime target identity checks. The duplication includes:

- resolving the accepted target schema snapshot;
- collecting ordered target primary-key component kinds;
- comparing local component arity with target PK arity;
- comparing local component kind against the accepted target kind;
- normalizing local relation component kind with `relation_local_component_key_kind`;
- constructing target `PrimaryKeyValue` boundaries later in the path.

Representative sites:

- `crates/icydb-core/src/db/relation/save_validate.rs:225`
- `crates/icydb-core/src/db/relation/reverse_index.rs:490`
- `crates/icydb-core/src/db/relation/save_validate.rs:546`
- `crates/icydb-core/src/db/relation/reverse_index.rs:627`

This is the strongest current consolidation candidate. The implementation should not merge save validation and reverse-index mutation preparation into one execution path. The safe extraction is a small accepted relation-edge descriptor builder owned by `db::relation`, plus one shared local-kind normalization helper.

Recommended action: build one shared accepted relation-edge descriptor and consume it from save validation and reverse-index preparation.

### D4 - Relation Full Target Identity Paths

The same invariant appears in save validation, reverse-index storage, delete blocking, and schema validation: a relation target is the full accepted `PrimaryKeyValue`, never the first primary-key component.

This repetition is intentional and should remain visible at the runtime boundaries. The only DRY improvement should be the shared descriptor from D2/D3.

Consolidation safety: keep boundary repetition; share descriptor construction only.

### D5 - Route Capability Facts

The previous route-capability duplication has improved. Current code centralizes fact derivation in `db/executor/planning/route/capability_facts.rs`, with consumers in load and aggregate route hints.

No action recommended. Further compression would risk hiding planner/executor boundary intent for little gain.

### D6 - Cursor Contract Transport

Cursor boundary, continuation, runtime, and spine modules intentionally keep distinct responsibilities. The repeated vocabulary around boundary validation, token materialization, and runtime continuation is protective.

No action recommended.

### D7 - SQL Result Shell Mapping

The SQL query/update execution surface still contains small repeated result-shell mappings for describe/show/explain/write result variants. This is facade repetition rather than semantic duplication.

Recommended action: optional local helper only if this file is already being touched. Do not make this a standalone cleanup slice.

### D8 - Accepted Generated-Compatible Row-Decode Proof

The accepted/generated-compatible row-decode bridge remains a defensive boundary. It protects accepted schema authority from generated-model fallback reconstruction. The repetition here is not a DRY failure.

No action recommended.

### D9 - Commit Marker Envelope

Commit marker, replay, and envelope guards intentionally repeat checks at durable boundaries. This remains boundary-protected defensive duplication.

No action recommended.

### D10 - Startup Field-Path and Expression Adapters

The split between startup field-path reconciliation and startup expression reconciliation is correct. They share schema mutation language, but they do not own identical invariants.

No action recommended.

### D11 - Index Reader / Preflight Overlay

Index reader and preflight paths still duplicate some key/entry vocabulary, but this is mostly boundary-protected. The recent key-owned index-entry hard cut reduces the earlier risk that index values would appear to own row identity.

No action recommended.

### D12 - Schema DDL Error Construction

`schema/reconcile.rs` constructs many `store_unsupported(format!(...))` errors in the DDL publication path. This is not currently a blocker, but it increases wording drift as SQL DDL support grows.

Recommended action: address with D1 if a publication gate helper is extracted. Avoid a separate error-string-only cleanup.

## Module Pressure Readout

| Module | Current pressure | Readout |
| --- | --- | --- |
| `crates/icydb-core/src/db/schema/reconcile.rs` | high local density, medium DRY pressure | One large owner now contains startup reconciliation, SQL DDL publication, drift tests, and metadata-change guards. Extract only publication envelopes, not schema authority. |
| `crates/icydb-core/src/db/relation/reverse_index.rs` | high local density, medium DRY pressure | Reverse-index logic is large but coherent. The duplicate accepted-edge lowering should be shared with save validation. |
| `crates/icydb-core/src/db/relation/save_validate.rs` | medium density, medium-low DRY pressure | Save validation should keep its runtime boundary, but consume a shared accepted-edge descriptor. |
| `crates/icydb-core/src/db/session/sql/execute/mod.rs` | medium density, low-medium DRY pressure | Public SQL entrypoint shells repeat result plumbing intentionally. Optional local helper only. |
| `crates/icydb-core/src/db/cursor/*` | medium density, low DRY pressure | Separation is role-based and protective. |

## Dangerous Consolidations

Do not consolidate these even though they share vocabulary:

- schema mutation proposal/reconciliation with SQL DDL frontend execution;
- save-time relation validation with reverse-index mutation preparation;
- reverse-index source identity with target identity;
- cursor token decoding with planner route selection;
- accepted row-decode authority with generated model convenience paths;
- commit marker storage with payload decoding or replay execution;
- physical startup reconciliation adapters for field-path and expression indexes.

These are authority or lifecycle boundaries, not accidental duplication.

## Recommended Patch Slices

| Priority | Slice | Owner boundary | Action | Expected benefit |
| --- | --- | --- | --- | --- |
| P1 | SQL DDL publication gate helper | `db::schema::reconcile` | Extract the common transition/admission/snapshot-write envelope for SQL DDL metadata publication. | Reduces future DDL drift; estimated 20-40 LoC. |
| P1 | Relation accepted-edge descriptor | `db::relation` | Share accepted relation-edge arity/kind/target metadata construction between save validation and reverse-index preparation. | Reduces relation tuple drift; estimated 20-35 LoC. |
| P2 | Relation local-kind helper dedupe | `db::relation` | Move duplicated `relation_local_component_key_kind` to one owner-local helper. | Small but high-confidence cleanup. |
| P3 | SQL result shell helper | `db::session::sql::execute` | Optional helper for result-shell publication when this file is next touched. | Minor call-site compression. |

No mandatory follow-up is triggered by this audit because high-risk divergence-prone patterns remain at zero.

## Verification Readout

| Command | Status | Notes |
| --- | --- | --- |
| `find docs/audits/reports -path '*dry-consolidation*.md' -print` | PASS | Located latest comparable baseline at `docs/audits/reports/2026-05/2026-05-14/dry-consolidation.md`. |
| `git rev-parse --short HEAD` | PASS | Snapshot identifier: `b144c6bca`. |
| `find crates/icydb-core/src -type f -name '*.rs' | wc -l` | PASS | In-scope Rust file count: 983. |
| `rg -n "fn execute_sql_ddl_\|insert_persisted_snapshot\|decide_schema_transition" crates/icydb-core/src/db/schema/reconcile.rs` | PASS | Confirmed repeated SQL DDL publication gates. |
| `rg -n "fn accepted_.*relation.*edge\|relation_local_component_key_kind" crates/icydb-core/src/db/relation` | PASS | Confirmed accepted relation-edge lowering duplication. |
| `rg -n "capabilit" crates/icydb-core/src/db/executor/planning` | PASS | Confirmed current route-capability fact owner and consumers. |

Runtime tests were not run because this audit produced documentation only and did not change production code.


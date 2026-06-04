# DRY Consolidation Audit - 2026-06-04

## Report Preamble

| Field | Value |
| --- | --- |
| Scope | Crosscutting DRY and redundancy scan over `crates/icydb-core/src`; tests, benches, examples, and generated artifacts excluded except where noted as supporting evidence. |
| Audit definition | `docs/audits/recurring/crosscutting/crosscutting-dry-consolidation.md` |
| Compared baseline | `docs/audits/reports/2026-05/2026-05-28/dry-consolidation.md` |
| Code snapshot | `41f89f5c9`, clean worktree at scan start. |
| Method tag/version | `DRY-1.2` |
| Comparability status | comparable; scope, taxonomy, owner-layer model, and safety model match the 2026-05-28 baseline. |

Method manifest:

```text
method_version = DRY-1.2
duplication_taxonomy = DT-1
owner_layer_taxonomy = OL-1
invariant_role_model = IR-1
facade_inclusion_rule = FI-1
consolidation_safety_model = CS-1
```

## Evidence Artifacts

- `docs/audits/reports/2026-06/2026-06-04/artifacts/dry-consolidation/dry-consolidation-pattern-counts.tsv`
- `docs/audits/reports/2026-06/2026-06-04/artifacts/dry-consolidation/dry-consolidation-module-pressure.tsv`

## Executive Summary

Verdict: **PASS with moderate cleanup pressure**.

The current tree has no high-risk divergence-prone duplication pattern. The most important change since the 2026-05-28 baseline is that the relation target contract pressure improved: accepted relation target contract lookup, relation local-kind normalization, and relation primary-key component validation now live in `db::relation` and are consumed by save validation and reverse-index preparation.

The remaining meaningful DRY pressure is owner-local. `db::schema::reconcile` is still large and repeats SQL DDL publication wrapper shape across field, index, default, nullability, rename, and drop operations, but 0.178 moved schema-owned admission, mutation execution, and identity construction into dedicated modules and added shared `require_sql_ddl_transition_plan` / `publish_sql_ddl_accepted_snapshot` helpers. This is cleanup pressure, not an authority violation.

DRY risk index: **5 / 10**.

## Baseline Delta

| Metric | 2026-05-28 baseline | 2026-06-04 current | Delta |
| --- | ---: | ---: | ---: |
| In-scope Rust files | 983 | 1008 | +25 |
| Total duplication patterns | 12 | 11 | -1 |
| High-risk divergence-prone patterns | 0 | 0 | 0 |
| Same-layer accidental patterns | 1 | 0 | -1 |
| Cross-layer intentional patterns | 4 | 4 | 0 |
| Defensive duplication patterns | 1 | 1 | 0 |
| Boundary-protected patterns | 9 | 9 | 0 |
| Invariants repeated in more than 3 places | 8 | 8 | 0 |
| Error-construction families repeated more than 3 times | 3 | 3 | 0 |
| Safe LoC reduction estimate | 35-75 | 28-60 | improved |

## Pattern Inventory

| ID | Pattern | Primary locations | Taxonomy | Risk | Classification |
| --- | --- | --- | --- | --- | --- |
| D1 | SQL DDL publication wrappers and metadata validators | `crates/icydb-core/src/db/schema/reconcile.rs:67`, `:112`, `:148`, `:238`, `:349`, `:429`, `:585`, `:684` | same-owner boilerplate plus operation-specific validation | medium | MIGRATE CALLS THEN EXTRACT LOCALLY |
| D2 | SQL DDL transition/publication envelope | `crates/icydb-core/src/db/schema/reconcile.rs:193`, `:220` | partially consolidated owner-local envelope | low-medium | KEEP HELPER; EXPAND ONLY WHEN TOUCHING |
| D3 | Accepted relation-edge tuple construction | `crates/icydb-core/src/db/relation/save_validate.rs:228`, `crates/icydb-core/src/db/relation/reverse_index.rs:499` | boundary-sensitive owner-local overlap | medium-low | SHARE DESCRIPTOR ONLY IF LOCAL |
| D4 | Relation target contract and component-kind helpers | `crates/icydb-core/src/db/relation/mod.rs:109`, `:195`, `:224` | consolidated helper boundary | low | KEEP |
| D5 | Route capability facts through load/aggregate hint consumers | `crates/icydb-core/src/db/executor/planning/route/capability_facts.rs`, `hints/load.rs`, `hints/aggregate.rs` | boundary-protected | low-medium | KEEP |
| D6 | Cursor continuation contract transport | `crates/icydb-core/src/db/cursor/*`, `crates/icydb-core/src/db/query/plan/continuation.rs`, `crates/icydb-core/src/db/executor/planning/continuation/*` | boundary-protected | low-medium | KEEP |
| D7 | SQL query/update/result shell mapping | `crates/icydb-core/src/db/session/sql/execute/mod.rs`, `write_returning.rs`, `sql/mod.rs` | facade shell repetition | low-medium | OPTIONAL LOCAL HELPER |
| D8 | Schema mutation admission/execution/identity modules | `crates/icydb-core/src/db/schema/mutation/ddl_admission.rs`, `execution.rs`, `identity.rs`, `delta.rs` | protective split | low | KEEP |
| D9 | Startup field-path and expression reconciliation adapters | `crates/icydb-core/src/db/schema/reconcile/startup_field_path.rs`, `startup_expression.rs` | phase-separated boundary | medium-low | KEEP |
| D10 | Accepted generated-compatible row-decode proof propagation | accepted-schema consumers under executor/session/runtime | defensive boundary | low-medium | KEEP |
| D11 | Commit marker envelope and replay guard duplication | `crates/icydb-core/src/db/commit/*` | defensive boundary | low-medium | KEEP |

## Detailed Findings

### D1 / D2 - SQL DDL Publication Shape

`schema/reconcile.rs` still contains eight SQL DDL execution entrypoints. They repeat a visible pattern: fetch accepted-before and accepted-after snapshots from the derivation, assert the expected transition kind or metadata-only shape, verify operation-specific target drift, then publish through the accepted identity guard.

0.178 improved the baseline pressure by extracting:

- `require_sql_ddl_transition_plan`;
- `publish_sql_ddl_accepted_snapshot`;
- schema-owned DDL admission and accepted-after derivation;
- schema mutation execution and identity modules.

The remaining repetition is now mostly per-operation metadata validation and error construction. A local helper could still reduce future drift, but it should not hide the operation-specific fail-closed checks or move publication semantics into SQL DDL.

Recommended cleanup: one local `SqlDdlPublicationEnvelope` or equivalent helper in `db::schema::reconcile` only when that file is already being touched.

### D3 / D4 - Relation Accepted-Edge Construction

The baseline's strongest relation helper duplication is partly resolved. Shared helpers now live in `db::relation`:

- `accepted_relation_edge_target_contract`;
- `validate_relation_primary_key_component_kind`;
- `relation_local_component_key_kind`.

Save validation and reverse-index preparation still independently build their own relation info structs from accepted relation edges. That split is mostly protective because the consumers have different runtime roles. The safe DRY target is a descriptor for common tuple relation facts, not a merged save/reverse execution path.

Recommended cleanup: optional owner-local descriptor extraction if relation code is touched. Do not merge save validation and reverse-index mutation preparation.

### D5 / D6 - Route and Cursor Boundaries

Route capability facts and cursor continuation contracts remain intentionally repeated across derivation and consumption boundaries. The current structure keeps capability ownership in route planning and continuation meaning in planner/cursor contracts while allowing runtime consumers to validate or revalidate their local state.

No cleanup recommended.

### D7 - SQL Result Shell Mapping

`session/sql/execute/mod.rs` repeats small query/update/result shell mappings for compiled, context-owned, and owned execution paths. This is facade plumbing, not semantic duplication.

Recommended cleanup: optional helper only if this module is touched for other reasons.

### D8 / D9 - Schema Mutation Split

0.178 reduced prior schema mutation hub pressure by splitting DDL admission, delta classification, execution/preflight contracts, and identity construction. The vocabulary repeats across these modules because the modules are different schema-owned phases, not because policy forked.

No consolidation recommended across these modules.

## Dangerous Consolidations

Do not consolidate these even though they share vocabulary:

- SQL DDL frontend binding with schema-owned mutation admission or publication;
- schema mutation admission with runner execution/preflight;
- schema mutation runtime identity with runner diagnostics;
- save-time relation validation with reverse-index mutation preparation;
- cursor token decoding with planner continuation contract derivation;
- route capability derivation with hint consumers;
- commit marker store-envelope validation with payload codec logic.

These are authority or lifecycle boundaries, not accidental duplication.

## Recommended Patch Slices

| Priority | Slice | Owner boundary | Action | Expected benefit |
| --- | --- | --- | --- | --- |
| P1 | SQL DDL publication envelope cleanup | `db::schema::reconcile` | Extract a small owner-local envelope for transition-plan validation, target drift context, and identity-checked publication where it does not hide operation-specific checks. | Reduces future DDL drift; estimated 18-35 LoC. |
| P2 | Relation tuple-edge descriptor | `db::relation` | Share accepted relation-edge tuple arity/kind/target facts between save validation and reverse-index preparation. | Reduces relation tuple drift; estimated 10-20 LoC. |
| P3 | SQL result shell helper | `db::session::sql::execute` | Compress repeated result/cache attribution shells when the file is next touched. | Minor call-site reduction; estimated 4-8 LoC. |

No mandatory follow-up is triggered because high-risk divergence-prone patterns remain at zero.

## Verification Readout

| Command | Status | Notes |
| --- | --- | --- |
| `find docs/audits/reports -path '*dry-consolidation.md' \| sort \| tail -10` | PASS | Latest comparable baseline is `docs/audits/reports/2026-05/2026-05-28/dry-consolidation.md`. |
| `git rev-parse --short HEAD` | PASS | Snapshot identifier: `41f89f5c9`. |
| `git status --short` | PASS | Clean at audit start. |
| `find crates/icydb-core/src -type f -name '*.rs' \| wc -l` | PASS | In-scope Rust file count: 1008. |
| `wc -l` over main pressure modules | PASS | Captured module pressure for schema reconcile/mutation, relation, and SQL execute modules. |
| `rg -n "pub\\(in crate::db\\) fn execute_sql_ddl_" crates/icydb-core/src/db/schema/reconcile.rs` | PASS | Found eight SQL DDL execution wrappers. |
| `rg -n "fn validate_sql_ddl_.*metadata_change\|fn require_sql_ddl_transition_plan\|fn publish_sql_ddl_accepted_snapshot"` | PASS | Confirmed shared transition/publish helpers and remaining metadata validators. |
| `rg -n "accepted_relation_edge_target_contract\|relation_local_component_key_kind\|validate_relation_primary_key_component_kind" crates/icydb-core/src/db/relation` | PASS | Confirmed shared relation helpers and remaining consumer overlap. |
| `bash scripts/ci/check-layer-authority-invariants.sh` | PASS | Layer authority invariants verified. |

Runtime tests were not run because this audit produced documentation and TSV artifacts only and did not change production code.

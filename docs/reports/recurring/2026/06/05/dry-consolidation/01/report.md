# DRY Consolidation Audit - 2026-06-05

## Report Preamble

| Field | Value |
| --- | --- |
| Scope | Crosscutting DRY and redundancy scan over `crates/icydb-core/src`; tests, benches, examples, and generated artifacts excluded except where noted as supporting evidence. |
| Audit definition | `docs/audits/recurring/crosscutting/crosscutting-dry-consolidation.md` |
| Compared baseline | `docs/audits/reports/2026-06/2026-06-04/dry-consolidation.md` |
| Code snapshot | `200ad67ca`, dirty worktree at scan start. |
| Method tag/version | `DRY-1.2` |
| Comparability status | comparable for taxonomy and risk classification; raw owner-file count is non-comparable because this run applies the documented tests-excluded file scope after large test-suite relocation. |

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

- `docs/audits/reports/2026-06/2026-06-05/artifacts/dry-consolidation/dry-consolidation-pattern-counts.tsv`
- `docs/audits/reports/2026-06/2026-06-05/artifacts/dry-consolidation/dry-consolidation-module-pressure.tsv`

## Executive Summary

Verdict: **PASS with very low cleanup pressure after owner-local consolidation**.

The current tree still has no high-risk divergence-prone duplication pattern. The largest DRY improvement since the previous run is scan quality plus applied owner-local cleanup: large inline test suites have moved into sibling `tests.rs` or `tests/` modules, SQL DDL same-layout field metadata validation and checked publication now share local helpers, relation scalar target descriptors and runtime descriptor construction are centralized in their owners, and SQL write/metadata result-cache plumbing has shared helpers.

Remaining duplication pressure is owner-local and smaller. SQL DDL reconciliation still has operation-specific publication wrappers, and relation save/reverse-index code still keeps separate runtime descriptors for different lifecycle roles. Those are valid boundaries, not blocking DRY risks.

DRY risk index: **3.4 / 10**.

## Baseline Delta

| Metric | 2026-06-04 baseline | 2026-06-05 current | Delta |
| --- | ---: | ---: | ---: |
| In-scope owner Rust files | N/A | 829 | N/A |
| Total duplication patterns | 11 | 7 | -4 |
| High-risk divergence-prone patterns | 0 | 0 | 0 |
| Same-layer accidental patterns | 0 | 0 | 0 |
| Cross-layer intentional patterns | 4 | 4 | 0 |
| Defensive duplication patterns | 1 | 1 | 0 |
| Boundary-protected patterns | 9 | 8 | -1 |
| Invariants repeated in more than 3 places | 8 | 8 | 0 |
| Error-construction families repeated more than 3 times | 3 | 3 | 0 |
| Safe LoC reduction estimate | 28-60 | 0-8 | improved |

## Pattern Inventory

| ID | Pattern | Primary locations | Taxonomy | Risk | Classification |
| --- | --- | --- | --- | --- | --- |
| D1 | SQL DDL transition/publication wrappers | `crates/icydb-core/src/db/schema/reconcile/sql_ddl.rs`, `crates/icydb-core/src/db/schema/reconcile/sql_ddl/field_metadata.rs` | shared local helpers plus operation-specific checks | low | KEEP OPERATION-SPECIFIC |
| D2 | SQL DDL field-metadata validators | `crates/icydb-core/src/db/schema/reconcile/sql_ddl/field_metadata.rs` | shared metadata walker plus operation-specific fail-closed checks | low | KEEP |
| D3 | Accepted relation tuple fact construction | `crates/icydb-core/src/db/relation/save_validate.rs`, `crates/icydb-core/src/db/relation/reverse_index.rs`, `crates/icydb-core/src/db/relation/mod.rs` | shared scalar/tuple descriptors with consumer-local runtime constructors | low | KEEP |
| D4 | Relation target contract and component-kind helpers | `crates/icydb-core/src/db/relation/mod.rs:181`, `:267`, `:296` | consolidated helper boundary | low | KEEP |
| D5 | Route capability facts through hint consumers | `crates/icydb-core/src/db/executor/planning/route/capability_facts.rs`, route hint consumers | intentional boundary duplication | low-medium | KEEP |
| D6 | Cursor continuation contract transport | `crates/icydb-core/src/db/cursor`, `crates/icydb-core/src/db/query/plan/continuation.rs`, executor continuation modules | intentional boundary duplication | low-medium | KEEP |
| D7 | SQL query/update/result shell mapping | `crates/icydb-core/src/db/session/sql/execute/mod.rs` and SQL facade call sites | facade shell repetition with shared write and metadata result/cache helpers | low | KEEP |
| D8 | Schema mutation admission/execution/identity vocabulary | `crates/icydb-core/src/db/schema/mutation/ddl_admission.rs`, `execution.rs`, `identity.rs`, `delta.rs` | protective schema-owned phase split | low | KEEP |
| D9 | Startup field-path and expression reconciliation adapters | `crates/icydb-core/src/db/schema/reconcile/startup_field_path.rs`, `startup_expression.rs` | phase-separated boundary | low | KEEP |
| D10 | Commit marker, replay guard, and accepted row-decode proof propagation | commit/replay and accepted-schema consumers | defensive boundary duplication | low-medium | KEEP |

## Detailed Findings

### D1 / D2 - SQL DDL Reconciliation Shape

SQL DDL reconciliation has improved since the 0.178 work: transition planning, publication identity checking, mutation execution, and fingerprint identity are no longer SQL-owned decisions. This cleanup also consolidated the repeated default/nullability/rename field walk into one `validate_sql_ddl_single_field_metadata_change` helper, centralized the DROP INDEX ready-state diagnostic, and shared the checked metadata publication shell for the metadata-only field operations that do not need row scans.

The remaining repetition is safe only while it stays inside `db::schema::reconcile`. Do not hide target-specific checks such as index identity, field metadata drift, ownership, retained slots, or unsupported physical work.

### D3 / D4 - Relation Tuple Facts

Relation helper ownership is in better shape than earlier baselines. `accepted_strong_scalar_relation_target_descriptor`, `accepted_relation_tuple_edge_descriptor`, `accepted_relation_edge_target_contract`, `validate_relation_primary_key_component_kind`, and `relation_local_component_key_kind` are shared from `db::relation`, while save validation and reverse-index preparation retain separate runtime descriptors with local constructors.

The remaining duplication is not a reason to merge save-time validation and reverse-index mutation preparation. Those consumers have different lifecycle roles and should stay separate.

### D5 / D6 - Route and Cursor Boundaries

Route capability and cursor continuation contracts still repeat vocabulary across derivation and consumption points. That is intentional boundary duplication: route planning owns eligibility facts, while runtime consumers validate or revalidate state through their local contracts.

No consolidation recommended.

### D7 - SQL Result Shell Mapping

`session/sql/execute/mod.rs` remains a large facade shell. Write metric/cache attribution and read-only metadata result/cache wrapping now run through shared helpers, and the remaining repetition is low-risk statement routing rather than semantic drift.

### D8 / D9 / D10 - Protective Repetition

Schema mutation modules intentionally repeat admission, execution, delta, and identity vocabulary because they represent different schema-owned phases. Startup reconciliation adapters and commit/replay guards are similarly protective: they keep generated reconciliation, accepted schema proof, and recovery boundaries explicit.

These should not be collapsed for DRY reasons.

## Dangerous Consolidations

Do not consolidate these despite repeated vocabulary:

- SQL DDL frontend binding with schema-owned mutation admission or publication.
- Schema mutation admission with runner execution/preflight.
- Save-time relation validation with reverse-index mutation preparation.
- Cursor token decoding with planner continuation contract derivation.
- Route capability derivation with hint consumers.
- Commit marker store-envelope validation with payload codec logic.

These are authority or lifecycle boundaries, not accidental duplication.

## Recommended Patch Slices

| Priority | Slice | Owner boundary | Action | Expected benefit |
| --- | --- | --- | --- | --- |
| P1 | SQL DDL publication envelope cleanup | `db::schema::reconcile` | No immediate action; keep future DDL additions on existing envelope helpers. | Watch-only. |
| P2 | Relation runtime descriptor cleanup | `db::relation` | No immediate action; keep save and reverse runtime descriptors separate. | Watch-only. |
| P3 | SQL facade shell cleanup | `db::session::sql::execute` | No immediate action; avoid larger facade restructuring unless execution routing changes. | Watch-only. |

No mandatory follow-up is triggered because high-risk divergence-prone patterns remain at zero.

## Verification Readout

| Command | Status | Notes |
| --- | --- | --- |
| `git rev-parse --short HEAD` | PASS | Snapshot identifier: `200ad67ca`. |
| `git status --short` | PASS | Dirty worktree at audit start; CLI changes are from another session and were not modified by this audit. |
| `wc -l` over main pressure modules | PASS | Captured schema reconcile/mutation, relation, and SQL execute module pressure. |
| `rg` over SQL DDL transition/publication helpers | PASS | Confirmed shared DDL helpers and remaining operation-local validators. |
| `rg` over relation accepted-target helpers | PASS | Confirmed shared relation helper ownership and remaining consumer-local tuple facts. |
| `bash scripts/ci/check-layer-authority-invariants.sh` | PASS | Layer authority invariants verified. |
| `bash scripts/ci/check-sql-branch-ownership-invariants.sh` | PASS | `recomputed_decision_count=0`, `propagated_decision_count=0`, `tracked_decisions=5`. |
| `bash scripts/ci/check-module-structure-hub-thresholds.sh` | PASS | Module-structure hub thresholds verified after cleanup. |
| `cargo test -p icydb-core --lib db::schema::reconcile` | PASS | 25 passed after SQL DDL metadata cleanup. |
| `cargo test -p icydb-core --lib db::relation` | PASS | 20 passed after relation descriptor cleanup. |
| `cargo test -p icydb-core --lib db::session::sql` | PASS | 3 passed after SQL write result/cache helper cleanup. |
| `cargo test -p icydb-core --lib` | PASS | 3698 passed, 2 ignored after the relation filter fix. |

Broad core library tests and focused post-cleanup tests covered the changed owner modules.

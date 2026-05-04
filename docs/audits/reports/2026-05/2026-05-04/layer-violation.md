# Layer Violation Audit - 2026-05-04

## Report Preamble

- scope: authority layering and semantic ownership boundaries across db runtime modules
- compared baseline report path: `docs/audits/reports/2026-04/2026-04-21/layer-violation.md`
- code snapshot identifier: `13ec2bef4` with dirty working tree at scan time
- method tag/version: `Method V3`
- comparability status: `comparable`, snapshot-qualified because two source files were already modified in the working tree

## Evidence Artifacts

- `docs/audits/reports/2026-05/2026-05-04/artifacts/layer-violation/layer-health-snapshot.txt`
- `docs/audits/reports/2026-05/2026-05-04/artifacts/layer-violation/route-planner-import-boundary.txt`
- `docs/audits/reports/2026-05/2026-05-04/artifacts/layer-violation/working-tree-scope.tsv`

## Working Tree Scope

| Path | Audit Treatment | Layer Impact |
| ---- | ---- | ---- |
| `crates/icydb-core/src/db/schema/describe.rs` | treated as existing user work | positive authority movement: accepted-schema describe fields now come from the persisted schema snapshot instead of generated model field order |
| `crates/icydb-core/src/db/session/mod.rs` | treated as existing user work | matching call-site change for the accepted-schema describe helper |
| `docs/audits/reports/2026-05/2026-05-04/*` | audit output from current run series | documentation only |

## Findings

| Check | Evidence | Status | Risk |
| ---- | ---- | ---- | ---- |
| Upward imports and cross-layer policy re-derivations | `bash scripts/ci/check-layer-authority-invariants.sh` | PASS (`0` upward imports, `0` policy re-derivations, `0` cross-layer predicate duplication`) | Low |
| Route planner import boundary | `bash scripts/ci/check-route-planner-import-boundary.sh` | PASS (`1` root import family: `executor`) | Low |
| Access and route authority fan-out | layer-health snapshot (`AccessPath decision owners: 2`, `RouteShape decision owners: 2`) | PASS; route shape owner count is lower than the 2026-04-21 report's `3` | Low |
| Predicate coercion ownership concentration | layer-health snapshot (`Predicate coercion owners: 4`, `Predicate boundary drift imports: 3`) | PASS; unchanged from tracked baseline ceilings | Medium |
| Enum fan-out beyond two layers | layer-health snapshot (`Enum fan-out > 2 layers: 1`; `AggregateKind::=4`) | PASS; `AggregateKind` is now at the script baseline of `4` and should remain watched | Medium |
| Ordering / comparator leakage outside index | layer-health snapshot (`Comparator definitions outside index: 0`) | PASS | Low |
| Canonicalization entrypoint sprawl | layer-health snapshot (`Canonicalization entrypoints: 1`) | PASS | Low |
| Commit marker low-level storage access | strict invariant script checks `with_commit_store(...)` outside `db/commit/*` | PASS; no leak reported | Low |
| Continuation envelope ownership | strict invariant script checks `anchor_within_envelope`, `resume_bounds_from_refs`, and `continuation_advanced` outside `db/index/envelope/mod.rs` | PASS; no leak reported | Low |
| Accepted schema describe authority | targeted inspection of dirty `describe.rs` and `session/mod.rs` edits | PASS; accepted schema description now uses `AcceptedSchemaSnapshot::persisted_snapshot()` for field order, field kind, slots, and nested leaves | Low |
| Runtime compile with current boundary wiring | `cargo check -p icydb-core --features sql` | PASS | Low-Medium |

## Interpretation

- This run remains clean from a strict layer-violation perspective.
- The tracked invariant script reports `0` upward imports, `0` cross-layer policy re-derivations, and `0` cross-layer predicate duplication.
- `RouteShape` decision ownership improved from `3` in the 2026-04-21 report to `2` in this run.
- `AggregateKind` fan-out is now reported as `4`, which is within the current script baseline but higher than the 2026-04-21 report's `3`; keep this as a monitoring item, not a violation.
- The dirty accepted-schema describe changes move in the correct direction for layer authority: when an accepted schema snapshot exists, the describe surface projects accepted persisted schema metadata rather than generated model field ordering.

## Legitimate Cross-Cutting (Do Not Merge)

| Area | Why Redundant | Risk If Merged |
| --- | --- | --- |
| Planner continuation contracts and cursor/runtime transport | keeps semantic contract ownership in planning while runtime layers validate and transport cursor state locally | High |
| Route capability derivation and route-hint consumers | preserves route-owned capability truth while allowing downstream hint and execution code to consume snapshots without re-deriving policy | High |
| Commit marker store envelope and payload codec | preserves the stable-storage trust boundary while marker payload code remains responsible only for payload shape | High |
| Predicate capability classification and runtime/index consumers | preserves predicate-owned capability truth while runtime and index compilation apply that truth in their own layers | Medium |
| Accepted schema snapshots and generated row codecs | accepted runtime schema owns layout truth; generated codecs remain compatibility-guarded extraction bridges until accepted field decode owns materialization | Medium-High |
| Runtime `Value` boundary adapters and typed slot codecs | runtime boundary conversion may use descriptor validation, but typed persistence codecs must remain statically owned and must not make `Value` persistable | High |

## Follow-Up Actions

- No mandatory layer-violation follow-up actions for this run.
- Monitoring-only: keep `AggregateKind` fan-out at or below the current four-layer baseline unless a later audit deliberately updates the model.
- Monitoring-only: keep predicate coercion ownership at the current four-owner set.
- Optional: after the accepted-schema describe edit lands, add or update focused tests proving `describe_entity_fields_with_persisted_schema(...)` follows persisted snapshot field order and nested leaves. This is a correctness guard, not a layer repair.

## Verification Readout

- `bash scripts/ci/check-layer-authority-invariants.sh` -> PASS
- `bash scripts/ci/check-route-planner-import-boundary.sh` -> PASS
- `cargo check -p icydb-core --features sql` -> PASS

## Verdict

No strict layer violations were detected. The current working tree is snapshot-qualified because it includes in-progress accepted-schema describe edits, but those edits move authority toward the accepted schema snapshot rather than leaking schema authority back to generated models.

Cross-cutting risk index: **3.2/10**

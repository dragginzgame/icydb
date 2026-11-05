> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Invariant Preservation Audit

## Run Metadata + Comparability Note

- scope: `invariant-preservation`

- recurring definition:
  `docs/audits/recurring/integrity/invariant-preservation.md`

- compared baseline report path:
  [report.md](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/05/11/invariant-preservation/01/report.md)

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

## Verification Readout

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

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/08/27/invariant-preservation/01/report.md).

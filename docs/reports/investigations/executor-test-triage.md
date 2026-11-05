> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Executor Test Triage

Date: 2026-03-26

Scope: pruned files under `crates/icydb-core/src/db/executor/tests` that are not part of the revived live subset.

## Summary

The revived subset is now:

- `continuation_structure`

- `cursor_validation`

- `lifecycle`

- `live_state`

- `load_structure`

- `metrics`

- `mutation_save`

- `ordering`

- `pagination`

- `post_access`

- `reverse_index`

- `semantics`

## Recommended Order

1. Delete the obsolete wrapper files after their surviving cases are accounted for.
2. Migrate route-owner tests file by file.
3. Split `semantics.rs` into owner-local slices.
4. Only then consider aggregate matrix revival.

## Follow-Up Status

- The primary-key set-access runtime contracts for `by_ids` dedup, PK-union dedup, and recursive union execution-descriptor shape now live in `db/executor/tests/set_access.rs`.

- The simple intersection set-access runtime contracts for canonical asc/desc overlap, no-overlap empty result, duplicate suppression, and recursive intersection execution-descriptor shape now live in `db/executor/tests/set_access.rs`.

- The nested composite overlap and descending paged union/intersection continuation contracts now live in `db/executor/tests/set_access.rs`.

- The three-child descending union continuation stress case now also lives in `db/executor/tests/set_access.rs`.

- The post-access filtering contracts for `by_id` plus optional equality and ordered pagination over mixed `IN` + text predicates now live in `db/executor/tests/post_access.rs`.

- The post-access `contains(...)` filtering and filtered delete-limit contracts now also live in `db/executor/tests/post_access.rs`.

- The strong-relation delete guard, reverse-index lifecycle, and reverse-index recovery contracts now live in `db/executor/tests/reverse_index.rs`.

- The stale-secondary missing-row-policy contracts now live in `db/executor/tests/stale_secondary.rs`.

- The optional-order missing-value ordering contract now lives in `db/executor/tests/ordering.rs`.

- The ordered delete-limit runtime contract now also lives in `db/executor/tests/ordering.rs`.

- The small no-cursor limit-window pagination contracts now live in `db/executor/tests/pagination.rs`, and the old `pagination/limit_no_cursor_matrix.rs` file has been deleted.

- The core primary-key cursor resume, next-boundary, and PK fast-vs-fallback pagination parity contracts now also live in `db/executor/tests/pagination.rs`.

- The first secondary index-range pagination parity slice now also lives in `db/executor/tests/pagination.rs`: paged window parity, pushdown-vs-fallback cursor-boundary parity, and shared-boundary resume parity.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/03/26/executor-test-triage/01/report.md).

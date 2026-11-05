> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# DB Redundancy, Shim, and Backwards-Compatibility Audit - 2026-05-01

## Report Preamble

- scope: `crates/icydb-core/src/db/`

- code snapshot identifier: `c88e171f43` (`dirty` working tree)

- audit focus:
  - redundant production code
  - transitional shims / bridge modules
  - backwards-compatibility or multi-version support paths
  - test-only legacy helpers that can hide architectural debt

- non-goal: this report does not classify normal route fallback behavior as
  backwards compatibility

## Executive Summary

The production `db/` tree does **not** show a major backwards-compatibility
problem. Persisted row, commit marker, and commit control-slot formats are
versioned but fail closed against unsupported versions. That matches the
current hard-cut policy better than decode fallback support would.

The real redundancy/shim pressure is architectural:

- grouped aggregate expression access still has a compatibility re-export
  module

- structural codecs have short alias namespaces while the original
  `structural_field` functions remain the real owner

- predicate/expression canonicalization still carries legacy plain-`Expr` and
  runtime-`Predicate` bridge wrappers

- `db::scalar_expr` remains a separate scalar micro-engine for index and
  predicate transforms, overlapping a narrow subset of the compiled expression
  function surface

- some test support still names and preserves old helper contracts, especially
  session SQL helpers

No finding below requires an emergency fix before normal development resumes.
The best cleanup order is to remove the true shims first, then tighten stage
artifacts and test support once grouped/expression consolidation settles.

## Method

Commands and searches used:

- `find crates/icydb-core/src/db -type f | sort`

- `find crates/icydb-core/src/db -type f -name '*.rs' | wc -l`

- `find crates/icydb-core/src/db -name '*.rs' -print0 | xargs -0 wc -l | sort -nr | head -60`

- `rg -n -i "\b(shim|compat|compatibility|legacy|deprecated|backward|backwards|fallback|transitional|temporary|TODO|FIXME|remove after|old|obsolete|adapter|bridge|re-export|reexport)\b" crates/icydb-core/src/db`

- `rg -n -i "compatibility re-export|legacy|deprecated|obsolete|old wrapper|old shared|old executor|old entity-row|old grouped|test-only fallback|compatibility lane|compatibility contract|old-version|versioned|format_version|schema_version|migration|decode fallback|fallback decode|v1|v2" crates/icydb-core/src/db --glob '!**/tests/**' --glob '!**/tests.rs'`

- `rg -n "pub\(in crate::db[^)]*\) use crate::db|pub\(crate\) use crate::db|pub use crate::db" crates/icydb-core/src/db`

- duplicate-name scans over non-test functions, structs, enums, and traits

- targeted reads of the files listed in each finding

Scale:

- Rust files under `db/`: 834

- Rust lines under `db/`: about 276k, including tests

## Recommended Cleanup Order

1. Delete `executor::aggregate::contracts::state::grouped_expr`. `Done.`
2. Decide whether structural codec alias modules become the real owner or go
   away. `Partially done: collection/by_kind/storage_key removed; storage
   remains.`
3. Tighten predicate canonicalization APIs around `CanonicalExpr` and
   `PredicateCompilation`, then delete plain-`Expr` production adapters.
4. Document and test the `db::scalar_expr` overlap with compiled expressions.
5. Move test-only legacy helpers into named support modules.
6. Opportunistically shrink barrel re-exports during related ownership work.

## Verification

This report began as an audit-only pass. The follow-up cleanup removed the
grouped expression shim and three narrow structural codec alias modules.

Post-report sanity checks:

- `git diff --check` passed

- `cargo check -p icydb-core` passed after the grouped expression cleanup

- `cargo check -p icydb-core` passed after the structural alias cleanup

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/05/01/db-redundancy-shim-compatibility/01/report.md).

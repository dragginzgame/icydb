# IcyDB 0.308 package layout

The maintainer authorized native package/workspace migration and approved the
narrow Core dev-support exception. Existing Canic runtime deferral is retained.

## Landing slices

1. Separate fourteen native qualification packages from product workspace and
   release ownership, migrate paths and callers, and qualify both locked graphs.
   Preserve the two dev-only Core support packages under `crates/test-support/`
   without exposing new testing APIs. Keep package names, versions, features and
   selected suppliers unchanged.
2. Qualify release multi-lock wiring and resolver isolation; preserve Canic's
   separate older graph and report its existing qualification obstruction.

## Package mapping

| Package | Previous location | Current location |
| --- | --- | --- |
| icydb-testing-audit-default-empty-fixtures | `schema/audit/default_empty` | `qualification/schema/audit/default_empty` |
| icydb-testing-audit-group-path-fixtures | `schema/audit/group_path` | `qualification/schema/audit/group_path` |
| icydb-testing-audit-one-simple-fixtures | `schema/audit/one_simple` | `qualification/schema/audit/one_simple` |
| icydb-testing-audit-sql-perf-fixtures | `schema/audit/sql_perf` | `qualification/schema/audit/sql_perf` |
| icydb-testing-audit-ten-simple-fixtures | `schema/audit/ten_simple` | `qualification/schema/audit/ten_simple` |
| icydb-testing-demo-rpg-fixtures | `schema/demo/rpg` | `qualification/schema/demo/rpg` |
| icydb-testing-test-fixtures | `schema/tests/fixtures` | `qualification/schema/tests/fixtures` |
| icydb-testing-test-sql-fixtures | `schema/tests/sql` | `qualification/schema/tests/sql` |
| icydb-testing-build | `testing/build` | `qualification/build` |
| icydb-testing-codec-query | `testing/codec-query` | `qualification/codec-query` |
| icydb-testing-model-schema-only | `testing/model-schema-only` | `qualification/model-schema-only` |
| icydb-testing-model-typed-adapter | `testing/model-typed-adapter` | `qualification/model-typed-adapter` |
| icydb-testing-source-checks | `testing/source-checks` | `qualification/source-checks` |
| icydb-testing-sql-generator | `testing/sql-generator` | `crates/test-support/sql-generator` |
| icydb-testing-sqlite-reference | `testing/sqlite-reference` | `crates/test-support/sqlite-reference` |
| icydb-testing-wasm-helpers | `testing/wasm-helpers` | `qualification/wasm-helpers` |

## Status

Ready within the approved native scope. All 54 package identities retain one
owner: 9 product members (7 published, 2 unpublished Core dev-support), 14 native
qualification members, and 31 members of the retained Canic workspace. Package
versions and external supplier versions/checksums are unchanged. The Canic lock
is byte-identical to the migration input.

Both landing slices are complete. Cargo callers, formatting, documentation,
workflow fetches, dependency policy and full configured CI leaves select the
new native workspace. Common tooling admission checks its selection before
CI/build/release dispatch. Publication inventory uses Cargo membership, including
nested private packages. Release editor selection follows the saved source's
workspace owner; recovery for existing pre-split release sources is retained.
The consumer mapping binds both native locks, normalizes their selected release
metadata, and preserves older qualification receipts without renewing them.

## Focused validation

- Both locked native metadata graphs and all 54 package tooling declarations pass.
  Product-to-qualification edges are absent. Core alone consumes the dev-support
  crates from a published package, through dev-dependencies; the oracle uses the
  generator internally.
- Clippy passes for all fourteen relocated native packages and both dev-support
  packages. The 71 generator/oracle tests, Core's native SQLite comparison and
  the maintained build-plan test pass.
- Actual current-input preparation passes for both release groups in disposable
  copies. Both locks preserve every external supplier identity/checksum and all
  private qualification versions. Normalization passes for both groups.
- An impossible qualification-only Build requirement rejects qualification
  resolution while locked product metadata still resolves. The real manifest
  was restored byte-for-byte afterward.
- Consumer Cargo adoption, caller/publication, saved-editor selection, 153 workflow
  policy cases, 66 scanner cases and 25 documentation cases pass. All 698 local
  references across 82 current documents pass, together with formatting,
  selected shell lint and whitespace checks.

Evidence: `target/evidence/package-layout-0308/`. The new qualification lock has
intent-to-add bookkeeping so dependency policy can discover it; no file contents
were staged. No commits, package-version edits or delivery ran.

## Remaining boundary

The approved Canic placement exception under #596 remains. Its full runtime
resolution/qualification is still deferred under #373; only manifest membership,
shared path propagation and tooling admission are claimed here. Full repository,
release and macOS gates were not requested and remain skipped user-owned
validation. No networks were started. IC cycles/instructions and total/code-
section Wasm deltas are unmeasured. This does not claim complete removal of the
legacy Canic layout exception or activate selected-stage release omissions.

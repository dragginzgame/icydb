# Source inventory and coverage

Snapshot: 2026-11-11, HEAD `e19f3808c52c15a387a58a361599bd3afd8f7966` plus the existing local 0.310/Testkit/Host adoption. This is a working-tree audit, not qualification of the published commit. No production changes were made by this audit.

## Counting method

Inventory uses `rg --files crates qualification testing canisters schema scripts`, selecting `.rs`, then counts physical lines with Python `splitlines()`. This includes comments and blanks; it is **not cloc**, production LOC, compiled size, or a deletion estimate. Test-named means a `tests`/`test-support` path component or a `tests`/`*_tests` file stem. Inline tests and qualification fixture source remain in the other column. Do not compare this baseline with earlier 332k code-only counts. No generated target/cache source is counted.

| Source family | Rust files | Other physical lines | Test-named physical lines |
| --- | ---: | ---: | ---: |
| `canisters/audit` | 38 | 5961 | 1254 |
| `canisters/demo` | 2 | 141 | 0 |
| `canisters/tests` | 20 | 0 | 2746 |
| `crates/icydb` | 63 | 6300 | 4173 |
| `crates/icydb-cli` | 59 | 6858 | 8836 |
| `crates/icydb-core` | 1574 | 256323 | 193927 |
| `crates/icydb-diagnostic-code` | 7 | 3067 | 874 |
| `crates/icydb-model` | 168 | 11808 | 7320 |
| `crates/icydb-model-macros` | 92 | 13489 | 4265 |
| `crates/icydb-schema` | 52 | 9836 | 4973 |
| `crates/test-support` | 51 | 0 | 28311 |
| `qualification/build` | 29 | 4644 | 2006 |
| `qualification/codec-query` | 1 | 40 | 0 |
| `qualification/model-schema-only` | 4 | 217 | 261 |
| `qualification/model-typed-adapter` | 9 | 562 | 734 |
| `qualification/schema` | 54 | 1845 | 2327 |
| `qualification/startup` | 1 | 0 | 107 |
| `qualification/wasm-helpers` | 4 | 632 | 83 |
| `schema/audit` | 3 | 692 | 0 |
| `scripts/ci` | 1 | 277 | 0 |
| `testing/canic-adapter` | 42 | 62 | 888 |
| `testing/endpoint-installation` | 2 | 24 | 0 |
| `testing/integration` | 40 | 999 | 17280 |
| `testing/logical-memory` | 3 | 265 | 0 |
| `testing/model-facade-only` | 7 | 556 | 122 |

## Core database source families

These are search-priority signals, not complexity scores.

| Family | Other physical lines | Test-named physical lines |
| --- | ---: | ---: |
| `schema` | 57280 | 52938 |
| `query` | 43084 | 23308 |
| `session` | 16002 | 44408 |
| `executor` | 46458 | 13729 |
| `sql` | 15386 | 8827 |
| `data` | 9695 | 7568 |
| `index` | 7637 | 7635 |
| `commit` | 8257 | 4827 |
| `predicate` | 6616 | 5659 |
| `integrity` | 6839 | 2035 |
| `journal` | 4096 | 3172 |
| `resource` | 2193 | 2483 |
| `relation` | 2573 | 1763 |
| `access` | 3539 | 731 |
| `key_taxonomy` | 1809 | 1533 |
| `startup` | 1390 | 1825 |
| `cursor` | 2148 | 949 |
| `mutation_job` | 1771 | 720 |
| `sql_shared` | 1586 | 579 |
| `registry` | 1537 | 573 |
| `database_format` | 1018 | 1054 |
| `diagnostics` | 1195 | 101 |
| `numeric` | 365 | 686 |
| `tests` | 0 | 957 |
| `identity` | 388 | 486 |
| `codec` | 570 | 218 |
| `runtime_entity_catalog` | 467 | 285 |
| `resumable_job` | 613 | 70 |
| `dynamic_write` | 569 | 64 |
| `response` | 265 | 225 |
| `mod` | 469 | 0 |
| `read_set` | 336 | 64 |
| `positioned_overlay` | 241 | 108 |
| `ordered_overlay` | 132 | 137 |
| `catalog` | 222 | 0 |
| `scalar_expr` | 157 | 48 |
| `memory_runtime` | 89 | 75 |
| `write_context` | 67 | 0 |
| `direction` | 25 | 0 |
| `storage` | 19 | 0 |
| `helper_tests` | 0 | 11 |

## Workspace membership

Read from the three manifests without resolving or updating dependencies. All declarations are inventoried; this does not imply every package implementation was reviewed.

### `Cargo.toml` (9 declared member entries)

- `crates/icydb`
- `crates/icydb-cli`
- `crates/icydb-core`
- `crates/icydb-diagnostic-code`
- `crates/icydb-model`
- `crates/icydb-model-macros`
- `crates/icydb-schema`
- `crates/test-support/sql-generator`
- `crates/test-support/sqlite-reference`

### `qualification/Cargo.toml` (15 declared member entries)

- `build`
- `codec-query`
- `model-schema-only`
- `model-typed-adapter`
- `schema/audit/default_empty`
- `schema/audit/group_path`
- `schema/audit/one_simple`
- `schema/audit/sql_perf`
- `schema/audit/ten_simple`
- `schema/demo/rpg`
- `schema/tests/fixtures`
- `schema/tests/sql`
- `source-checks`
- `startup`
- `wasm-helpers`

### `testing/canic-adapter/Cargo.toml` (31 declared member entries)

- `.`
- `../../canisters/audit/codec_wire_query`
- `../../canisters/audit/default_empty`
- `../../canisters/audit/default_empty_metrics`
- `../../canisters/audit/group_path_sql_query`
- `../../canisters/audit/nested_relation_direct`
- `../../canisters/audit/nested_relation_none`
- `../../canisters/audit/nested_relation_repeated`
- `../../canisters/audit/nested_relation_shallow`
- `../../canisters/audit/one_entity_dynamic_query`
- `../../canisters/audit/one_entity_reachable_operations`
- `../../canisters/audit/one_entity_sql_query`
- `../../canisters/audit/one_entity_typed_query`
- `../../canisters/audit/request_future_scale`
- `../../canisters/audit/sql_perf`
- `../../canisters/audit/ten_entity_reachable_operations`
- `../../canisters/audit/ten_entity_typed_query`
- `../../canisters/demo/rpg`
- `../../canisters/tests/lifecycle_participant`
- `../../canisters/tests/read_authority`
- `../../canisters/tests/schema_guard`
- `../../canisters/tests/schema_public`
- `../../canisters/tests/sql`
- `../../canisters/tests/sql_bounded`
- `../../canisters/tests/sql_guard`
- `../../canisters/tests/wire_rollback`
- `../../schema/audit/nested_relation`
- `../../testing/endpoint-installation`
- `../../testing/integration`
- `../../testing/logical-memory`
- `../../testing/model-facade-only`

## Coverage limits

- Inspected current owners and selected callers for query normalization/cache identity, access lowering/traversal, structural mutation coordination, schema application/lowering, accepted schema projections, overlay metadata, diagnostic projection and generated typed adapters.
- Inspected the one/ten-entity reachable-operation fixture pair and existing Wasm measurement policy.
- Inventoried all Rust source families above. This is not a line-by-line review of all files.
- Recovery, relation indexes, journal codecs, aggregate/group execution, CLI, source generators, all schema transition variants and all qualification packages need deeper semantic review. Hotspot entries distinguish this work from verified findings.
- No external consumer API search, macro-expansion inventory, dependency-liveness proof, compilation, test execution, Wasm build or IC measurement was performed in this audit.
- Canic runtime qualification remains deferred under [#373](https://github.com/dragginzgame/icydb/issues/373). Existing local #634 isolation is part of the baseline, not a new saving.

The findings are an evidence report, not a parallel issue queue. Any accepted follow-up needing tracking belongs in GitHub issues.

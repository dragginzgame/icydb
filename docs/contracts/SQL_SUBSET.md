# IcyDB SQL Subset Contract

This document defines the current supported public IcyDB SQL boundary.
Anything not stated here is outside the supported SQL surface and must fail
closed.

This contract defines the current public SQL frontend.

All state-changing SQL remains subject to
`docs/contracts/WRITE_ADMISSION.md`. SQL exposure policy and trusted execution
never disable accepted-schema row validation.

The `icydb-sql-feature` comments are stable evidence identifiers. Their current
coverage obligations live in the
[SQL coverage manifest](../../testing/integration/tests/sql_correctness_support/coverage_manifest.rs);
that manifest indexes this contract and does not define or widen SQL behavior.

## Scope

- Applies to IcyDB SQL parsing, lowering, validation, and execution semantics.
- Applies only to single-entity statements.
- Defines the admitted public SQL shapes, not internal parser route metadata.
- Does not define storage internals, planner heuristics, or canister ABI shape.

## Core Rule

<!-- icydb-sql-feature id="surface.single_entity" kind="semantic" status="accepted" -->
Every admitted executable SQL statement targets exactly one entity.

IcyDB SQL is a constrained single-entity language for:

- filtering
- ordering
- pagination
- scalar projection
- grouped queries and aggregates
- narrow built-in expression forms
- explain and schema/introspection commands

IcyDB SQL is not a general-purpose relational SQL engine.
It is also not Postgres-style transaction SQL: it does not provide implicit
transaction blocks, rollback-on-returned-error semantics, isolation levels, or
cross-entity/cross-canister transaction coordination.

Typed and fluent APIs are the canonical public surfaces.
The remaining public SQL surfaces are:

<!-- icydb-sql-feature id="surface.trusted_entrypoints" kind="policy" status="accepted" -->
- `execute_trusted_sql_query::<E>(...)`
- `execute_trusted_sql_mutation::<E>(...)`
- `execute_admin_sql_ddl::<E>(...)`

Both stay hard-bound to one concrete entity type and return SQL-shaped output.

Read-admission lanes, generated endpoint lane ownership, and the current
read-surface inventory are documented in `docs/contracts/READ_ADMISSION.md`.
In particular, generated `icydb_query` is controller-gated admin SQL, not a
generated `PublicRead` endpoint. IcyDB does not generate non-controller public
SQL read endpoints; caller-facing reads should use typed/fluent APIs so the
default bounded read-admission gate applies.

## Cursor Pagination

<!-- icydb-sql-feature id="pagination.scalar_cursor" kind="syntax" status="rejected" -->
Cursor-based pagination is not part of the scalar SQL surface.

<!-- icydb-sql-feature id="pagination.scalar_limit_offset" kind="semantic" status="accepted" -->
- SQL uses `LIMIT` / `OFFSET` for scalar windowing.
- Cursor pagination is available through typed and fluent APIs.
- This is intentional: cursor semantics are transport-level, not query
  semantics.

<!-- icydb-sql-feature id="pagination.grouped_cursor" kind="semantic" status="accepted" -->
Grouped SQL is the explicit exception.
Grouped SQL result payloads may carry `next_cursor` because grouped execution
already returns structured continuation-aware results as part of its admitted
surface.

## Operational vs Semantic Features

The SQL surface defines query semantics only.

SQL covers:

- filtering
- ordering
- projection
- grouping
- aggregation
- mutation

The following are intentionally not part of SQL:

<!-- icydb-sql-feature id="operational.transport_controls" kind="syntax" status="rejected" -->
- cursor-based pagination
- continuation tokens
- streaming controls
<!-- icydb-sql-feature id="operational.byte_metrics" kind="syntax" status="rejected" -->
- byte-metric diagnostics such as `bytes()` and `bytes_by(...)`

These are available only through typed and fluent APIs.

SQL guarantees semantic equivalence for admitted query and mutation shapes, but
not transport-level or diagnostic behavior.

Returned `Err` values are ordinary canister responses. If application code
performs one successful mutation and later returns `Err`, the earlier mutation
is not rolled back by IcyDB or by IC message semantics. IcyDB's atomicity
contracts apply only to the specific IcyDB mutation operation or explicit
atomic batch helper being executed.

## Supported Public SQL Statements

### `SELECT`

Supported `SELECT` families are:

<!-- icydb-sql-feature id="select.scalar_rows" kind="syntax" status="accepted" -->
- scalar row loads
<!-- icydb-sql-feature id="select.scalar_distinct" kind="syntax" status="accepted" -->
- scalar `DISTINCT` loads
<!-- icydb-sql-feature id="select.global_aggregate" kind="syntax" status="accepted" -->
- global aggregate loads with one or more aggregate projection terminals and no
  `GROUP BY`
<!-- icydb-sql-feature id="select.grouped_aggregate" kind="syntax" status="accepted" -->
- grouped aggregate loads
<!-- icydb-sql-feature id="select.aggregate_distinct_filter" kind="semantic" status="accepted" -->
- aggregate `DISTINCT` terminals and aggregate `FILTER (WHERE ...)` modifiers
<!-- icydb-sql-feature id="select.computed_projection" kind="semantic" status="accepted" -->
- narrow computed projection loads, including admitted bounded arithmetic,
  numeric scalar functions, text-function projection forms, and searched `CASE`

<!-- icydb-sql-feature id="select.scalar_composition" kind="interaction" status="accepted" -->
- scalar `WHERE`, computed projection, projection-alias ordering, `LIMIT`, and
  `OFFSET` clauses compose within their independently admitted bounds

<!-- icydb-sql-feature id="select.grouped_composition" kind="interaction" status="accepted" -->
- grouped `WHERE`, aggregate projection, `GROUP BY`, `HAVING`, aggregate-alias
  ordering, and `LIMIT` clauses compose within their independently admitted bounds

<!-- icydb-sql-feature id="ordering.null_values" kind="semantic" status="accepted" -->
`ORDER BY` uses IcyDB's canonical value comparator. For nullable values, `ASC`
places `NULL` before present values and `DESC` reverses that comparator, so
`NULL` sorts after present values. Later `ORDER BY` terms remain tie-breakers
inside equal nullable groups.

#### Exact Primary-Key Reads

<!-- icydb-sql-feature id="select.exact_primary_key" kind="interaction" status="accepted" -->
Strict scalar primary-key equality in SQL is an exact-key read when the accepted
runtime schema proves the field is the entity's scalar primary key and the
literal value has the exact primary-key type.

Supported exact-key SQL forms include:

- `WHERE pk = literal`;
- commuted literal equality, `WHERE literal = pk`;
- finite literal primary-key `IN (...)` lists within public read-admission
  policy.

These forms may be admitted by the public read gate without fake `LIMIT`
ceremony because the planner can select `ByKey`, `ByKeys`, or `Empty` access.
Invalid exact-key-looking shapes fail closed instead of falling back to a scan.
That includes wrong literal types, malformed `IN` lists, over-budget key-list
inputs, and invalid residual predicates.

<!-- icydb-sql-feature id="select.placeholder_parameters" kind="syntax" status="rejected" -->
SQL placeholder parameters are not part of the current public SQL subset. A
shape such as `WHERE pk = ?` is rejected before primary-key canonicalization.
If SQL parameters are added later, parameter binding must preserve the same
accepted-schema key encoding, cache-safety, and fail-closed contracts.

### `EXPLAIN`

<!-- icydb-sql-feature id="explain.query_delete" kind="syntax" status="accepted" -->
Supported shapes:

- `EXPLAIN SELECT ...`
- `EXPLAIN DELETE ...`
- `EXPLAIN EXECUTION SELECT ...`
- `EXPLAIN EXECUTION DELETE ...`
- `EXPLAIN JSON SELECT ...`
- `EXPLAIN JSON DELETE ...`

`EXPLAIN` is an operational SQL surface.

### Introspection

Supported commands:

<!-- icydb-sql-feature id="introspection.describe" kind="syntax" status="accepted" -->
- `DESCRIBE entity`
<!-- icydb-sql-feature id="introspection.show_indexes" kind="syntax" status="accepted" -->
- `SHOW INDEXES FROM entity`
- `SHOW INDEXES IN entity`
<!-- icydb-sql-feature id="introspection.show_columns" kind="syntax" status="accepted" -->
- `SHOW COLUMNS entity`
<!-- icydb-sql-feature id="introspection.show_entities" kind="syntax" status="accepted" -->
- `SHOW ENTITIES` / `SHOW ENTITIES VERBOSE`
<!-- icydb-sql-feature id="introspection.show_entity" kind="syntax" status="accepted" -->
- `SHOW ENTITY entity` / `SHOW ENTITY entity VERBOSE`
<!-- icydb-sql-feature id="introspection.show_stores" kind="syntax" status="accepted" -->
- `SHOW STORES` / `SHOW STORES VERBOSE`
<!-- icydb-sql-feature id="introspection.show_memory" kind="syntax" status="accepted" -->
- `SHOW MEMORY`

<!-- icydb-sql-feature id="introspection.catalog_projection" kind="semantic" status="accepted" -->
`SHOW ENTITIES` lists registered runtime entities with owning store, storage
mode, compact schema counts, and `sv` schema version. `SHOW ENTITY` returns
the same row shape filtered to one entity name, using exact matching first and
case-insensitive matching as a fallback. `SHOW STORES` lists registered stores
and their storage modes. `SHOW MEMORY` lists stable-memory tags, memory IDs,
and their owning stores. The default shell rendering stays compact; `VERBOSE`
exposes full entity and store paths for debugging.

<!-- icydb-sql-feature id="introspection.generated_policy" kind="policy" status="accepted" -->
Generated `icydb_query` gates `EXPLAIN`, `DESCRIBE`, and `SHOW` by
`[canisters.<name>.sql.introspection]`. The default policy is `local = true`
and `ic = false`; direct builds with an unknown target fail closed.

<!-- icydb-sql-feature id="introspection.storage_modes" kind="semantic" status="accepted" -->
Storage modes have distinct contracts. `heap` is volatile live storage with
absent stable allocation identity and no row/index recovery. `journaled` is
the durable cached-stable store: live reads use Rust BTree projections,
committed journal records are folded into canonical stable data/index/schema
BTrees, and `SHOW MEMORY` reports the fourth journal-tail memory role
separately from the three canonical stable roles. Direct `stable` storage is
not a supported current storage mode. The full operator-facing storage
durability boundary is documented in `docs/contracts/DURABILITY.md`.

<!-- icydb-sql-feature id="introspection.index_origin" kind="semantic" status="accepted" -->
`SHOW INDEXES` includes index lifecycle and origin annotations. Generated
entity-model indexes report `origin=generated`; indexes added through SQL DDL
report `origin=ddl`. Only DDL-origin field-path indexes are droppable through
SQL DDL.

`DESCRIBE` includes the same generated-vs-DDL index origin metadata in its
structured index payload and shell rendering, so schema tooling can distinguish
model-owned indexes from DDL-created indexes without scraping `SHOW INDEXES`.

<!-- icydb-sql-feature id="introspection.unsupported_modifiers" kind="syntax" status="rejected" -->
Introspection modifiers not listed above are outside the current subset. In
particular, filtering clauses and extra entity operands fail closed instead of
being ignored or interpreted as alternate catalog commands.

### DDL

Supported shapes:

<!-- icydb-sql-feature id="ddl.create_index_field_path" kind="syntax" status="accepted" -->
- `CREATE INDEX name ON entity (field_path)`
<!-- icydb-sql-feature id="ddl.create_index_multi_field" kind="syntax" status="accepted" -->
- `CREATE INDEX name ON entity (field_path, another_field_path)`
<!-- icydb-sql-feature id="ddl.index_ascending" kind="semantic" status="accepted" -->
- `CREATE INDEX name ON entity (field_path ASC)`
<!-- icydb-sql-feature id="ddl.create_index_filtered" kind="syntax" status="accepted" -->
- `CREATE INDEX name ON entity (field_path) WHERE predicate`
<!-- icydb-sql-feature id="ddl.create_index_expression" kind="syntax" status="accepted" -->
- `CREATE INDEX name ON entity (LOWER(field_path))`
- `CREATE INDEX name ON entity (UPPER(field_path))`
- `CREATE INDEX name ON entity (TRIM(field_path))`
<!-- icydb-sql-feature id="ddl.create_index_if_not_exists" kind="semantic" status="accepted" -->
- `CREATE INDEX IF NOT EXISTS name ON entity (field_path)`
- `CREATE INDEX IF NOT EXISTS name ON entity (LOWER(field_path))`
<!-- icydb-sql-feature id="ddl.create_unique_index" kind="syntax" status="accepted" -->
- `CREATE UNIQUE INDEX name ON entity (field_path)`
- `CREATE UNIQUE INDEX name ON entity (LOWER(field_path))`
<!-- icydb-sql-feature id="ddl.drop_index" kind="syntax" status="accepted" -->
- `DROP INDEX name ON entity`
- `DROP INDEX name`
<!-- icydb-sql-feature id="ddl.drop_index_if_exists" kind="semantic" status="accepted" -->
- `DROP INDEX IF EXISTS name ON entity`
- `DROP INDEX IF EXISTS name`
<!-- icydb-sql-feature id="ddl.alter_add_column" kind="syntax" status="accepted" -->
- `ALTER TABLE entity ADD COLUMN field type`
- `ALTER TABLE entity ADD COLUMN field type DEFAULT value`
<!-- icydb-sql-feature id="ddl.alter_column_default" kind="syntax" status="accepted" -->
- `ALTER TABLE entity ALTER COLUMN field SET DEFAULT value`
- `ALTER TABLE entity ALTER COLUMN field DROP DEFAULT`
<!-- icydb-sql-feature id="ddl.alter_column_nullability" kind="syntax" status="accepted" -->
- `ALTER TABLE entity ALTER COLUMN field SET NOT NULL`
- `ALTER TABLE entity ALTER COLUMN field DROP NOT NULL`
<!-- icydb-sql-feature id="ddl.rename_column" kind="syntax" status="accepted" -->
- `ALTER TABLE entity RENAME COLUMN old_name TO new_name`
<!-- icydb-sql-feature id="ddl.drop_column" kind="syntax" status="accepted" -->
- `ALTER TABLE entity DROP COLUMN field`
- `ALTER TABLE entity DROP COLUMN IF EXISTS field`

SQL DDL is a frontend over accepted schema catalog mutation, not the source of
schema authority. Schema mutation and row-rewrite admission remain governed by
`docs/contracts/WRITE_ADMISSION.md`.

`CREATE INDEX` currently admits field-path secondary indexes and deterministic
text expression secondary indexes. Single-field, multi-field, unique, explicit
`ASC`, filtered `WHERE` predicates, and `LOWER`/`UPPER`/`TRIM` expression keys
are supported. Every field path must already exist in the accepted schema, must
be indexable, and must not duplicate an accepted index name or identical
accepted index contract.
`CREATE INDEX IF NOT EXISTS` no-ops only when the accepted catalog already has
the exact requested index contract. Conflicting existing definitions still
reject.
<!-- icydb-sql-feature id="ddl.index_descending" kind="syntax" status="rejected" -->
`ASC` is accepted as IcyDB's default deterministic physical key order. `DESC`
is not yet supported for SQL DDL indexes and fails with explicit
unsupported-feature diagnostics.

<!-- icydb-sql-feature id="ddl.generated_owned_objects" kind="policy" status="rejected" -->
`DROP INDEX` currently admits secondary indexes that were created through SQL
DDL. Generated/model-declared indexes are owned by the entity schema macro and
must be removed there, then reconciled through the normal accepted-schema
publication path.
`DROP INDEX IF EXISTS` no-ops only when the target index is absent. Existing
generated/model-owned and otherwise unsupported indexes still reject.
Typed SQL DDL may omit `ON entity` for `DROP INDEX`. Generated canister DDL
requires `ON entity` so dispatch does not guess a target from canister shape.

`ALTER TABLE ... ADD COLUMN ...` publishes DDL-owned accepted fields for
supported scalar column types. Nullable no-default additions materialize older
rows as `NULL`; supported SQL defaults are encoded into accepted schema
metadata and can make a new field required.

`ALTER TABLE ... ALTER COLUMN ... SET/DROP DEFAULT` and `SET/DROP NOT NULL`
publish metadata changes for DDL-owned fields only. `SET NOT NULL` scans
existing rows through the accepted schema and rejects if any row materializes
`NULL`. Generated/model-owned fields remain Rust-schema owned.

`ALTER TABLE ... RENAME COLUMN ... TO ...` publishes metadata-only accepted
schema changes for DDL-owned fields. Field ID, row slot, default/nullability,
decode contracts, and direct field-path index identity remain stable; accepted
field names, direct field-path index labels, and expression-index
source/canonical labels are updated together. Filtered-index predicate SQL
labels relabel through the reduced predicate AST. Generated fields reject
before publication.

`ALTER TABLE ... DROP COLUMN ...` rewrites rows and publishes a dense accepted
schema for DDL-owned fields. Active metadata removes the field, surviving field
IDs and physical slots are renumbered to `1..N` and `0..N-1`, and every stored
row is rewritten to that current layout before publication. A later
`ADD COLUMN` allocates the next dense identity. Primary-key, generated, and
index-dependent fields reject before publication.
`DROP COLUMN IF EXISTS` reports `no_op` only when the target field is absent.

<!-- icydb-sql-feature id="ddl.destructive_publication_atomicity" kind="interaction" status="accepted" -->
Destructive DDL keeps physical state and accepted-schema publication atomic at
the guarded operation boundary. Rejection before a durable commit marker owns
the candidate leaves the accepted-before schema and physical state
authoritative. Once a marker owns the candidate, guarded reentry completes the
accepted-after schema and its required physical state before serving another
operation.

## Public SQL Mutation Execution

Supported public mutation shapes are:

<!-- icydb-sql-feature id="mutation.insert" kind="syntax" status="accepted" -->
- `INSERT`
<!-- icydb-sql-feature id="mutation.update" kind="syntax" status="accepted" -->
- `UPDATE`
<!-- icydb-sql-feature id="mutation.delete" kind="syntax" status="accepted" -->
- `DELETE`
<!-- icydb-sql-feature id="mutation.returning" kind="syntax" status="accepted" -->
- admitted narrow `... RETURNING`

Mutation ownership still primarily lives on typed and fluent APIs:

- `create(...)`
- `insert(...)`
- `update(...)`
- `replace(...)`
- `delete::<E>()`
- the corresponding typed/fluent `...returning...` helpers

Every SQL row after-image is decoded against accepted field contracts and then
enters the same structural write-admission pipeline used by non-SQL structural
mutation. `trusted` in a SQL API name describes caller-owned authorization and
surface policy; it is not a schema-validation bypass.

Public SQL ownership is split deliberately:

<!-- icydb-sql-feature id="mutation.lane_ownership" kind="policy" status="accepted" -->
- `execute_trusted_sql_query::<E>(...)` owns read, explain, and introspection SQL
- `execute_trusted_sql_mutation::<E>(...)` owns state-changing SQL
- `execute_admin_sql_ddl::<E>(...)` owns accepted-catalog schema DDL SQL

### SQL `UPDATE` Availability By Surface

`UPDATE` is an existing session/library write-lane capability. Generated query
and DDL endpoints still reject it; generated canister update exposure is a
separate opt-in write endpoint with an explicit public-safe policy.

Current boundary:

<!-- icydb-sql-feature id="mutation.trusted_update" kind="policy" status="accepted" -->
- `execute_trusted_sql_mutation::<E>(...)` admits supported single-entity `UPDATE`
  statements.
- `execute_trusted_sql_mutation::<E>(...)` admits current narrow
  `UPDATE ... RETURNING` forms.
<!-- icydb-sql-feature id="mutation.generated_query_ddl" kind="policy" status="rejected" -->
- generated `icydb_query` rejects row mutation SQL, including `UPDATE`.
- generated `icydb_ddl` rejects row mutation SQL, including `UPDATE`.
<!-- icydb-sql-feature id="mutation.generated_update_disabled" kind="policy" status="accepted" -->
- generated `icydb_update` is not part of the default generated canister
  surface; it is emitted only when the canister config selects an update policy.
<!-- icydb-sql-feature id="mutation.generated_update_primary_key" kind="policy" status="accepted" -->
- `update = true` and `update = "primary_key"` select the public
  primary-key-only policy.
<!-- icydb-sql-feature id="mutation.generated_update_bounded" kind="policy" status="accepted" -->
- `update = "bounded"` selects the public bounded deterministic policy, which
  requires explicit primary-key ordering and a limit.

<!-- icydb-sql-feature id="mutation.trusted_update_window" kind="interaction" status="accepted" -->
Current `execute_trusted_sql_mutation::<E>(...)` support includes primary-key and
non-primary-key predicates, explicit `ORDER BY`, `LIMIT`, and `OFFSET` where
the reduced SQL write lane admits them. That broader session/library behavior
does not define the policy for generated public SQL write endpoints. Generated
`icydb_update` dispatch must choose one configured `UPDATE` policy before
executing row mutation SQL, and must not call the broad session/library
`execute_trusted_sql_mutation::<E>(...)` lane directly.

## Blob Literals and Blob Values

<!-- icydb-sql-feature id="blob.hex_literal" kind="syntax" status="accepted" -->
SQL accepts hex blob literals in the `X'...'` / `x'...'` form. The hex body
must contain only hexadecimal digits and must have an even number of digits.
<!-- icydb-sql-feature id="blob.literal_size_limit" kind="policy" status="accepted" -->
The decoded payload is capped at 1,048,576 bytes per literal so oversized SQL
text fails before allocating unbounded blob buffers.

<!-- icydb-sql-feature id="blob.read_write_compare" kind="semantic" status="accepted" -->
Supported blob behavior:

- `INSERT` / `UPDATE` can write blob fields from hex blob literals.
- `SELECT` and `DELETE ... RETURNING` can return blob fields.
- `WHERE blob_field = X'...'` and `WHERE blob_field <> X'...'` compare blob
  bytes exactly.
- `OCTET_LENGTH(blob_field)` returns the blob byte length without changing
  `LENGTH(text)` character-count behavior.

Unsupported blob behavior:

<!-- icydb-sql-feature id="blob.ordering" kind="semantic" status="rejected" -->
- `ORDER BY blob_field` is rejected because raw blob values are not orderable
  through the public SQL surface.
- SQL does not provide streaming blob reads or chunked blob writes; large
  payload transport remains better suited to typed APIs that can expose
  chunk-oriented application boundaries.

## Entity Naming And Aliases

<!-- icydb-sql-feature id="naming.single_binding" kind="syntax" status="accepted" -->
The admitted single-entity naming surface is:

- unqualified entity names
- schema-qualified entity names such as `public.Customer`
- one optional single-table alias, with or without `AS`

Examples:

- `SELECT * FROM Customer c`
- `SELECT c.name FROM Customer AS c`
- `DELETE FROM Customer c WHERE c.age < 20`
- `UPDATE Customer AS c SET age = 22 WHERE c.name = 'Ada'`
- `INSERT INTO Customer c (name, age) VALUES ('Ada', 22)`

No statement may introduce more than one entity binding.

## Projection

Supported scalar projection forms are:

<!-- icydb-sql-feature id="projection.scalar" kind="semantic" status="accepted" -->
- `SELECT *`
- `SELECT field, ...`
- `SELECT DISTINCT *`
- `SELECT DISTINCT field, ...`
- admitted bounded scalar expression projections, including searched `CASE`

Supported aggregate projection forms are:

<!-- icydb-sql-feature id="projection.aggregate" kind="semantic" status="accepted" -->
- one or more aggregate terminals with no `GROUP BY`
- aggregate terminals with admitted input expressions
- aggregate terminals with `DISTINCT`
- aggregate terminals with `FILTER (WHERE predicate)`
- grouped projection where grouped key items come first and aggregate or
  post-aggregate computed items come after them

Supported grouped projection examples:

<!-- icydb-sql-feature id="projection.grouped_layout" kind="interaction" status="accepted" -->
- `SELECT age, COUNT(*) FROM Customer GROUP BY age`
- `SELECT name, COUNT(*), SUM(age) FROM Customer GROUP BY name`
- `SELECT TRIM(name), COUNT(*) FROM Customer GROUP BY name`
- `SELECT age, ROUND(AVG(age), 2) FROM Customer GROUP BY age`
- `SELECT age, AVG(age + 1) + AVG(age + 1) FROM Customer GROUP BY age`
- `SELECT age, CASE WHEN COUNT(*) > 1 THEN 'multi' ELSE 'single' END FROM Customer GROUP BY age`
- `SELECT age, CASE WHEN COUNT(*) > 1 THEN TRUE ELSE FALSE END FROM Customer GROUP BY age`

Unsupported grouped projection examples:

<!-- icydb-sql-feature id="projection.invalid_grouped_layout" kind="semantic" status="rejected" -->
- grouped aggregates without grouped keys in the projection
- grouped keys appearing after aggregate outputs
- grouped projection terms that reference non-group fields outside the admitted
  grouped key and aggregate output authority
- bounded text functions inside grouped projection when they reference raw
  non-group fields instead of grouped key values

## Shared SQL Expression Family

The admitted SQL expression family is shared across projection, aggregate
inputs, grouped/global `HAVING`, and `WHERE`.

Supported numeric scalar functions include:

<!-- icydb-sql-feature id="expression.numeric_functions" kind="semantic" status="accepted" -->
- unary numeric functions: `ABS`, `CBRT`, `CEIL` / `CEILING`, `EXP`, `FLOOR`,
  `LN`, `LOG2`, `LOG10`, `SIGN`, `SQRT`
- binary numeric functions: `LOG(base, x)`, `MOD(x, y)`, `POWER(x, y)` /
  `POW(x, y)`
- scale-taking numeric functions: `ROUND(x, scale)`, `TRUNC(x, scale)` /
  `TRUNCATE(x, scale)`

<!-- icydb-sql-feature id="expression.text_functions" kind="semantic" status="accepted" -->
Supported text scalar functions include `LOWER`, `UPPER`, `LENGTH`,
`OCTET_LENGTH`, `TRIM`, `LTRIM`, `RTRIM`, `LEFT`, `RIGHT`, `STARTS_WITH`,
`ENDS_WITH`, `CONTAINS`, `POSITION`, `REPLACE`, and `SUBSTRING`.

<!-- icydb-sql-feature id="expression.value_selection" kind="semantic" status="accepted" -->
`COALESCE` and `NULLIF` provide the admitted value-selection forms. Function
call shapes, argument types, and direct ordering eligibility remain bounded by
the clause-specific lowering contract; admission in the shared expression
family does not make every expression a valid `ORDER BY` target.

The current conditional form is intentionally narrow:

<!-- icydb-sql-feature id="expression.searched_case" kind="semantic" status="accepted" -->
- searched `CASE WHEN ... THEN ... [ELSE ...] END`

Supported searched `CASE` contexts are:

- scalar `SELECT` projections
- aggregate input expressions such as `SUM(CASE WHEN ... THEN ... ELSE ... END)`
- grouped/global aggregate `HAVING`
- `WHERE`, when the selected branch collapses onto the admitted boolean filter
  surface

Within those contexts, searched `CASE` conditions admit the same bounded
boolean/comparison expression lane used by that clause, including the admitted
postfix predicate family such as:

- `IS NULL` / `IS NOT NULL`
- `IS TRUE` / `IS FALSE` / `IS NOT TRUE` / `IS NOT FALSE`
- `LIKE` / `NOT LIKE` / `ILIKE` / `NOT ILIKE`
- `IN (...)`
- `BETWEEN ... AND ...` / `NOT BETWEEN ... AND ...`

Still intentionally excluded:

<!-- icydb-sql-feature id="expression.simple_case" kind="syntax" status="rejected" -->
- simple `CASE value WHEN ...`
- subqueries or window expressions inside `CASE`
- `CASE` as a loophole for unsupported expression families in that clause

## Projection Aliases

<!-- icydb-sql-feature id="projection.aliases" kind="semantic" status="accepted" -->
Projection aliases are supported in `SELECT` lists.

Both forms are admitted:

- `SELECT name AS display_name FROM Customer`
- `SELECT COUNT(*) total FROM Customer GROUP BY name`

Aliases may label:

- scalar field projections
- aggregate projections
- admitted scalar computed projections
- admitted grouped post-aggregate computed projections

`ORDER BY <alias>` is supported only when the alias resolves to an already
supported order target:

<!-- icydb-sql-feature id="ordering.projection_alias" kind="interaction" status="accepted" -->
- a plain field
- `LOWER(field)`
- `UPPER(field)`
- admitted bounded scalar computed order targets such as field-plus-literal,
  field-plus-field, and `ROUND(...)`
- admitted grouped aggregate order targets, including bounded grouped Top-K
  alias forms such as `ORDER BY avg_age DESC`

Aliases do not widen the order-expression surface.

## Predicates

Supported `WHERE` predicate forms are:

<!-- icydb-sql-feature id="predicate.boolean_comparison" kind="semantic" status="accepted" -->
- `AND`, `OR`, `NOT`
- parenthesized predicate trees
- comparison operators
  - `=`
  - `!=`
  - `<`
  - `<=`
  - `>`
  - `>=`
  - `<>`
<!-- icydb-sql-feature id="predicate.field_comparison" kind="semantic" status="accepted" -->
- field-to-field comparisons on the same comparison family
<!-- icydb-sql-feature id="predicate.grouped_where_field_comparison" kind="interaction" status="accepted" -->
- field-to-field comparisons in a grouped query's pre-aggregation `WHERE` lane
<!-- icydb-sql-feature id="predicate.membership" kind="semantic" status="accepted" -->
- `IN (...)`
- `NOT IN (...)`
<!-- icydb-sql-feature id="predicate.range" kind="semantic" status="accepted" -->
- `BETWEEN ... AND ...`
- `NOT BETWEEN ... AND ...`
<!-- icydb-sql-feature id="predicate.null" kind="semantic" status="accepted" -->
- `IS NULL`
- `IS NOT NULL`
<!-- icydb-sql-feature id="predicate.boolean_truth" kind="semantic" status="accepted" -->
- `IS TRUE`
- `IS FALSE`
- `IS NOT TRUE`
- `IS NOT FALSE`
<!-- icydb-sql-feature id="predicate.prefix_pattern" kind="semantic" status="accepted" -->
- prefix `LIKE 'prefix%'`
- prefix `NOT LIKE 'prefix%'`
- prefix `ILIKE 'prefix%'`
- prefix `NOT ILIKE 'prefix%'`
<!-- icydb-sql-feature id="predicate.starts_with" kind="semantic" status="accepted" -->
- `STARTS_WITH(field, 'prefix')`
- searched `CASE`, when it returns values that stay on the admitted boolean
  filtering lane

Narrow casefolded predicate forms are also supported:

<!-- icydb-sql-feature id="predicate.casefold_prefix" kind="semantic" status="accepted" -->
- `LOWER(field) LIKE 'prefix%'`
- `UPPER(field) LIKE 'PREFIX%'`
- `STARTS_WITH(LOWER(field), 'prefix')`
- `STARTS_WITH(UPPER(field), 'PREFIX')`

Field-bound range predicates are also supported on the plain-field lane:

<!-- icydb-sql-feature id="predicate.field_bound_range" kind="semantic" status="accepted" -->
- `field BETWEEN lower_field AND upper_field`
- `field NOT BETWEEN lower_field AND upper_field`

<!-- icydb-sql-feature id="predicate.expression_arguments" kind="interaction" status="accepted" -->
The residual-expression lane also admits supported scalar expressions as text
predicate arguments, such as
`STARTS_WITH(REPLACE(name, 'a', 'A'), TRIM('Al'))`. These shapes do not claim
index-predicate extraction when their expression form is not indexable.

Still intentionally excluded from the admitted predicate lane:

<!-- icydb-sql-feature id="predicate.non_prefix_pattern" kind="semantic" status="rejected" -->
- non-prefix `LIKE` / `NOT LIKE` / `ILIKE` / `NOT ILIKE`
- grouped `HAVING` variants that reuse the plain-field boolean special forms
  or text-pattern lane directly

## `HAVING`

Supported `HAVING` forms are:

<!-- icydb-sql-feature id="having.grouped_aggregate" kind="semantic" status="accepted" -->
- grouped aggregate `HAVING` over grouped keys and aggregate outputs
<!-- icydb-sql-feature id="having.global_aggregate" kind="semantic" status="accepted" -->
- global aggregate `HAVING` over the implicit single aggregate group
- admitted post-aggregate scalar expressions, including bounded arithmetic,
  wrappers, and searched `CASE`

Still intentionally excluded:

<!-- icydb-sql-feature id="having.raw_row_escape" kind="semantic" status="rejected" -->
- raw-row-only expressions that escape post-aggregate authority
- grouped `HAVING` reuse of the plain-field text-pattern or boolean-special
  predicate lane

## Public SQL Write `RETURNING`

Supported `RETURNING` forms are intentionally narrow:

<!-- icydb-sql-feature id="returning.star" kind="semantic" status="accepted" -->
- `RETURNING *`
<!-- icydb-sql-feature id="returning.fields" kind="semantic" status="accepted" -->
- `RETURNING field, ...`

`RETURNING` is admitted on the public SQL write lane for:

- `INSERT ... RETURNING`
- `UPDATE ... RETURNING`
- `DELETE ... RETURNING`

For `UPDATE`, `RETURNING` is available where the underlying SQL write surface
admits the `UPDATE` shape. Generated query and DDL endpoints still reject
`UPDATE` before `RETURNING` semantics apply; generated `icydb_update` admits
`UPDATE RETURNING` only where the configured update policy admits the target
selection shape.

Unsupported `RETURNING` projection forms remain fail-closed:

<!-- icydb-sql-feature id="returning.computed" kind="semantic" status="rejected" -->
- computed expressions in `RETURNING`
- aggregate expressions in `RETURNING`
- other widened projection families beyond `*` or plain field lists

## Explicitly Rejected SQL Families

The finite boundaries below are maintained rejection contracts. They do not
claim to enumerate every SQL input outside IcyDB's constrained subset.

<!-- icydb-sql-feature id="query.multi_entity" kind="syntax" status="rejected" -->
- multi-entity statements, additional `FROM` bindings, and joins
<!-- icydb-sql-feature id="query.subquery_cte" kind="syntax" status="rejected" -->
- subqueries and common table expressions
<!-- icydb-sql-feature id="query.set_operations" kind="syntax" status="rejected" -->
- `UNION`, `INTERSECT`, and `EXCEPT`
<!-- icydb-sql-feature id="query.window_functions" kind="syntax" status="rejected" -->
- window functions and `OVER (...)`
<!-- icydb-sql-feature id="query.transactions" kind="syntax" status="rejected" -->
- transaction-control statements such as `BEGIN`, `COMMIT`, and `ROLLBACK`
<!-- icydb-sql-feature id="expression.cast" kind="syntax" status="rejected" -->
- `CAST(...)` expressions

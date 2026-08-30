# IcyDB SQL Subset Contract

This document defines the current supported public IcyDB SQL boundary.
Anything not stated here is outside the supported SQL surface and must fail
closed.

This contract defines the current public SQL frontend.

All state-changing SQL remains subject to
`docs/contracts/WRITE_ADMISSION.md`. SQL exposure policy and trusted execution
never disable accepted-schema row validation.

Machine-readable coverage obligations live in the
[SQL coverage manifest](../../testing/integration/tests/sql_correctness_support/coverage_manifest.rs).
The manifest is the code-owned testing authority; this contract remains the
human-readable definition of supported behavior.

## Scope

- Applies to IcyDB SQL parsing, lowering, validation, and execution semantics.
- Applies only to single-entity statements.
- Defines the admitted public SQL shapes, not internal parser route metadata.
- Does not define storage internals, planner heuristics, or canister ABI shape.

## Core Rule

Every admitted query, mutation, DDL, Quick integrity, or Deep-start statement
targets exactly one entity. Deep continuation and abort are the explicit
exception: their authorized job identity already owns the exact accepted
entity, so those forms reject an additional caller-authored entity.

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

Typed and dynamic APIs are the canonical caller-facing read surfaces.
The remaining public SQL surfaces are:

- `execute_trusted_sql_query(...)`
- `execute_trusted_sql_mutation(...)`
- `execute_trusted_sql_exact_update(..., require_affected_at_most)`
- `execute_trusted_sql_prefix_update(...)`
- `start_trusted_sql_mutation_job(job_id, ...)`
- `mutation_job_state(job_id)`
- `advance_trusted_mutation_job(...)`
- `cancel_unadvanced_mutation_job(job_id, expected_sequence)`
- `acknowledge_mutation_job(job_id, expected_terminal_sequence)`
- `progress_job_inventory()`
- `execute_admin_sql_ddl(...)`
- `execute_admin_integrity_sql(...)`

Query and row-mutation entry points resolve the entity named by the statement
directly through accepted catalog authority; they do not require a generated
Rust entity parameter. DDL remains bound to its current typed administrative
surface. The integrity entry point resolves entity-bearing starts through the
registered runtime selector and then requires an exact accepted-authority
match. Query and direct mutation surfaces return SQL-shaped output; resumable
update and integrity execution return their canonical typed receipts instead.

Read-admission lanes, generated endpoint lane ownership, and the current
read-surface inventory are documented in `docs/contracts/READ_ADMISSION.md`.
In particular, generated `icydb_query` is controller-gated by default and may
instead replace controller authority with one synchronous application guard.
Guarded mode authorizes the complete admitted SQL read lane, rejects anonymous
callers, and does not become a generated `PublicRead` endpoint. This includes
`SHOW`, `DESCRIBE`, and `EXPLAIN` when SQL introspection is enabled. The
separately declared `icydb_schema` method may have its own guard, but it neither
substitutes for nor widens SQL introspection authority. Caller-facing reads
needing a narrower contract should use typed/dynamic APIs so the default
bounded read-admission gate applies.

## Cursor Pagination

Cursor-based pagination is not part of the scalar SQL surface.

- SQL uses `LIMIT` / `OFFSET` for scalar windowing.
- Scalar cursor pagination is not exposed by the maintained typed/dynamic API.
- This is intentional: cursor semantics are transport-level, not query
  semantics.

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

The following are intentionally not part of query SQL:

- cursor-based pagination
- continuation tokens
- streaming controls
- byte-metric diagnostics such as `bytes()` and `bytes_by(...)`

These are not part of the maintained scalar query surface.

`CHECK INTEGRITY` is the explicit operational exception. It carries only an
opaque engine-issued job identity and acknowledgement sequence; callers never
author physical phase, checkpoint, revision, or proof-vector state.

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

- scalar row loads
- scalar `DISTINCT` loads
- global aggregate loads with one or more aggregate projection terminals and no
  `GROUP BY`
- grouped aggregate loads
- aggregate `DISTINCT` terminals and aggregate `FILTER (WHERE ...)` modifiers
- narrow computed projection loads, including admitted bounded arithmetic,
  numeric scalar functions, text-function projection forms, and searched `CASE`

- scalar `WHERE`, computed projection, projection-alias ordering, `LIMIT`, and
  `OFFSET` clauses compose within their independently admitted bounds

- grouped `WHERE`, aggregate projection, `GROUP BY`, `HAVING`, aggregate-alias
  ordering, and `LIMIT` clauses compose within their independently admitted bounds

`ORDER BY` uses IcyDB's canonical value comparator. For nullable values, `ASC`
places `NULL` before present values and `DESC` reverses that comparator, so
`NULL` sorts after present values. Later `ORDER BY` terms remain tie-breakers
inside equal nullable groups.

#### Fixed-width `U256`

The `u256` schema type represents exactly `0..=2^256-1`. SQL decimal literals
use `U256 'value'`; malformed, negative, or out-of-range literals fail closed.
The type remains distinct from every other numeric domain and does not gain an
implicit mixed-width coercion.

`U256` supports equality, ordering, indexed ranges, projection arithmetic,
searched `CASE`, grouping, scalar and aggregate `DISTINCT`, `MIN`, `MAX`, and
`SUM`. Addition, subtraction, multiplication, division, remainder, and
`SUM(U256)` are checked: overflow, underflow, and a zero divisor return the
existing typed numeric execution error rather than wrapping modulo `2^256`.
`AVG(U256)`, bitwise operators, shifts, and bit tests are outside the current
SQL subset.

#### Exact Primary-Key Reads

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

SQL placeholder parameters are not part of the current public SQL subset. A
shape such as `WHERE pk = ?` is rejected before primary-key canonicalization.
If SQL parameters are added later, parameter binding must preserve the same
accepted-schema key encoding, cache-safety, and fail-closed contracts.

### `EXPLAIN`

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

- `DESCRIBE entity` / `DESCRIBE entity VERBOSE`
- `SHOW INDEXES FROM entity`
- `SHOW INDEXES IN entity`
- `SHOW COLUMNS entity` / `SHOW COLUMNS entity VERBOSE`
- `SHOW RELATIONS FROM entity`
- `SHOW RELATIONS IN entity`
- `SHOW CONSTRAINTS FROM entity`
- `SHOW CONSTRAINTS IN entity`
- `SHOW ENTITIES` / `SHOW ENTITIES VERBOSE`
- `SHOW ENTITY entity` / `SHOW ENTITY entity VERBOSE`
- `SHOW STORES` / `SHOW STORES VERBOSE`
- `SHOW MEMORY`

`SHOW ENTITIES` lists registered runtime entities with owning store, storage
mode, compact schema counts, and `sv` schema version. `SHOW ENTITY` returns
the same row shape filtered to one entity name, using exact matching first and
case-insensitive matching as a fallback. `SHOW STORES` lists registered stores
and their storage modes. `SHOW MEMORY` lists stable-memory tags, memory IDs,
and their owning stores. The default shell rendering stays compact; `VERBOSE`
exposes full entity and store paths for debugging.

`SHOW CONSTRAINTS` identifies the backing index ID and accepted index name for
every unique constraint. It also renders the accepted index predicate. An
unfiltered unique constraint reports `unique_index_v1`; a filtered unique
constraint reports `partial_unique_index_v1`, so partial uniqueness is never
presented as an unconditional table constraint.

Generated `icydb_query` gates `EXPLAIN`, `DESCRIBE`, and `SHOW` by
the visible source declaration. `icydb_sql_query(introspection = true)`
uses the `icydb/sql` Cargo capability and admits the frozen introspection
families; the `false` form rejects them with `SqlIntrospectionDisabled`.
Canister-owned Cargo features may place the declaration behind `#[cfg(...)]`
for local/test builds. No build target or configuration file selects this
policy implicitly.

Storage modes have distinct contracts. `heap` is volatile live storage with
absent stable allocation identity and no row/index recovery. `journaled` is
the durable cached-stable store: live reads use Rust BTree projections,
committed journal records are folded into canonical stable data/index/schema
BTrees, and `SHOW MEMORY` reports the fourth journal-tail memory role
separately from the three canonical stable roles. Direct `stable` storage is
not a supported current storage mode. The full operator-facing storage
durability boundary is documented in `docs/contracts/DURABILITY.md`.

`SHOW INDEXES` includes index lifecycle and origin annotations. Generated
entity-model indexes report `origin=generated`; indexes added through SQL DDL
report `origin=ddl`. Only DDL-origin field-path indexes are droppable through
SQL DDL.

Default `DESCRIBE` and `SHOW COLUMNS` return the same compact accepted column
rows with fixed `name`, `type`, `nullable`, `key`, `default`, and `extra`
columns. Key labels are `PRI`, single-field `UNI`, compound/non-unique `MUL`,
or `-`; a compound unique member never implies independent uniqueness. Compact
defaults use accepted insert behavior only, and nested fields use canonical
dotted paths with ancestor-aware nullability.

`DESCRIBE ... VERBOSE` retains the complete operational schema dossier,
including generated-vs-DDL index origin, accepted identity state, row-layout
history, relations, constraints, and validation progress. `SHOW COLUMNS ...
VERBOSE` returns its detailed accepted field/layout table only. Those verbose
field rows expose insertion policy separately from historical physical
absence: `insert_omission`, bounded canonical `insert_default` facts,
`introduced_in_layout`, and separately decoded `historical_fill` facts.
Corrupt accepted temporal payloads reject introspection rather than falling
back to generated metadata or a byte-only display.

`SHOW RELATIONS FROM|IN entity` returns accepted relation rows in stable
relation-ID order. Its shell table uses the qualified accepted target path and
the closed `Single`, `List`, or `Set` cardinality. Verbose `DESCRIBE` and `SHOW
CONSTRAINTS` continue to consume the maintained constraint projection ordered
by stable constraint ID. It reports accepted identity, origin, current display
names, referenced fields, structural semantics, activation state, durable
validation phase/counters, and canonical accepted check SQL.

While a targeted-rule semantic edit is pending, the projection emits two
adjacent lifecycle entries with the same stable constraint ID: the currently
validated operation first, then the candidate activation and its validation
state. Promotion returns the projection to one validated entry; abort removes
only the candidate entry. The response shape is unchanged.

Introspection modifiers not listed above are outside the current subset. In
particular, filtering clauses and extra entity operands fail closed instead of
being ignored or interpreted as alternate catalog commands.

### `CHECK INTEGRITY`

The direct administrative SQL surface accepts exactly:

```sql
CHECK INTEGRITY entity QUICK
CHECK INTEGRITY entity DEEP START 'submission_key'
CHECK INTEGRITY DEEP CONTINUE '64-character-job-id' AFTER page_sequence
CHECK INTEGRITY DEEP ABORT '64-character-job-id'
```

`execute_admin_integrity_sql(...)` requires the caller to enforce controller
or equivalent integrity-specific authorization and supply the same bounded
stable owner identity used by the typed surface. The job ID is an engine-issued
64-character hexadecimal identity and is never authorization.

SQL lowers these forms directly to `IntegrityCheckRequest`; SQL does not own a
second execution controller or response shape. Quick and start resolve the SQL
entity through registered runtime routing, then the integrity controller
requires the tag, entity path, and store path to match accepted authority.
Continue and abort resolve entity ownership only from the authorized durable
job.

Unknown modes, missing submission keys, malformed job IDs, negative or missing
acknowledgement sequences, additional entity arguments on continue/abort, and
all predicates, projections, ordering, limits, or caller-authored internal
progress fail closed.

A visible `icydb_integrity` declaration emits the distinct controller-gated
update endpoint when the `icydb/sql` capability is compiled. The endpoint
derives the durable job owner from the authenticated caller principal and
delegates to `execute_admin_integrity_sql(...)`; it does not pass through
generated query, DDL, or row-mutation dispatch. Compiling the capability alone
exports nothing.

Applications using the direct method must provide their own equivalent
authorization boundary. Ordinary query, mutation, and DDL entry points reject
this grammar.

### DDL

Supported shapes:

- `CREATE INDEX name ON entity (field_path)`
- `CREATE INDEX name ON entity (field_path, another_field_path)`
- `CREATE INDEX name ON entity (field_path ASC)`
- `CREATE INDEX name ON entity (field_path) WHERE predicate`
- `CREATE INDEX name ON entity (LOWER(field_path))`
- `CREATE INDEX name ON entity (UPPER(field_path))`
- `CREATE INDEX name ON entity (TRIM(field_path))`
- `CREATE INDEX IF NOT EXISTS name ON entity (field_path)`
- `CREATE INDEX IF NOT EXISTS name ON entity (LOWER(field_path))`
- `CREATE UNIQUE INDEX name ON entity (field_path)`
- `CREATE UNIQUE INDEX name ON entity (LOWER(field_path))`
- `DROP INDEX name ON entity`
- `DROP INDEX name`
- `DROP INDEX IF EXISTS name ON entity`
- `DROP INDEX IF EXISTS name`
- `ALTER TABLE entity ADD COLUMN field type`
- `ALTER TABLE entity ADD COLUMN field type DEFAULT value`
- `ALTER TABLE entity ALTER COLUMN field SET DEFAULT value`
- `ALTER TABLE entity ALTER COLUMN field DROP DEFAULT`
- `ALTER TABLE entity ALTER COLUMN field SET NOT NULL`
- `ALTER TABLE entity ALTER COLUMN field DROP NOT NULL`
- `ALTER TABLE entity RENAME COLUMN old_name TO new_name`
- `ALTER TABLE entity DROP COLUMN field`
- `ALTER TABLE entity DROP COLUMN IF EXISTS field`
- `ALTER TABLE entity ADD CONSTRAINT name CHECK (expression)`
- `ALTER TABLE entity ADD CONSTRAINT name CHECK (expression) NOT VALID`
- `ALTER TABLE entity VALIDATE CONSTRAINT name`
- `ALTER TABLE entity VALIDATE CONSTRAINT name AFTER page_sequence`
- `ALTER TABLE entity DROP CONSTRAINT name`
- `ALTER TABLE entity DROP CONSTRAINT IF EXISTS name`

SQL DDL is a frontend over accepted schema catalog mutation, not the source of
schema authority. Schema mutation and row-rewrite admission remain governed by
`docs/contracts/WRITE_ADMISSION.md`.

`CREATE INDEX` currently admits field-path secondary indexes and deterministic
text expression secondary indexes. Single-field, multi-field, unique, explicit
`ASC`, filtered `WHERE` predicates, and `LOWER`/`UPPER`/`TRIM` expression keys
are supported. Every field path must already exist in the accepted schema, must
be indexable, and must not duplicate an accepted index name or identical
accepted index contract. Non-unique indexes publish through the existing exact
physical rebuild boundary. `CREATE UNIQUE INDEX` instead publishes a
planner-invisible, new-write-gated candidate; bounded
`VALIDATE CONSTRAINT index_name` calls build and verify its isolated generation,
and only a clean promotion makes it planner-visible. `DROP INDEX` aborts a
pending SQL-owned unique activation or removes an already accepted SQL-owned
index. `CREATE INDEX IF NOT EXISTS` no-ops only when the accepted catalog or a
live candidate already has the exact requested index contract. Conflicting
existing definitions still reject.

A unique index whose accepted key can omit a top-level nullable source must
carry an explicit matching `field IS NOT NULL` conjunct for every such source.
Duplicate matching conjuncts are ignored, and additional admitted conjuncts
are allowed. `OR`, `NOT`, comparisons, function inference, and unrelated
guards do not substitute for the exact conjunct. Omit-capable nested unique
sources reject because the maintained predicate binder is top-level only.
Non-unique nullable index membership is unchanged. The schema-owned accepted
contract validates SQL candidates, generated candidates, active and pending
accepted indexes, nullability changes, promotion, and reopen; SQL does not own
a second interpretation of this rule.

The existing filtered-index implication owner may select such an accepted
index only when the complete query predicate proves every accepted filter
conjunct. An exact matching `field IS NOT NULL`, or a supported strict equality
or ordered comparison of that field with a bound non-null value, proves its
non-null guard. Every composite guard and every additional filter conjunct
must be proved. Null or unknown values and unsupported `OR`, `NOT`, cross-field,
or expression-to-source reasoning stay conservative, so unavailable proof
keeps the maintained full-scan route rather than treating omitted rows as
absent.

`ASC` is accepted as IcyDB's default deterministic physical key order. `DESC`
is not yet supported for SQL DDL indexes and fails with explicit
unsupported-feature diagnostics.

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
operate on DDL-owned fields only. `SET NOT NULL` publishes a new-write gate and
keeps the accepted field nullable while bounded `VALIDATE CONSTRAINT` pages
prove historical rows. A clean promotion atomically tightens the field;
findings remain typed and resumable. `DROP NOT NULL` aborts a pending activation
and its validation job or removes an already accepted DDL-owned not-null
constraint. Generated/model-owned fields remain Rust-schema owned.

`ALTER TABLE ... RENAME COLUMN ... TO ...` publishes metadata-only accepted
schema changes for DDL-owned fields. Field ID, row slot, default/nullability,
decode contracts, and direct field-path index identity remain stable; accepted
field names, direct field-path index labels, and expression-index
source/canonical labels are updated together. Filtered-index predicate SQL
labels relabel through the reduced predicate AST and rebind against the full
final accepted schema. Rewrite or rebind failure rejects before publication;
stale predicate text is never retained as a fallback. Generated fields reject
before publication.

`ALTER TABLE ... DROP COLUMN ...` is admitted only when the entity is exactly
empty. It publishes a dense accepted schema for a DDL-owned field without a row
rewrite: active metadata removes the field, and surviving field IDs and
physical slots are renumbered to `1..N` and `0..N-1`. A later `ADD COLUMN`
allocates the next dense identity. Nonempty entities plus primary-key,
generated, and index-dependent fields reject before publication.
`DROP COLUMN IF EXISTS` reports `no_op` only when the target field is absent.

Accepted `Identity::next` fields participate in SQL DML but not SQL DDL.
`INSERT` may omit the field or spell `DEFAULT`, and `RETURNING` exposes the
generated exact Nat value. Explicit values reject because Identity is
GENERATED ALWAYS. `GENERATED ... AS IDENTITY`, custom start/step, BY DEFAULT,
cycle, alter, restart, and reseed syntax remain unsupported; SQL cannot create
or mutate Identity policy in this release.

`ADD CONSTRAINT ... CHECK` accepts the bounded V1 check-expression subset and
binds field identity against the accepted-before catalog. Plain `ADD` validates
the complete historical domain in one bounded call and publishes the validated
constraint only when that exact proof succeeds. If rows violate the expression
or the proof exceeds its bound, the operation changes no accepted schema,
activation, validation job, or physical state.

`ADD ... NOT VALID` explicitly publishes an `EnforcingNewWrites` activation.
Future writes are gated immediately, while historical validation advances only
through `VALIDATE CONSTRAINT`. Each validation response reports typed
constraint/job identity, full-width page sequence, engine-owned state,
revision status, cumulative rows, bounded findings, and completion. A finding
page remains stable until acknowledged by its exact `AFTER page_sequence`.
`DROP CONSTRAINT` drops or aborts only SQL-DDL-owned checks; generated and
structural constraint ownership remains outside this surface.

Destructive DDL keeps physical state and accepted-schema publication atomic at
the guarded operation boundary. Rejection before a durable commit marker owns
the candidate leaves the accepted-before schema and physical state
authoritative. Once a marker owns the candidate, startup-driver reentry
completes the accepted-after schema and its required physical state before
serving another operation.

## Public SQL Mutation Execution

Supported public mutation shapes are:

- `INSERT`
- `UPDATE`
- `DELETE`
- admitted narrow `... RETURNING`

`INSERT` accepts `DEFAULT` in direct `VALUES` positions and accepts
`INSERT INTO entity DEFAULT VALUES` without an explicit column list. `UPDATE`
accepts `SET field = DEFAULT`. These are write-intent forms, not general scalar
expressions: `DEFAULT` in predicates, projections, function arguments, or
nested expressions fails closed. Insert omission and explicit insert
`DEFAULT` use the same current accepted insertion policy while preserving
request-specific diagnostics. Update omission preserves the current value;
`SET field = DEFAULT` applies the current accepted ordinary default or nullable
`NULL`, and rejects required, generated, or managed fields.

Mutation ownership lives on one accepted structural write lane:

- `execute_trusted_structural_mutation(...)` for entity/field-name writes;
- `execute_trusted_structural_mutation_batch(...)` for bounded atomic
  same-store mixed writes across at most 64 accepted entities;
- `execute_trusted_typed_write(...)` for explicitly generated typed adapters;
- `execute_trusted_typed_write_row(...)` for the same typed write with exact
  single-row validation and accepted projection;
- `execute_trusted_structural_mutation_batch_rows(...)` for ordered projected
  rows from a same-entity structural batch;
- `trusted_typed_write_batch()` for binding-resolved generated inputs over the
  same bounded same-store structural batch;
- `execute_trusted_structural_insert_batch(...)` for atomic same-entity
  insert convenience.

Every SQL row after-image is decoded against accepted field contracts and then
enters the same structural write-admission pipeline used by non-SQL structural
mutation. `trusted` in a SQL API name describes caller-owned authorization and
surface policy; it is not a schema-validation bypass.

Public SQL ownership is split deliberately:

- `execute_trusted_sql_query(...)` owns accepted-catalog-driven read, explain,
  and introspection SQL
- `execute_trusted_sql_mutation(...)` owns trusted `INSERT` and `DELETE`
- `execute_trusted_sql_exact_update(...)` owns complete-set SQL `UPDATE`
- `execute_trusted_sql_prefix_update(...)` owns intentional ordered-prefix
  SQL `UPDATE`
- `start_trusted_sql_mutation_job(...)`, `mutation_job_state(...)`,
  `advance_trusted_mutation_job(...)`, `cancel_unadvanced_mutation_job(...)`,
  `acknowledge_mutation_job(...)`, and `progress_job_inventory()` own durable
  trusted convergence and capacity recovery without exposing SQL or
  continuation custody
- `execute_admin_sql_ddl(...)` owns accepted-catalog schema DDL SQL

The current durable advance dispatches one engine-owned Forward or Verify page.
Forward examines at most 4,096 authoritative keys, stages at most 240 updates,
stops before either its 16 MiB raw key-plus-row scan limit or the structural
writer's exact 16 MiB key/before/after staging limit, commits target rows with
the next sequence/replay receipt atomically, and advances zero-update pages
through an exact progress replacement. The frozen operation timestamp remains
the logical statement identity; writer-managed physical time is captured from
the current advance message. Forward exhaustion captures the exact
target-entity journal revision produced by its prepared commit and enters
Verify. Verify examines at most 4,096 keys within its independent 16 MiB raw scan
limit, retains one revision baseline across all Verify messages, and reports
`Completed` only after clean exhaustion at that baseline. Target revision drift
or a residual row restarts Forward from the beginning; accepted-authority,
internal-policy, candidate-size, managed-time, or admitted execution-policy
drift persists a typed `RestartRequired` terminal receipt.

Each non-replay advance uses one fixed engine-owned execution allocation of 30
billion instructions with a 5-billion failure reserve and further IC
update-message margin. Exact replay returns before page execution. Applications
cannot select or increase count, byte, or instruction budgets.

### SQL `UPDATE` Availability By Surface

`UPDATE` requires explicit intent. Generated query and DDL endpoints still
reject it; generated canister update exposure remains a separate opt-in write
endpoint with an explicit public-safe policy.

Current boundary:

- `execute_trusted_sql_exact_update(...)` accepts a positive
  `require_affected_at_most` assertion and selects in canonical primary-key
  order. A cap-plus-one match proves affected-row overflow; an independent
  cap-plus-one scanned-key probe enforces the engine scan budget. Either bound
  rejects before mutation, while success commits the complete matching set.
- exact `UPDATE` accepts current narrow `RETURNING` forms under the same row
  assertion and the engine response-byte bound.
- `execute_trusted_sql_mutation(...)` rejects `UPDATE`; it cannot infer
  exact versus prefix intent.
- trusted resumable `UPDATE` starts one IcyDB-custodied durable mutation job.
  The application retains only its single-incarnation job ID, expected
  sequence, and bounded idempotency key. Each advance scans in authoritative
  primary-key order, commits at most one independently atomic fixed-patch page,
  and reports completion only after a clean full verification sweep at one
  unchanged target-entity journal revision.
- resumable execution is restricted to journaled stores, fixed authored
  assignments, stable scopes, batch-independent accepted constraints, and
  entity graphs without application write callbacks. It has no row `RETURNING`
  or cumulative affected-row claim, and raw mutation continuations never leave
  IcyDB custody.
- generated `icydb_query` rejects row mutation SQL, including `UPDATE`.
- generated `icydb_ddl` rejects row mutation SQL, including `UPDATE`.
- generated `icydb_update` is not part of the default generated canister
  surface; it is emitted only when the canister config selects an update policy.
- `update = true` and `update = "primary_key"` select the public
  primary-key-only policy.
- `update = "bounded"` selects the public bounded deterministic policy, which
  requires explicit primary-key ordering and a limit.

`execute_trusted_sql_prefix_update(...)` retains the maintained bounded
policy: a positive limit no greater than 100, explicit canonical ascending
primary-key order, and no offset. The limit selects only that intentional
prefix and makes no complete-set claim. Generated `icydb_update` dispatch uses
the same configured public policies and never calls the broad trusted mutation
lane directly.

## Blob Literals and Blob Values

SQL accepts hex blob literals in the `X'...'` / `x'...'` form. The hex body
must contain only hexadecimal digits and must have an even number of digits.
The decoded payload is capped at 1,048,576 bytes per literal so oversized SQL
text fails before allocating unbounded blob buffers.

Supported blob behavior:

- `INSERT` / `UPDATE` can write blob fields from hex blob literals.
- `SELECT` and `DELETE ... RETURNING` can return blob fields.
- `WHERE blob_field = X'...'` and `WHERE blob_field <> X'...'` compare blob
  bytes exactly.
- `OCTET_LENGTH(blob_field)` returns the blob byte length without changing
  `LENGTH(text)` character-count behavior.

Unsupported blob behavior:

- `ORDER BY blob_field` is rejected because raw blob values are not orderable
  through the public SQL surface.
- SQL does not provide streaming blob reads or chunked blob writes; large
  payload transport remains better suited to typed APIs that can expose
  chunk-oriented application boundaries.

## Entity Naming And Aliases

The admitted single-entity naming surface is:

- unqualified entity names
- schema-qualified entity names such as `public.Customer`
- one optional single-table alias, with or without `AS`

Unquoted entity names resolve against accepted catalog display names using
ASCII case-insensitive matching. For example, `DESCRIBE customer` resolves an
accepted entity authored as `Customer`, and metadata continues to report the
authored spelling. Immutable entity source keys remain exact and are not SQL
display-name aliases.

Examples:

- `SELECT * FROM Customer c`
- `SELECT c.name FROM Customer AS c`
- `DELETE FROM Customer c WHERE c.age < 20`
- `UPDATE Customer AS c SET age = 22 WHERE c.name = 'Ada'`
- `INSERT INTO Customer c (name, age) VALUES ('Ada', 22)`

No statement may introduce more than one entity binding.

## Projection

Supported scalar projection forms are:

- `SELECT *`
- `SELECT field, ...`
- `SELECT DISTINCT *`
- `SELECT DISTINCT field, ...`
- admitted bounded scalar expression projections, including searched `CASE`

Supported aggregate projection forms are:

- one or more aggregate terminals with no `GROUP BY`
- aggregate terminals with admitted input expressions
- aggregate terminals with `DISTINCT`
- aggregate terminals with `FILTER (WHERE predicate)`
- grouped projection where grouped key items come first and aggregate or
  post-aggregate computed items come after them

Supported grouped projection examples:

- `SELECT age, COUNT(*) FROM Customer GROUP BY age`
- `SELECT name, COUNT(*), SUM(age) FROM Customer GROUP BY name`
- `SELECT TRIM(name), COUNT(*) FROM Customer GROUP BY name`
- `SELECT age, ROUND(AVG(age), 2) FROM Customer GROUP BY age`
- `SELECT age, AVG(age + 1) + AVG(age + 1) FROM Customer GROUP BY age`
- `SELECT age, CASE WHEN COUNT(*) > 1 THEN 'multi' ELSE 'single' END FROM Customer GROUP BY age`
- `SELECT age, CASE WHEN COUNT(*) > 1 THEN TRUE ELSE FALSE END FROM Customer GROUP BY age`

Unsupported grouped projection examples:

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

- unary numeric functions: `ABS`, `CBRT`, `CEIL` / `CEILING`, `EXP`, `FLOOR`,
  `LN`, `LOG2`, `LOG10`, `SIGN`, `SQRT`
- binary numeric functions: `LOG(base, x)`, `MOD(x, y)`, `POWER(x, y)` /
  `POW(x, y)`
- scale-taking numeric functions: `ROUND(x, scale)`, `TRUNC(x, scale)` /
  `TRUNCATE(x, scale)`

Supported text scalar functions include `LOWER`, `UPPER`, `LENGTH`,
`OCTET_LENGTH`, `TRIM`, `LTRIM`, `RTRIM`, `LEFT`, `RIGHT`, `STARTS_WITH`,
`ENDS_WITH`, `CONTAINS`, `POSITION`, `REPLACE`, and `SUBSTRING`.

`COALESCE` and `NULLIF` provide the admitted value-selection forms. Function
call shapes, argument types, and direct ordering eligibility remain bounded by
the clause-specific lowering contract; admission in the shared expression
family does not make every expression a valid `ORDER BY` target.

The current conditional form is intentionally narrow:

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

- simple `CASE value WHEN ...`
- subqueries or window expressions inside `CASE`
- `CASE` as a loophole for unsupported expression families in that clause

## Projection Aliases

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
- field-to-field comparisons on the same comparison family
- field-to-field comparisons in a grouped query's pre-aggregation `WHERE` lane
- `IN (...)`
- `NOT IN (...)`
- `BETWEEN ... AND ...`
- `NOT BETWEEN ... AND ...`
- `IS NULL`
- `IS NOT NULL`
- `IS TRUE`
- `IS FALSE`
- `IS NOT TRUE`
- `IS NOT FALSE`
- prefix `LIKE 'prefix%'`
- prefix `NOT LIKE 'prefix%'`
- prefix `ILIKE 'prefix%'`
- prefix `NOT ILIKE 'prefix%'`
- `STARTS_WITH(field, 'prefix')`
- searched `CASE`, when it returns values that stay on the admitted boolean
  filtering lane

Narrow casefolded predicate forms are also supported:

- `LOWER(field) LIKE 'prefix%'`
- `STARTS_WITH(LOWER(field), 'prefix')`

`UPPER(...)` remains available as an ordinary scalar expression, including in
full SQL boolean expressions, projections, ordering, and accepted expression
indexes. The reduced predicate-only contract does not reinterpret `UPPER` as
casefolding because Unicode uppercase and lowercase transforms are not
equivalent.

Field-bound range predicates are also supported on the plain-field lane:

- `field BETWEEN lower_field AND upper_field`
- `field NOT BETWEEN lower_field AND upper_field`

The residual-expression lane also admits supported scalar expressions as text
predicate arguments, such as
`STARTS_WITH(REPLACE(name, 'a', 'A'), TRIM('Al'))`. These shapes do not claim
index-predicate extraction when their expression form is not indexable.

Still intentionally excluded from the admitted predicate lane:

- non-prefix `LIKE` / `NOT LIKE` / `ILIKE` / `NOT ILIKE`
- grouped `HAVING` variants that reuse the plain-field boolean special forms
  or text-pattern lane directly

## `HAVING`

Supported `HAVING` forms are:

- grouped aggregate `HAVING` over grouped keys and aggregate outputs
- global aggregate `HAVING` over the implicit single aggregate group
- admitted post-aggregate scalar expressions, including bounded arithmetic,
  wrappers, and searched `CASE`

Still intentionally excluded:

- raw-row-only expressions that escape post-aggregate authority
- grouped `HAVING` reuse of the plain-field text-pattern or boolean-special
  predicate lane

## Public SQL Write `RETURNING`

Supported `RETURNING` forms are intentionally narrow:

- `RETURNING *`
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

- computed expressions in `RETURNING`
- aggregate expressions in `RETURNING`
- other widened projection families beyond `*` or plain field lists

## Explicitly Rejected SQL Families

The finite boundaries below are maintained rejection contracts. They do not
claim to enumerate every SQL input outside IcyDB's constrained subset.

- multi-entity statements, additional `FROM` bindings, and joins
- subqueries and common table expressions
- `UNION`, `INTERSECT`, and `EXCEPT`
- window functions and `OVER (...)`
- transaction-control statements such as `BEGIN`, `COMMIT`, and `ROLLBACK`
- `CAST(...)` expressions

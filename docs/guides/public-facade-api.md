# Public Database Facade API Reference

This is the current command vocabulary for the public database/session facade
under `icydb::db` after the 0.198 read-intent hard cut. It is a reference map,
not a behavioral contract. Detailed read-admission rules live in
[`READ_ADMISSION.md`](../contracts/READ_ADMISSION.md), and endpoint migration
guidance lives in [`read-intent.md`](read-intent.md).

## Why The API Is Intent-First

IcyDB runs inside an Internet Computer canister, not beside the application as
a separate database server. A canister method is already the public API
boundary, the authorization boundary, and the resource boundary. That changes
the cost of ambiguous database helpers.

In PostgreSQL, MySQL, SQLite, and similar databases, `LIMIT`, `OFFSET`, and
ad-hoc SQL are normal query modifiers. The database server owns the connection
pool, planner budget, statement timeout, transaction machinery, and operational
policy around those statements. Application code can still misuse them, but the
database is not usually the same object as the public endpoint.

Many ORMs also expose convenient names such as `one`, `all`, `first`, or
`find_many`. Those names are ergonomic, but they can blur the endpoint promise:
does `all` mean every matching row, every row that fits a cap, or every row in
the current page? Does `one` mean primary-key exactness, "first row under this
order", or "error if more than one row matches"?

IcyDB chooses explicit read intent instead:

| Common database style | IcyDB style | Reason |
| --- | --- | --- |
| `LIMIT 1` for lookup | `by_id(id).try_one()` | Exact lookup should be proved by key shape, not by truncating a result set. |
| `SELECT * ... LIMIT n` for lists | `page(n)?` / `next_page(n, cursor)?` | Public list endpoints should be cursor pages with explicit first-page and continuation calls. |
| `all()` for collections | `collect_complete()` | Complete reads should either return the whole small set or fail instead of silently truncating. |
| `count()` after materialization | `count_exact()` | Exact aggregates should not accidentally mean "over the first N rows." |
| Large admin `LIMIT` scans | `trusted_read_unchecked().admin_batch(...)` | Maintenance scans need a visibly trusted lane and engine-owned batch sizing. |
| Compatibility aliases | One maintained spelling per behavior | Pre-1.0 hard cuts keep generated code, docs, and endpoint reviews unambiguous. |

The result is a slightly more explicit API, but it is easier to audit:
caller-facing endpoints state whether they return an exact row, a public page,
a complete small set, an exact aggregate, or a deliberate partial row window.
That gives IcyDB enough information to reject unsafe shapes with typed errors
before they become expensive or misleading canister calls.

## Read Intent Summary

Use the method that names the endpoint promise:

| Endpoint promise | Public facade command |
| --- | --- |
| Exact row by primary key | `load::<E>().by_id(id).try_one()` |
| Exact rows by primary keys | `load::<E>().by_ids(ids).execute_rows()` |
| Existence | `load::<E>().exists()` / `load::<E>().not_exists()` |
| Public first page | `load::<E>().order_term(...).page(n)?` |
| Public next page | `load::<E>().order_term(...).next_page(n, cursor)?` |
| Complete small set | `load::<E>().collect_complete()` |
| Deliberate partial row window | `load::<E>().partial_window(n).execute_rows()` |
| Exact aggregate | `count_exact()`, `sum_exact(field)`, `min_id_exact()`, `min_exact_by(field)`, `max_id_exact()`, `max_exact_by(field)`, `avg_exact(field)` |
| Trusted maintenance batch | `trusted_read_unchecked().admin_batch(AdminBatchRequest::new())` |

Load queries do not expose public `.limit(...)`, `.one()`, or `.all()`
aliases. They also do not expose fluent `.offset(...)`; caller-facing list
endpoints use `page(limit)` and `next_page(limit, cursor)`. Use `partial_window(...)`
only when returning a partial row window is the endpoint contract. Delete
queries use `max_affected(...)` for mutation safety caps so affected-row bounds
do not share read-limit vocabulary.

## API Tiers

The facade has four audiences. Keep normal endpoint code in Tier 1 unless the
endpoint is deliberately admin, diagnostic, SQL-backed, or generated.

| Tier | Audience | Use for |
| --- | --- | --- |
| Tier 1: Normal endpoint API | Ordinary app endpoints | Intent-first typed/fluent reads, typed writes, bounded deletes, and schema-aware structural mutation. |
| Tier 2: Trusted/admin API | Controller-gated maintenance | Broad maintenance reads and operational SQL after caller authorization. |
| Tier 3: Diagnostics API | Debugging and observability | Planning, EXPLAIN, trace, attribution, catalog, storage, and metrics inspection. |
| Tier 4: Internal/generated API | Generated code and policy wrappers | Validated SQL policy helpers and low-level query/response adapters. Listed for audit, not for normal endpoint code. |

## Tier 1: Normal Endpoint API

These are the commands ordinary caller-facing endpoints should reach for first.

### Session Entry

`DbSession<C>` is the canister-local facade returned by the fallible generated
`db!()` / `db()` helpers. Bootstrap failures preserve their typed
`DatabaseBootstrapError` cause. Normal endpoint code propagates that boundary,
then starts from `db.load::<E>()`, `db.delete::<E>()`, or a typed write command.

```rust
DbSession::new(core_session)

let db = db()?;
db.load::<E>()
db.load_with_consistency::<E>(policy)

db.delete::<E>()
db.delete_with_consistency::<E>(policy)
```

### Endpoint Cookbook

```rust
// Exact lookup.
db.load::<User>()
    .by_id(user_id)
    .try_one()

// Exact ID set.
db.load::<User>()
    .by_ids(user_ids)
    .execute_rows()

// Public cursor page.
let first_page = db.load::<User>()
    .filter_eq("status", "active")
    .order_desc("created_at")
    .order_asc("id")
    .page(50)?;

let next_page = db.load::<User>()
    .filter_eq("status", "active")
    .order_desc("created_at")
    .order_asc("id")
    .next_page(50, cursor)?;

// Complete small set.
db.load::<Role>()
    .filter_eq("project_id", project_id)
    .collect_complete()

// Exact aggregate.
db.load::<User>()
    .filter_eq("status", "active")
    .count_exact()

// Existence check.
db.load::<User>()
    .filter_eq("email", email)
    .exists()

// Deliberate partial row window.
db.load::<Event>()
    .order_desc("created_at")
    .partial_window(100)
    .execute_rows()

// Trusted maintenance batches are Tier 2; do not use them as ordinary
// caller-facing endpoint recipes.
```

Most endpoint code should not call `execute()` directly. Prefer a terminal that
states the endpoint promise: `try_one`, `page`, `collect_complete`, an exact
aggregate helper, `exists`, or `not_exists`. Use `execute()` when the caller
intentionally handles the full `QueryResponse<E>` shape.

### Load Query Shape

These commands refine `db.load::<E>()` and
`db.load_with_consistency::<E>(...)`.

```rust
.by_id(id)
.by_ids(ids)

.filter(expr)
.filter_eq(field, value)
.filter_ne(field, value)
.filter_lt(field, value)
.filter_lte(field, value)
.filter_gt(field, value)
.filter_gte(field, value)
.filter_text_eq_ci(field, value)

.filter_eq_field(left_field, right_field)
.filter_ne_field(left_field, right_field)
.filter_lt_field(left_field, right_field)
.filter_lte_field(left_field, right_field)
.filter_gt_field(left_field, right_field)
.filter_gte_field(left_field, right_field)

.filter_in(field, values)
.filter_not_in(field, values)
.filter_contains(field, value)
.filter_is_null(field)
.filter_is_not_null(field)
.filter_is_missing(field)
.filter_is_empty(field)
.filter_is_not_empty(field)

.filter_text_contains(field, value)
.filter_text_contains_ci(field, value)
.filter_text_starts_with(field, value)
.filter_text_starts_with_ci(field, value)
.filter_text_ends_with(field, value)
.filter_text_ends_with_ci(field, value)

.filter_between(field, lower, upper)
.filter_between_fields(field, lower_field, upper_field)
.filter_not_between(field, lower, upper)
.filter_not_between_fields(field, lower_field, upper_field)

.order_term(term)
.order_by(direction, expr)
.order_asc(expr)
.order_desc(expr)
.order_terms(terms)

.partial_window(n)

.group_by(field)?
.aggregate(expr)
.grouped_limits(max_groups, max_group_bytes)
.having_group(field, op, value)?
.having_aggregate(aggregate_index, op, value)?

.singleton()
```

`singleton()` is only for `SingletonEntity` types. It is not a generic one-row
terminal. For normal one-row reads, use `.by_id(id).try_one()`.

`partial_window(n)` switches into `PartialWindowLoadQuery`. That wrapper
exposes partial materialization and diagnostics only; it intentionally does not
expose semantic terminals such as `page(...)`, `collect_complete()`,
`exists()`, or exact aggregate helpers.

### Load Terminals

Use these commands to execute a load query.

```rust
.page(limit)?
.next_page(limit, cursor)?

.try_one()
.exists()
.not_exists()
.collect_complete()

.count_exact()
.min_id_exact()
.min_exact_by(field)
.max_id_exact()
.max_exact_by(field)
.sum_exact(field)
.avg_exact(field)
```

`page(limit)` and `next_page(limit, cursor)` execute the public page terminal
directly. Use `execute_rows()` for exact ID sets or deliberate partial windows
when the endpoint contract is row-shaped. Use `execute()` for advanced callers
that intentionally inspect `QueryResponse<E>`.

```rust
.execute_rows()
.execute()
```

### Delete API

These commands refine `db.delete::<E>()` and
`db.delete_with_consistency::<E>(...)`.

```rust
.by_id(id)
.by_ids(ids)

.filter(expr)
.filter_eq(field, value)
.filter_ne(field, value)
.filter_lt(field, value)
.filter_lte(field, value)
.filter_gt(field, value)
.filter_gte(field, value)
.filter_text_eq_ci(field, value)

.filter_eq_field(left_field, right_field)
.filter_ne_field(left_field, right_field)
.filter_lt_field(left_field, right_field)
.filter_lte_field(left_field, right_field)
.filter_gt_field(left_field, right_field)
.filter_gte_field(left_field, right_field)

.filter_in(field, values)
.filter_not_in(field, values)
.filter_contains(field, value)
.filter_is_null(field)
.filter_is_not_null(field)
.filter_is_missing(field)
.filter_is_empty(field)
.filter_is_not_empty(field)

.filter_text_contains(field, value)
.filter_text_contains_ci(field, value)
.filter_text_starts_with(field, value)
.filter_text_starts_with_ci(field, value)
.filter_text_ends_with(field, value)
.filter_text_ends_with_ci(field, value)

.filter_between(field, lower, upper)
.filter_between_fields(field, lower_field, upper_field)
.filter_not_between(field, lower, upper)
.filter_not_between_fields(field, lower_field, upper_field)

.order_term(term)
.order_by(direction, expr)
.order_asc(expr)
.order_desc(expr)
.order_terms(terms)

.max_affected(n)
.returning_all()
.returning(fields)
.singleton()

.execute()
.is_empty()
.count()
.require_one()
.require_some()
```

`returning_all()` and `returning(fields)` switch to
`SessionDeleteReturningQuery`; that returning query keeps the same shape,
planning, and `max_affected(...)` commands, but `execute()` returns
`RowProjectionOutput`.

`max_affected(n)` is a mutation safety cap. It is not a read materialization
limit and is intentionally named differently from read-intent terminals.

### Write API

Typed writes live directly on `DbSession<C>`.

```rust
db.insert(entity)
db.insert_returning_all(entity)
db.insert_returning(entity, fields)

db.create(input)
db.create_returning_all(input)
db.create_returning(input, fields)

db.replace(entity)
db.update(entity)
db.update_returning_all(entity)
db.update_returning(entity, fields)

db.insert_many_atomic(entities)
db.insert_many_non_atomic(entities)
db.replace_many_atomic(entities)
db.replace_many_non_atomic(entities)
db.update_many_atomic(entities)
db.update_many_non_atomic(entities)
```

Use the `*_many_atomic` helpers when the same-entity batch must be
all-or-nothing. The `*_many_non_atomic` helpers are explicit fail-fast,
prefix-commit APIs.

### Structural Mutation

Structural mutation is the dynamic field-name write ingress. Build field
patches through the session so names resolve through the accepted schema.

```rust
StructuralPatch::new()

db.structural_patch::<E, _, _>(fields)
db.mutate_structural::<E>(key, patch, MutationMode::Insert)
db.mutate_structural::<E>(key, patch, MutationMode::Update)
db.mutate_structural::<E>(key, patch, MutationMode::Replace)
```

## Tier 2: Trusted/Admin API

Trusted/admin code must first enforce caller authorization outside IcyDB. The
database facade then makes the trusted lane visible in code review.

```rust
db.load::<E>()
    .trusted_read_unchecked()
    .admin_batch(AdminBatchRequest::new())

db.load::<E>()
    .trusted_read_unchecked()
    .execute_rows()
```

`AdminBatchRequest` is for trusted maintenance scans. The batch size is
engine-owned; callers may only provide continuation cursors.

```rust
AdminBatchRequest::new()
AdminBatchRequest::next(cursor)
```

SQL session commands are available with the `sql` feature. They are
trusted/admin surfaces unless wrapped by an application-owned policy.

```rust
db.execute_trusted_sql_query::<E>(sql)
db.execute_trusted_sql_mutation::<E>(sql)
db.execute_trusted_sql_exact_update::<E>(sql, require_affected_at_most)
db.execute_trusted_sql_prefix_update::<E>(sql)
db.execute_admin_sql_ddl::<E>(sql)
```

The broad mutation helper accepts `INSERT` and `DELETE`. An `UPDATE` must state
whether the complete target is required (`exact`) or one deliberate ordered
`LIMIT` window is sufficient (`prefix`). Exact selection uses authoritative
primary-key traversal and rejects affected-row or scan-budget overflow before
any row is changed; prefix success reports only the selected window.

Do not expose caller-controlled SQL through these helpers in ordinary public
endpoints. Prefer typed/fluent read-intent APIs or an application-owned SQL
allowlist that maps user input to fixed validated statements.

## Tier 3: Diagnostics API

Diagnostics inspect planning, route choice, admission, storage, and catalog
state. They do not authorize broad row execution.

### Query Diagnostics

These commands inspect a load or delete query without changing rows.

```rust
.plan_hash_hex()
.trace()
.explain()
```

Load queries also expose read-terminal explain commands:

```rust
.explain_exists()
.explain_not_exists()
.explain_count_exact()
.explain_min_id_exact()
.explain_min_exact_by(field)
.explain_max_id_exact()
.explain_max_exact_by(field)
.explain_sum_exact(field)
.explain_avg_exact(field)

.explain_execution()
.explain_execution_text()
.explain_execution_json()
.explain_execution_verbose()
```

Feature-gated diagnostics helpers:

```rust
.exists_with_attribution()
.collect_complete_with_attribution()
.count_exact_with_attribution()
```

### Catalog And Storage Diagnostics

Catalog helpers read the accepted runtime schema/catalog state.

```rust
db.debug()
db.metrics_sink(sink)

db.show_indexes::<E>()
db.show_columns::<E>()
db.show_entities()
db.try_show_entities()
db.show_stores()
db.show_memory()

db.describe_entity::<E>()
db.try_describe_entity::<E>()
db.storage_report(name_to_path)
```

## Tier 4: Internal/Generated API

This section is listed for audit completeness. These methods are not the normal
developer-facing facade and should not be taught as endpoint recipes.

### Direct Query Diagnostics

Prebuilt `Query<E>` execution is not a public endpoint recipe, and active docs
intentionally do not provide copyable direct-query examples. Remaining hidden
`Query<E>` surfaces are diagnostics/planning hooks such as trace and
attribution. Public facade fluent wrappers do not expose raw query extraction;
diagnostics tooling should prefer hidden fluent attribution terminals.

### Generated/Policy SQL Helpers

These helpers are used by generated SQL policy wrappers and validated
application-owned SQL ingress. Do not call them directly from ordinary endpoint
code unless that endpoint owns the policy boundary explicitly.

```rust
db.execute_trusted_sql_query_with_perf_attribution::<E>(sql)
db.execute_trusted_sql_query_with_attribution::<E>(sql)

db.execute_validated_sql_public_primary_key_update::<E>(plan)
db.execute_sql_public_primary_key_update::<E>(sql)
db.execute_validated_sql_public_bounded_update::<E>(plan)
db.execute_sql_public_bounded_update::<E>(sql)

db.execute_validated_sql_public_primary_key_delete::<E>(plan)
db.execute_sql_public_primary_key_delete::<E>(sql)
db.execute_validated_sql_public_bounded_delete::<E>(plan)
db.execute_sql_public_bounded_delete::<E>(sql)
```

## Response Types Appendix

Most endpoint code should use terminal return types directly. This appendix
lists response narrowing helpers for advanced callers and generated/policy
boundaries.

### Query Responses

`execute()` returns `QueryResponse<E>`, which must be narrowed to scalar rows
or grouped rows when the caller expects a concrete shape.

```rust
response.is_rows()
response.is_grouped()
response.into_rows()
response.into_grouped()
```

Scalar row responses:

```rust
Response::from_core(core_response)

rows.count()
rows.exists()
rows.require_one()
rows.require_some()

rows.entity()
rows.try_entity()
rows.entities()

rows.id()
rows.try_id()
rows.ids()
rows.contains_id(id)
```

Paged responses:

```rust
page.items()
page.next_cursor()
page.read_intent()
page.into_items()
page.into_next_cursor()

grouped.items()
grouped.next_cursor()
grouped.execution_trace()
grouped.into_items()
grouped.into_next_cursor()
grouped.into_execution_trace()
```

Write/mutation results:

```rust
let inserted: E = db.insert(entity)?;
let inserted_many: Vec<E> = db.insert_many_atomic(entities)?;
let affected_rows: u32 = db.delete::<E>().max_affected(10).execute()?;
let returning: RowProjectionOutput = db.insert_returning_all(entity)?;
```

Typed writes return their current domain value directly. SQL mutations return
`SqlQueryResult`; there is no separate mutation-response compatibility facade.

Row projection payloads:

```rust
RowProjectionOutput {
    entity,
    columns,
    rows,
    row_count,
}
output.rendered_rows()

render_output_value_text(value)
```

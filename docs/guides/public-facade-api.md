# Public Database Facade API Reference

This is the current command vocabulary for the public database/session facade
under `icydb::db`. It is a reference map, not a behavioral contract. Detailed
read-admission rules live in [`READ_ADMISSION.md`](../contracts/READ_ADMISSION.md),
and endpoint migration guidance lives in [`read-intent.md`](read-intent.md).

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
| `SELECT * ... LIMIT n` for lists | `page(PageRequest::first(n))?.execute()` | Public list endpoints should be cursor pages with request-owned continuation. |
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
| Public page | `load::<E>().order_term(...).page(PageRequest::first(n))?.execute()` |
| Complete small set | `load::<E>().collect_complete()` |
| Deliberate partial row window | `load::<E>().partial_window(n).execute_rows()` |
| Exact aggregate | `count_exact()`, `sum_exact(field)`, `min_exact()`, `min_exact_by(field)`, `max_exact()`, `max_exact_by(field)`, `avg_exact(field)` |
| Trusted maintenance batch | `trusted_read_unchecked().admin_batch(AdminBatchRequest::new())` |

Load queries do not expose public `.limit(...)`, `.one()`, or `.all()`
aliases. Use `partial_window(...)` only when returning a partial row window is
the endpoint contract. `limit(...)` remains on delete queries, where it bounds
affected rows rather than read materialization.

## Session Commands

`DbSession<C>` is the canister-local facade returned by generated `db!()` /
`db()` helpers.

```rust
DbSession::new(core_session)

db.debug()
db.metrics_sink(sink)

db.load::<E>()
db.load_with_consistency::<E>(policy)

db.delete::<E>()
db.delete_with_consistency::<E>(policy)
```

## Load Query Shape

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

.offset(n)
.partial_window(n)

.group_by(field)?
.aggregate(expr)
.grouped_limits(max_groups, max_group_bytes)
.having_group(field, op, value)?
.having_aggregate(aggregate_index, op, value)?

.trusted_read_unchecked()
.only()
```

`only()` is available for `SingletonEntity` types.

## Load Terminals

Use these commands to execute a load query.

```rust
.execute()
.execute_rows()
.execute_trusted()
.execute_rows_trusted()

.page(request)?
.execute()
.execute_trusted()
.admin_batch(request)

.try_one()
.exists()
.not_exists()
.collect_complete()

.count_exact()
.min_exact()
.min_exact_by(field)
.max_exact()
.max_exact_by(field)
.sum_exact(field)
.avg_exact(field)
```

Feature-gated diagnostics helpers:

```rust
.exists_with_attribution()
.collect_complete_with_attribution()
.count_exact_with_attribution()
```

## Paging Requests

`PageRequest` is for caller-facing cursor pages. IcyDB clamps requested page
sizes to the public page cap before admission and execution.

```rust
PageRequest::new()
PageRequest::first(limit)
PageRequest::next(limit, cursor)

request.with_limit(limit)
request.with_cursor(cursor)
request.limit()
request.cursor()
```

`AdminBatchRequest` is for trusted maintenance scans. The batch size is
engine-owned; callers may only provide continuation cursors.

```rust
AdminBatchRequest::new()
AdminBatchRequest::next(cursor)

request.with_cursor(cursor)
request.cursor()
```

## Explain And Planning

These commands inspect a load or delete query without changing rows.

```rust
.query()
.plan_hash_hex()
.trace()
.planned()
.plan()
.explain()
```

Load queries also expose read-terminal explain commands:

```rust
.explain_exists()
.explain_not_exists()
.explain_count_exact()
.explain_min_exact()
.explain_min_exact_by(field)
.explain_max_exact()
.explain_max_exact_by(field)
.explain_sum_exact(field)
.explain_avg_exact(field)

.explain_execution()
.explain_execution_text()
.explain_execution_json()
.explain_execution_verbose()
```

Session-level query inspection:

```rust
db.trace_query(query)
```

## Delete Commands

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

.limit(n)
.returning_all()
.returning(fields)
.only()

.execute()
.is_empty()
.count()
.require_one()
.require_some()
```

`returning_all()` and `returning(fields)` switch to
`SessionDeleteReturningQuery`; that returning query keeps the same shape,
planning, and `limit(...)` commands, but `execute()` returns
`RowProjectionOutput`.

## Write Commands

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

## Structural Mutation

Structural mutation is the dynamic field-name write ingress. Build field
patches through the session so names resolve through the accepted schema.

```rust
StructuralPatch::new()

db.structural_patch::<E, _, _>(fields)
db.mutate_structural::<E>(key, patch, MutationMode::Insert)
db.mutate_structural::<E>(key, patch, MutationMode::Update)
db.mutate_structural::<E>(key, patch, MutationMode::Replace)
```

## Catalog And Storage

Catalog helpers read the accepted runtime schema/catalog state.

```rust
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

## Direct Query Execution

These commands execute prebuilt `Query<E>` values rather than a fluent load.

```rust
db.execute_query(query)
db.execute_query_trusted(query)
```

Feature-gated diagnostics helper:

```rust
db.execute_query_result_with_attribution(query)
```

## SQL Commands

SQL session commands are available with the `sql` feature. They are
trusted/admin surfaces unless wrapped by an application-owned policy.

```rust
db.execute_sql_query::<E>(sql)
db.execute_sql_update::<E>(sql)
db.execute_sql_ddl::<E>(sql)
```

Hidden generated/policy helpers:

```rust
db.execute_sql_query_with_perf_attribution::<E>(sql)
db.execute_sql_query_with_attribution::<E>(sql)

db.execute_validated_sql_public_primary_key_update::<E>(plan)
db.execute_sql_public_primary_key_update::<E>(sql)
db.execute_validated_sql_public_bounded_update::<E>(plan)
db.execute_sql_public_bounded_update::<E>(sql)

db.execute_validated_sql_public_primary_key_delete::<E>(plan)
db.execute_sql_public_primary_key_delete::<E>(sql)
db.execute_validated_sql_public_bounded_delete::<E>(plan)
db.execute_sql_public_bounded_delete::<E>(sql)
```

## Query Responses

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

Projection responses:

```rust
ProjectionResponse::from_core(core_projection)

projection.count()
projection.exists()
projection.rows()
projection.iter()
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

Write/mutation responses:

```rust
MutationResult::from_count(row_count)
MutationResult::from_entity(entity)
MutationResult::from_entities(entities)
MutationResult::from_core_batch(batch)

result.row_count()
result.count()
result.is_empty()
result.exists()
result.entity()
result.entities()
result.id()
result.ids()
```

Row projection payloads:

```rust
ProjectionRows::new(columns, rows, row_count)
projection.columns()
projection.rows()
projection.rendered_rows()
projection.row_count()
projection.into_columns_rows_and_count()

RowProjectionOutput::from_projection(entity, projection)
output.as_projection_rows()
output.rendered_rows()

render_output_value_text(value)
```

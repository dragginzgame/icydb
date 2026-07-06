# IcyDB Read Admission Contract

This document defines the operational lane contract for read execution
surfaces. Query semantics remain documented in `QUERY_CONTRACT.md`,
`QUERY_PRACTICE.md`, and `SQL_SUBSET.md`; this document answers which surfaces
may execute reads and which admission lane they use.

## Core Rule

Any production canister surface that executes caller-controlled read work must
make its lane explicit.

Ordinary typed/fluent read execution is bounded by default. The normal
`DbSession::execute_query`, `FluentLoadQuery::execute`, `execute_rows`,
cursor-paged `execute`, and fluent terminal execution methods use the built-in
default bounded-read policy. Trusted maintenance/admin code that has already
enforced caller authorization and its own resource policy must choose an
explicit `*_trusted` execution method or mark the fluent query with
`trusted_read_unchecked()` when it needs to bypass those default bounds.

The current lanes are:

- `PublicRead`: caller-facing bounded reads. These require finite returned-row
  and response-byte caps, reject unsafe full scans by default, reject non-zero
  `OFFSET`, and require explicit grouped budgets for grouped queries. Exact
  primary-key `by_id(...)` / `by_ids(...)` reads may use their selected
  key-count upper bound as the returned-row cap.
- `AdminAdHoc`: trusted/controller-gated operational reads. These may use the
  broad SQL query helper, but the endpoint must remain visibly controller
  gated and must not be mistaken for a public read surface.
- `DiagnosticExplain`: EXPLAIN-only diagnostics. This lane may parse, lower,
  plan, and evaluate admission, but it must not execute data rows.
- `DevTest`: local tests and harnesses only.

Estimates may be reported by diagnostics, but estimates do not authorize
`PublicRead` execution.

If `PublicRead` admission depends on a route-proven pushed or limit-stopped
read, runtime must not silently degrade to an unadmitted materialized or
post-access route. The fallback route must either be independently admitted by
the same policy, or execution must fail closed with the shared read-admission
diagnostic before doing the broader work.

## Read Surface Inventory

| Surface | Lane | Guard | Query execution authority |
| --- | --- | --- | --- |
| `DbSession::execute_sql_query::<E>` | `AdminAdHoc` by caller contract | caller-owned | Trusted single-entity SQL query helper. It is not public-safe by itself. |
| `DbSession::execute_query::<E>` / `FluentLoadQuery::execute` / `execute_rows` / terminal execution / paged `execute` | `PublicRead` default policy | built-in plus caller auth | Ordinary typed/fluent execution. It rejects unsafe full scans, non-zero offset, materialized sorts, missing row bounds, and grouped reads without query hard limits. Exact selected primary-key access supplies its own row bound. |
| `DbSession::execute_query_trusted::<E>` / `FluentLoadQuery::*_trusted` execution methods / `trusted_read_unchecked()` | trusted caller contract | caller-owned | Explicit bypass for maintenance/admin code with its own authorization and resource policy. It is not public-safe by itself. |
| generated `icydb_query` | `AdminAdHoc` | controller-gated | Generated SQL query endpoint. It uses the trusted perf-attributed SQL helper and remains admin-only. |
| generated `icydb_ddl` | not a read-admission lane | controller-gated | Schema mutation frontend, governed by DDL admission and schema authority. |
| generated `icydb_update` | not a read-admission lane | controller-gated | SQL write endpoint, governed by explicit write policy. |
| generated `icydb_schema` / `icydb_schema_check` | diagnostic/admin | controller-gated | Accepted-schema diagnostics, not row-query execution. |
| generated `icydb_snapshot` | diagnostic/admin | build-option gated | Storage report diagnostics, not row-query execution. |
| generated `icydb_metrics` / `icydb_metrics_extended` | diagnostic | build-option gated | Metrics diagnostics, not row-query execution. |

IcyDB does not generate non-controller public SQL read endpoints. A canister
must not expose caller-controlled SQL through `execute_sql_query`; that helper
is a trusted/admin lane. Generated `icydb.toml` SQL settings intentionally have
no `sql.public_read` key.

## Which API should I use?

For migration examples and endpoint-intent guidance, see
[`docs/guides/read-intent.md`](../guides/read-intent.md).

| You want to... | Use | Notes |
| --- | --- | --- |
| serve normal users | ordinary typed/fluent execution | Default bounded admission rejects unsafe public read shapes before row execution. |
| check whether any row exists | `exists()` / `not_exists()` without a raw `limit(...)` | Existence is a semantic terminal. It owns its bounded route and rejects a prior raw row cap as caller-intent ambiguity. |
| return every row in a small bounded set | `collect_complete()` without a raw `limit(...)` | Complete small-set collection owns an internal lookahead limit and fails instead of silently truncating when the set exceeds the public-read cap. |
| return an exact count or sum | `count_exact()` / `sum_exact(field)` without a raw `limit(...)` | Exact aggregates use aggregate execution over the admitted shape. Use `count()` / `sum_by(...)` only when aggregating the effective row window is the intended contract. |
| process a trusted maintenance batch | `trusted_read_unchecked().admin_batch(AdminBatchRequest::...)` | Admin batches are trusted-only, cursor-batched, and use an engine-owned batch size. They are not public list shortcuts. |
| run controller diagnostics | trusted/admin execution | Caller authorization and an explicit resource policy are required before calling trusted helpers. |
| explain why a query fails | EXPLAIN or admission diagnostics | Diagnostics describe planning/admission; they do not bypass recovery or authorize execution. |
| paginate public results | cursor-paged ordinary execution | Prefer cursor pagination; non-zero `OFFSET` is rejected by the public lane. |
| run a broad maintenance scan | trusted execution | Keep it controller/admin-only and apply a maintenance resource policy. |
| expose arbitrary SQL publicly | do not | Generated SQL remains controller-gated; caller-facing SQL must be application-owned and tightly allowlisted. |

## Generated SQL Query Surface

The generated `icydb_query` endpoint is deliberately not a public read lane.

Required properties:

- it must call `icydb_sql_surface_require_controller("query")` before
  dispatch;
- it may use `execute_sql_query_with_perf_attribution` as the trusted
  controller/admin helper;
- it must not silently become a `PublicRead` endpoint;
- introspection remains separately controlled by generated SQL surface flags;
- adding any non-controller generated SQL query endpoint is outside the current
  generated-surface contract.

## Public Endpoint Guidance

Public endpoints should prefer typed or fluent APIs where the query shape is
known to the canister author. Ordinary typed/fluent execution is bounded by
default, so a full-scan query that accidentally reaches `execute_query()`,
`execute()`, `execute_rows()`, cursor-paged `execute()`, or a fluent terminal
returns the shared read-admission error before row execution. Endpoints must
still enforce caller authorization before entering IcyDB and any final
application-level response-byte budget after shaping the typed response.

Example default behavior:

```rust
// Rejected before row execution when `age` is not route-proven by an index.
let err = db()
    .load::<User>()
    .order_term(icydb::asc("age"))
    .limit(1)
    .execute_rows();

// Admitted when the selected route is index-backed and the result is bounded.
let users = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .limit(10)
    .execute_rows()?;

// Also admitted: exact selected primary-key access proves at most one row, so
// a redundant LIMIT is not required.
let user = db()
    .load::<User>()
    .by_id(icydb::Id::<User>::from_key(user_id))
    .try_one()?;

let exists = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").eq("sam"))
    .exists()?;

let exact_count = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("id").eq(user_id))
    .count_exact()?;

let exact_sum = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("id").eq(user_id))
    .sum_exact("age")?;

let small_users = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .collect_complete()?;
```

If the rejected query is intentional maintenance work, keep it off arbitrary
caller paths and use an explicit trusted API after caller authorization:

```rust
let users = db()
    .load::<User>()
    .order_term(icydb::asc("age"))
    .limit(1)
    .trusted_read_unchecked()
    .execute_rows()?;
```

The default typed/fluent policy is intentionally conservative:

- maximum returned rows: 100;
- maximum plan-level response bytes: 128 KiB where the surface can prove it;
- full scans are rejected;
- an index-backed access proof is required;
- selected primary-key `by_id(...)` / `by_ids(...)` access may satisfy the
  returned-row bound from the exact key count;
- non-zero `OFFSET` is rejected;
- materialized sorts are rejected;
- grouped reads require query-owned `grouped_limits(...)` and must fit within
  100 groups, 64 KiB per group, and 1024 distinct entries.

Use the `*_trusted` execution methods, or `trusted_read_unchecked()` for a
terminal chain, only for controller/admin paths or maintenance code that has
its own bounded execution policy. Do not expose trusted execution directly to
arbitrary callers.

Typed/fluent grouped reads need two explicit budgets before they are suitable
for `PublicRead` admission:

- the query shape must carry grouped execution hard limits through
  `grouped_limits(max_groups, max_group_bytes)`;
- the query must fit the built-in grouped read policy, including the
  distinct-entry budget for grouped aggregates that use `DISTINCT`.

Example grouped public read:

```rust
let groups = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .group_by("status")?
    .aggregate(icydb::count())
    .grouped_limits(100, 64 * 1024)
    .execute()?
    .into_grouped()?;
```

## Common Rejections And Fixes

Read-admission errors keep a stable diagnostic identity through
`DiagnosticCode::QueryReadAdmission` plus a `QueryReadAdmissionCode` detail.
The human-facing message may become more helpful over time, but endpoint logic
should branch on the diagnostic detail, not rendered text.

0.198 read-intent errors use `DiagnosticCode::QueryIntent` when the query
builder shape is ambiguous before read admission runs. For example,
`limit(n).exists()` is rejected because `exists()` owns the bounded existence
route; use `exists()` without a raw row-window cap, or use `execute_rows()` when
the low-level bounded row window is the actual endpoint contract.
`limit(n).execute_paged(PageRequest::...)` is rejected because `PageRequest`
owns page size and cursor continuation. `limit(n).collect_complete()` is
rejected because complete reads must not silently cap or truncate the result.
`limit(n).count_exact()` is rejected for the same reason: exact aggregates must
not mean "aggregate the first N rows." `limit(n).sum_exact(field)` is rejected
on the same contract. Use `count()` or `sum_by(...)` only when the endpoint
deliberately aggregates the effective bounded row window. `admin_batch(...)`
requires `trusted_read_unchecked()` and rejects prior raw `limit(...)`; trusted
batch size is engine-owned.

| Query shape | Diagnostic detail | Typical fix |
| --- | --- | --- |
| Ordinary read without a finite row, exact selected primary-key access, or grouped bound | `QueryReadAdmissionCode::PublicQueryRequiresLimit` | Choose the endpoint's read intent first: use request-owned `PageRequest` paging for public lists, `collect_complete()` for complete small sets, `count_exact()` / `sum_exact(field)` for exact aggregates, strict exact primary-key equality / bounded primary-key `IN (...)` / `by_id(...)` / bounded `by_ids(...)` for exact key reads, or grouped `grouped_limits(...)` when the grouped shape itself supplies the bound. Keep raw `limit(...)` only for endpoints that deliberately return a bounded row window. |
| Ordinary read with `LIMIT 1` but no route-proven index access | `QueryReadAdmissionCode::UnboundedFullScanRejected` | Add an index for the filter/order, tighten the predicate, or move the broad scan behind a controller/admin trusted path. |
| Ordinary read whose selected route cannot prove an index-backed access path | `QueryReadAdmissionCode::PublicQueryRequiresIndex` | Add a matching index or change the query to use an indexed predicate/order. |
| Ordinary read whose selected plan cannot prove a scan bound | `QueryReadAdmissionCode::ScanBoundUnavailable` | Add a suitable index, tighten the predicate, or move the query behind a trusted admin endpoint. |
| Ordinary read whose proven scan bound exceeds the public budget | `QueryReadAdmissionCode::ScanBoundExceedsPolicy` | Tighten the predicate or lower the query bound so the proven scan fits the endpoint budget. |
| Ordinary read whose only scan bound is estimated | `QueryReadAdmissionCode::EstimatedOnlyBoundRejected` | Add a suitable index, tighten the predicate, or move the query behind a trusted admin endpoint. |
| Ordinary read using non-zero `OFFSET` | `QueryReadAdmissionCode::PublicQueryOffsetRejected` | Use cursor pagination instead of offset pagination. |
| Ordinary read ordered by a field the selected route cannot satisfy | `QueryReadAdmissionCode::SortRequiresMaterialization` | Order by the selected index order, add a suitable composite index, or keep the report trusted/admin-only. |
| Ordinary read whose materialized row bound exceeds the public budget | `QueryReadAdmissionCode::MaterializationExceedsBudget` | Reduce the materialized row bound or use an index-backed order that avoids materialization. |
| Ordinary read whose response may exceed the endpoint byte budget | `QueryReadAdmissionCode::ProjectionResponseMayExceedLimit` | Lower the row bound, return narrower projections, or split the read into smaller cursor-paged requests. |
| Ordinary read whose returned-row bound exceeds the public row budget | `QueryReadAdmissionCode::ReturnedRowBoundExceedsPolicy` | Lower `LIMIT` or split the query into smaller cursor-paged reads. |
| Ordinary exact primary-key `IN (...)` or typed `by_ids(...)` read whose key-list input work exceeds the public budget | `QueryReadAdmissionCode::PrimaryKeyInputExceedsPolicy` | Reduce the primary-key list or move the broad key-set read behind a trusted admin endpoint. |
| Grouped read without query-owned group and memory limits | `QueryReadAdmissionCode::GroupedQueryRequiresLimits` | Add `grouped_limits(max_groups, max_group_bytes)` and keep `DISTINCT` aggregate state inside policy. |
| Grouped read whose query-owned limits exceed the public policy | `QueryReadAdmissionCode::GroupedQueryExceedsBudget` | Lower `grouped_limits(...)`, reduce grouped `DISTINCT` state, or move the report behind a trusted/admin endpoint. |
| EXPLAIN or diagnostic lane asked to execute rows | `QueryReadAdmissionCode::DiagnosticLaneDoesNotExecute` | Use EXPLAIN for diagnostics only, then execute through an admitted ordinary or explicit trusted path. |
| Introspection requested through a lane that does not expose it | `QueryReadAdmissionCode::IntrospectionDisabledForLane` | Use a controller-gated diagnostic/admin endpoint for introspection. |
| Caller-controlled SQL sent to the trusted SQL helper | `QueryReadAdmissionCode::UnsupportedStatementForQueryLane` | Prefer typed/fluent reads or a tightly allowlisted application-owned SQL surface. |

## Copyable Rejection Examples

The snippets below use `User` and field names as placeholders. They are meant
to be copied into canister-owned code and adapted to the model's real indexed
fields. Ordinary examples intentionally stay on `execute()`, `execute_rows()`,
or `execute_paged(PageRequest::...)` so they exercise the default bounded
public-read lane.

### Missing returned-row bound

`QueryReadAdmissionCode::PublicQueryRequiresLimit`

```rust
let err = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .execute_rows();
```

Fix it by adding a finite row bound, or by using grouped execution with
explicit grouped budgets when the query is genuinely grouped. For exact
primary-key reads, strict primary-key filters and the explicit key APIs both
produce selected exact-key proofs when the accepted schema can prove the shape:

```rust
let users = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .limit(10)
    .execute_rows()?;

let user = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("id").eq(user_id))
    .try_one()?;

let same_user = db()
    .load::<User>()
    .by_id(icydb::Id::<User>::from_key(user_id))
    .try_one()?;
```

### Full scan, missing index proof, or missing scan bound

`QueryReadAdmissionCode::UnboundedFullScanRejected`
`QueryReadAdmissionCode::PublicQueryRequiresIndex`
`QueryReadAdmissionCode::ScanBoundUnavailable`
`QueryReadAdmissionCode::EstimatedOnlyBoundRejected`

```rust
let err = db()
    .load::<User>()
    .order_term(icydb::asc("age"))
    .limit(1)
    .execute_rows();
```

`LIMIT 1` bounds returned rows, not scanned rows. The public lane needs a
route-proven bounded access path.

```rust
let users = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .limit(10)
    .execute_rows()?;
```

If the broad scan is maintenance work, keep it controller/admin-only:

```rust
require_controller()?;

let users = db()
    .load::<User>()
    .order_term(icydb::asc("age"))
    .limit(1)
    .trusted_read_unchecked()
    .execute_rows()?;
```

### Proven scan bound above policy

`QueryReadAdmissionCode::ScanBoundExceedsPolicy`

```rust
let err = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("s"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .limit(1_000)
    .execute_rows();
```

Fix it by tightening the predicate, lowering the page size, or moving the
large report behind a trusted/admin endpoint:

```rust
let users = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .limit(25)
    .execute_rows()?;
```

### Non-zero offset

`QueryReadAdmissionCode::PublicQueryOffsetRejected`

```rust
let err = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .limit(10)
    .offset(10)
    .execute_rows();
```

Use cursor pagination for caller-facing pages:

```rust
let page = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .execute_paged(icydb::db::PageRequest::first(10))?;
```

### Materialized sort or materialization budget

`QueryReadAdmissionCode::SortRequiresMaterialization`
`QueryReadAdmissionCode::MaterializationExceedsBudget`

```rust
let err = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("active").eq(true))
    .filter(icydb::FieldRef::new("tier").eq("gold"))
    .order_term(icydb::asc("age"))
    .order_term(icydb::asc("id"))
    .limit(10)
    .execute_rows();
```

Fix it by ordering with the selected index route, adding a suitable composite
index, or keeping the report trusted/admin-only:

```rust
let users = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("tier").eq("gold"))
    .order_term(icydb::asc("tier"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .limit(10)
    .execute_rows()?;
```

### Response or returned-row budget

`QueryReadAdmissionCode::ProjectionResponseMayExceedLimit`
`QueryReadAdmissionCode::ReturnedRowBoundExceedsPolicy`
`QueryReadAdmissionCode::PrimaryKeyInputExceedsPolicy`

```rust
let err = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .limit(1_000)
    .execute_rows();
```

Fix it by reducing the row bound, using smaller cursor pages, or returning a
narrower application-shaped response after the admitted read:

For exact primary-key `IN (...)` and typed `by_ids(...)` reads, the returned-row
bound is still the deduplicated key count, but public read admission also caps
key-list input work before deduplication. Large key lists, duplicate-heavy key
lists, or large variable-width key payloads can reject with
`PrimaryKeyInputExceedsPolicy` even when the deduplicated returned-row count is
small.

```rust
let users = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .limit(25)
    .execute_rows()?;
```

### Missing grouped budgets

`QueryReadAdmissionCode::GroupedQueryRequiresLimits`

```rust
let err = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .group_by("status")?
    .aggregate(icydb::count())
    .execute();
```

Add query-owned grouped limits:

```rust
let groups = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .group_by("status")?
    .aggregate(icydb::count())
    .grouped_limits(100, 64 * 1024)
    .execute()?
    .into_grouped()?;
```

Grouped `DISTINCT` aggregates also need to fit the default distinct-entry
budget.

### Grouped budget above policy

`QueryReadAdmissionCode::GroupedQueryExceedsBudget`

```rust
let err = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .group_by("status")?
    .aggregate(icydb::count())
    .grouped_limits(10_000, 1024 * 1024)
    .execute();
```

Lower the grouped limits, reduce grouped `DISTINCT` state, or move the report
behind a trusted/admin endpoint:

```rust
let groups = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .group_by("status")?
    .aggregate(icydb::count())
    .grouped_limits(100, 64 * 1024)
    .execute()?
    .into_grouped()?;
```

### Diagnostic lane used as execution

`QueryReadAdmissionCode::DiagnosticLaneDoesNotExecute`

```rust
let explain = db()
    .load::<User>()
    .order_term(icydb::asc("age"))
    .limit(1)
    .explain_execution_verbose()?;
```

Use the explanation to fix the route; do not treat EXPLAIN as a row-execution
API. After adding a suitable index or changing the query shape, execute through
the ordinary public lane:

```rust
let users = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .limit(10)
    .execute_rows()?;
```

### Introspection from the wrong lane

`QueryReadAdmissionCode::IntrospectionDisabledForLane`

```rust
// Do not expose introspection through arbitrary public read handlers.
fn public_query(sql: String) -> Result<(), icydb::Error> {
    db().execute_sql_query::<User>(&sql)?;
    Ok(())
}
```

Keep introspection behind a controller-gated diagnostic/admin endpoint:

```rust
fn controller_describe_users() -> Result<(), icydb::Error> {
    require_controller()?;
    db().execute_sql_query::<User>("DESCRIBE User")?;
    Ok(())
}
```

### Unsupported caller-controlled SQL

`QueryReadAdmissionCode::UnsupportedStatementForQueryLane`

```rust
// Do not pass caller-controlled SQL into the trusted SQL helper.
fn public_query(sql: String) -> Result<(), icydb::Error> {
    db().execute_sql_query::<User>(&sql)?;
    Ok(())
}
```

Prefer a typed/fluent endpoint or a tightly allowlisted application-owned SQL
surface:

```rust
fn public_users_by_prefix(prefix: String) -> Result<Vec<User>, icydb::Error> {
    require_authenticated_user()?;

    Ok(db()
        .load::<User>()
        .filter(icydb::FieldRef::new("username").text_starts_with(prefix))
        .order_term(icydb::asc("username"))
        .order_term(icydb::asc("id"))
        .limit(25)
        .execute_rows()?
        .entities())
}

fn controller_sql(sql: String) -> Result<(), icydb::Error> {
    require_controller()?;
    db().execute_sql_query::<User>(&sql)?;
    Ok(())
}
```

## EXPLAIN Admission Diagnostics

EXPLAIN is the fastest way to see why a public read shape would fail, but it
does not authorize execution and does not bypass guarded recovery.

```rust
let explain = db()
    .load::<User>()
    .order_term(icydb::asc("age"))
    .limit(1)
    .explain_execution_verbose()?;
```

For an unindexed `age` order, the verbose admission block should point at a
full-scan or materialized-sort rejection. The production fix is to change the
query to an indexed, bounded shape:

```rust
let users = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .limit(10)
    .execute_rows()?;
```

If the shape is intentionally broad operational work, keep the actual
execution on an explicit trusted/admin path after controller authorization.

## Future Policy And Endpoint Work

Configurable public read policies are intentionally not part of the
application-facing facade. Reintroducing them requires a fresh design with
demonstrated user demand, examples that prevent accidental public full scans,
and a new hard-cut regression guard.

Generated public typed endpoints are also outside the current generated
endpoint contract. Adding them requires a separate endpoint-authority design
covering caller authorization, generated method visibility, response budgets,
and read-admission evidence. Generated SQL remains controller-gated.

If a public endpoint accepts caller-provided SQL, it must:

- reject anonymous callers and perform any application authorization before
  entering IcyDB;
- not pass that SQL to `execute_sql_query`;
- use an application-owned SQL parser/allowlist or a typed/fluent endpoint
  instead;
- keep generated SQL endpoints controller-gated.

## Regression Guard

The repository invariant checks keep the hard-cut public read contract from
quietly drifting. They fail if:

- public facade custom-policy helpers or builders reappear;
- generated SQL starts accepting a `sql.public_read` key;
- generated SQL query endpoints stop requiring the controller gate;
- generated SQL query glue constructs hidden public-read policies;
- public facade method docs stop naming the default bounded read-admission
  gate;
- grouped facade docs stop pointing grouped callers to
  `execute().into_grouped()`;
- top-level developer, facade, query-contract, or SQL-contract docs stop
  linking to this read-admission contract;
- downstream setup docs stop describing generated readonly SQL as
  controller-gated admin SQL;
- public SQL helper docs stop warning that caller-controlled SQL is not
  public-safe by itself;
- internal `QueryAdmissionRejection` variants stop matching public
  `QueryReadAdmissionCode` variants one-for-one;
- the documented default row, response-byte, group, group-byte, or distinct
  budgets drift from the source constants.

## Persisted Format

Read admission is a pre-execution runtime policy. It does not change marker,
journal, row, schema, index, cursor, fold watermark, or structural-value
persisted formats.

# IcyDB Read Admission Contract

This document defines the admission and trusted-bypass contract for read
execution surfaces. Query semantics remain documented in `QUERY_CONTRACT.md`,
`QUERY_PRACTICE.md`, and `SQL_SUBSET.md`; this document answers which surfaces
are policy-evaluated and which explicitly bypass that policy.

Row mutation is outside this contract. Unlike trusted read execution, write
execution has no accepted-schema bypass; see `WRITE_ADMISSION.md`.

## Core Rule

Any production canister surface that executes caller-controlled read work must
make its lane explicit.

Ordinary typed/fluent read execution is bounded by default. Normal endpoint
code should use `load::<E>()` plus a semantic terminal such as `try_one()`,
`page(...)`, `collect_complete()`, `exists()`, or an exact aggregate helper.
Fluent `execute`, `execute_rows`, cursor-paged `execute`, and fluent terminal
execution methods use the built-in default bounded-read policy. Trusted
maintenance/admin fluent code that has already enforced caller authorization
and its own resource policy must mark the query with
`trusted_read_unchecked()` before executing the normal fluent terminal.

The current evaluated admission lanes are:

- `PublicRead`: caller-facing bounded reads. These require finite returned-row
  caps, reject unsafe full scans by default, and require explicit grouped
  budgets for grouped queries. Exact primary-key `by_id(...)` / `by_ids(...)`
  reads may use their selected key-count upper bound as the returned-row cap.
  The fluent surface does not expose `OFFSET`; public continuation is
  cursor/keyset-based through `page(...)` and `next_page(...)`.
- `DiagnosticExplain`: EXPLAIN-only diagnostics. This lane may parse, lower,
  plan, and evaluate admission, but it must not execute data rows.

Trusted SQL and trusted fluent maintenance reads do not manufacture a policy
lane. They are explicit trusted bypass surfaces outside evaluated
read-admission policy and require caller-owned authorization and resource
control. Generated SQL retains its separate controller gate.

Estimates may be reported by diagnostics, but estimates do not authorize
`PublicRead` execution.

If `PublicRead` admission depends on a route-proven pushed or limit-stopped
read, runtime must not silently degrade to an unadmitted materialized or
post-access route. The fallback route must either be independently admitted by
the same policy, or execution must fail closed with the shared read-admission
diagnostic before doing the broader work.

## Persisted Row Admission

Read-policy admission and persisted-row admission are separate boundaries. A
row is readable only when its envelope carries a layout version inside the
accepted entity-local history window and its physical slot count exactly
matches that stamped layout. Slot count is validation evidence; it never
selects or infers the layout.

Fields introduced after the stamped version materialize only from their frozen
accepted historical fill. A current-layout row with a missing slot, an initial
field missing from any row, an unknown layout version, or a layout/slot-count
mismatch is corruption. Reads do not consult current insert defaults or
generated models to repair malformed rows, and any successful rewrite of a
historical row emits a complete current-layout row.

## Integrity Inspection Admission

Quick and Deep integrity inspection are explicit trusted diagnostic operations,
not query read-admission lanes. Both consume the same fingerprinted
`AcceptedInspectionPlan` compiled from verified accepted authority. The plan
owns current and admitted historical row-layout decoding, frozen historical
fills, exact accepted value catalogs, validated checks, active index
projections, and source-owned relation declarations. Integrity never
reconstructs those facts from generated models or current insert defaults.

Quick performs only bounded metadata/control work. Deep privately resumes raw
physical rows and active derived domains, but public callers provide only an
authorized job ID and receipt acknowledgement. Neither operation grants a
trusted query bypass, returns application rows, or repairs malformed state.

## Read Surface Inventory

| Surface | Admission or bypass | Guard | Query execution authority |
| --- | --- | --- | --- |
| `DbSession::execute_trusted_sql_query::<E>` | trusted bypass | caller-owned | Explicit trusted single-entity SQL query helper. It is not public-safe by itself. |
| `FluentLoadQuery::execute` / `execute_rows` / terminal execution / paged `execute` | `PublicRead` default policy | built-in plus caller auth | Ordinary typed/fluent execution. It rejects unsafe full scans, materialized sorts, missing row bounds, and grouped reads without query hard limits. Exact selected primary-key access supplies its own row bound; cursor/keyset methods own continuation. |
| `trusted_read_unchecked()` fluent lane | trusted caller contract | caller-owned | Explicit bypass for maintenance/admin fluent code with its own authorization and resource policy. It is not public-safe by itself. Fluent load queries use normal terminal names after entering the trusted lane. |
| generated `icydb_query` | trusted bypass | controller-gated | Generated SQL query endpoint. It uses the trusted perf-attributed SQL helper and remains admin-only. |
| generated `icydb_ddl` | not a read-admission lane | controller-gated | Schema mutation frontend, governed by DDL admission, accepted-schema authority, and the write-admission contract. |
| generated `icydb_update` | not a read-admission lane | controller-gated | SQL write endpoint, governed by write admission and explicit exposure policy. |
| generated `icydb_integrity` | diagnostic/admin | controller-gated | Opt-in Quick/Deep integrity frontend. It binds durable jobs to the caller and delegates to accepted-native integrity authority; it is not a row-query lane. |
| generated `icydb_schema` / `icydb_schema_check` | diagnostic/admin | controller-gated | Accepted-schema diagnostics, not row-query execution. |
| generated `icydb_snapshot` | diagnostic/admin | build-option gated | Storage report diagnostics, not row-query execution. |
| generated `icydb_metrics` / `icydb_metrics_extended` | diagnostic | build-option gated | Metrics diagnostics, not row-query execution. |

IcyDB does not generate non-controller public SQL read endpoints. A canister
must not expose caller-controlled SQL through `execute_trusted_sql_query`;
that helper explicitly bypasses public-read policy. Generated `icydb.toml` SQL
settings intentionally have no `sql.public_read` key.

## Which API should I use?

For migration examples and endpoint-intent guidance, see
[`docs/guides/read-intent.md`](../guides/read-intent.md).

| You want to... | Use | Notes |
| --- | --- | --- |
| serve normal users | ordinary typed/fluent execution | Default bounded admission rejects unsafe public read shapes before row execution. |
| check whether any row exists | `exists()` / `not_exists()` without `partial_window(...)` | Existence is a semantic terminal. It owns its bounded route and rejects a prior row-window cap as caller-intent ambiguity. |
| return every row in a small bounded set | `collect_complete()` without `partial_window(...)` | Complete small-set collection owns an internal lookahead limit and fails instead of silently truncating when the set exceeds the public-read cap. |
| return an exact aggregate | `count_exact()`, `sum_exact(field)`, `min_id_exact()`, `min_exact_by(field)`, `max_id_exact()`, `max_exact_by(field)`, or `avg_exact(field)` without `partial_window(...)` | Exact aggregates use aggregate execution over the admitted shape. Public partial-window aggregate aliases are not exposed. |
| process a trusted maintenance batch | `trusted_read_unchecked().admin_batch(AdminBatchRequest::...)` | Admin batches are trusted-only, cursor-batched, and use an engine-owned batch size. They are not public list shortcuts. |
| run controller diagnostics | generated/admin diagnostic surfaces | Caller authorization and an explicit resource policy are required before running broad diagnostics. |
| explain why a query fails | EXPLAIN or admission diagnostics | Diagnostics describe planning/admission; they do not bypass recovery or authorize execution. |
| paginate public results | cursor-paged ordinary execution | Use `page(...)` and `next_page(...)`; fluent `OFFSET` is intentionally not exposed. |
| run a broad maintenance scan | trusted execution | Keep it controller/admin-only and apply a maintenance resource policy. |
| expose arbitrary SQL publicly | do not | Generated SQL remains controller-gated; caller-facing SQL must be application-owned and tightly allowlisted. |

Diagnostics-only terminal attribution and paged responses expose
`ReadIntentKind` so perf and observability tools can distinguish bounded
row-window terminals from semantic read intents such as existence checks, exact
aggregates, complete small-set reads, public pages, and trusted admin batches.
This field is reporting metadata only; it does not configure admission or grant
access.

Semantic aggregate EXPLAIN helpers expose the same metadata for supported
read-intent terminals. `explain_exists()` reports `ExistenceCheck`, while
`explain_count_exact()`, `explain_sum_exact(field)`, `explain_min_id_exact()`,
`explain_min_exact_by(field)`, `explain_max_id_exact()`,
`explain_max_exact_by(field)`, and `explain_avg_exact(field)` report
`ExactAggregate`. Ordinary low-level aggregate explains remain `Unspecified`.

Public endpoint review checklist:

- caller authorization happens before the query enters IcyDB;
- the return type makes the promise visible: page, complete set, optional row,
  exact aggregate, or trusted admin batch;
- public list endpoints use `page(limit)` / `next_page(limit, cursor)` cursor pagination, not generated SQL wrappers or giant row-window caps;
- complete-result endpoints use `collect_complete()` and fail when too many
  rows exist instead of truncating;
- exact aggregate endpoints use semantic exact helpers such as
  `count_exact()`, `sum_exact(field)`, `min_id_exact()`,
  `min_exact_by(field)`, `max_id_exact()`, `max_exact_by(field)`, or
  `avg_exact(field)`;
- trusted maintenance scans are controller/admin-gated and use trusted read
  helpers such as `trusted_read_unchecked().admin_batch(...)`.

## Generated SQL Query Surface

The generated `icydb_query` endpoint is deliberately not a public read lane.
Generated SQL endpoints are controller-gated admin surfaces, not public
list/count/complete endpoint templates. Hand-written public read endpoint templates
live in [`docs/guides/read-intent.md`](../guides/read-intent.md).

Required properties:

- it must call `icydb_sql_surface_require_controller("query")` before
  dispatch;
- it may use `execute_trusted_sql_query_with_perf_attribution` as the trusted
  controller/admin helper;
- it must not silently become a `PublicRead` endpoint;
- introspection remains separately controlled by generated SQL surface flags;
- adding any non-controller generated SQL query endpoint is outside the current
  generated-surface contract.

## Public Endpoint Guidance

Public endpoints should prefer typed or fluent APIs where the query shape is
known to the canister author. Ordinary typed/fluent execution is bounded by
default, so a full-scan query that accidentally reaches `execute()`,
`execute_rows()`, cursor-paged `execute()`, or a fluent terminal returns the
shared read-admission error before row execution. Endpoints must still enforce
caller authorization before entering IcyDB and any final application-level
response-byte budget after shaping the typed response.

Example default behavior:

```rust
// Rejected before row execution when `age` is not route-proven by an index.
let err = db()?
    .load::<User>()
    .order_term(icydb::asc("age"))
    .partial_window(1)
    .execute_rows();

// Admitted as a public cursor page when the selected route is index-backed.
let users_page = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .page(10)?;

// Also admitted: exact selected primary-key access proves at most one row, so
// a redundant LIMIT is not required.
let user = db()?
    .load::<User>()
    .by_id(icydb::Id::<User>::from_key(user_id))
    .try_one()?;

let exists = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("username").eq("sam"))
    .exists()?;

let exact_count = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("id").eq(user_id))
    .count_exact()?;

let exact_sum = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("id").eq(user_id))
    .sum_exact("age")?;

let small_users = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .collect_complete()?;
```

If the rejected query is intentional maintenance work, keep it off arbitrary
caller paths and use an explicit trusted API after caller authorization:

```rust
let users = db()?
    .load::<User>()
    .order_term(icydb::asc("age"))
    .partial_window(1)
    .trusted_read_unchecked()
    .execute_rows()?;
```

The default typed/fluent policy is intentionally conservative:

- maximum returned rows: 100;
- full scans are rejected;
- an index-backed access proof is required;
- selected primary-key `by_id(...)` / `by_ids(...)` access may satisfy the
  returned-row bound from the exact key count;
- materialized sorts are rejected;
- grouped reads require query-owned `grouped_limits(...)` and must fit within
  100 groups, 64 KiB per group, and 1024 distinct entries.

Use `trusted_read_unchecked()` only for controller/admin paths or maintenance
code that has its own bounded execution policy. Do not expose trusted execution
directly to arbitrary callers.

Typed/fluent grouped reads need two explicit budgets before they are suitable
for `PublicRead` admission:

- the query shape must carry grouped execution hard limits through
  `grouped_limits(max_groups, max_group_bytes)`;
- the query must fit the built-in grouped read policy, including the
  distinct-entry budget for grouped aggregates that use `DISTINCT`.

Example grouped public read:

```rust
let groups = db()?
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
builder shape is ambiguous before read admission runs. The public facade now
types `partial_window(n)` as `PartialWindowLoadQuery`, so semantic terminals
such as `exists()`, `page(...)`, `collect_complete()`, and exact aggregate
helpers are not reachable after selecting a partial row-window intent. Core and
direct-query equivalents still fail closed if a raw row-window cap is combined
with terminals that own their own bounds. `admin_batch(...)` requires
`trusted_read_unchecked()` and rejects prior `partial_window(...)`; trusted
batch size is engine-owned.

| Query shape | Diagnostic detail | Typical fix |
| --- | --- | --- |
| Ordinary read without a finite row, exact selected primary-key access, or grouped bound | `QueryReadAdmissionCode::PublicQueryRequiresLimit` | Choose the endpoint's read intent first: use `page(limit)` / `next_page(limit, cursor)` for public lists, `collect_complete()` for complete small sets, semantic `*_exact` helpers for exact aggregates, strict exact primary-key equality / bounded primary-key `IN (...)` / `by_id(...)` / bounded `by_ids(...)` for exact key reads, or grouped `grouped_limits(...)` when the grouped shape itself supplies the bound. Use `partial_window(...)` only for endpoints that deliberately return a partial row window. |
| Ordinary read with `LIMIT 1` but no route-proven index access | `QueryReadAdmissionCode::UnboundedFullScanRejected` | Add an index for the filter/order, tighten the predicate, or move the broad scan behind a controller/admin trusted path. |
| Ordinary read whose selected route cannot prove an index-backed access path | `QueryReadAdmissionCode::PublicQueryRequiresIndex` | Add a matching index or change the query to use an indexed predicate/order. |
| Ordinary read ordered by a field the selected route cannot satisfy | `QueryReadAdmissionCode::SortRequiresMaterialization` | Order by the selected index order, add a suitable composite index, or keep the report trusted/admin-only. |
| Ordinary read whose returned-row bound exceeds the public row budget | `QueryReadAdmissionCode::ReturnedRowBoundExceedsPolicy` | Lower `LIMIT` or split the query into smaller cursor-paged reads. |
| Ordinary exact primary-key `IN (...)` or typed `by_ids(...)` read whose key-list input work exceeds the public budget | `QueryReadAdmissionCode::PrimaryKeyInputExceedsPolicy` | Reduce the primary-key list or move the broad key-set read behind a trusted admin endpoint. |
| Grouped read without query-owned group and memory limits | `QueryReadAdmissionCode::GroupedQueryRequiresLimits` | Add `grouped_limits(max_groups, max_group_bytes)` and keep `DISTINCT` aggregate state inside policy. |
| Grouped read whose query-owned limits exceed the public policy | `QueryReadAdmissionCode::GroupedQueryExceedsBudget` | Lower `grouped_limits(...)`, reduce grouped `DISTINCT` state, or move the report behind a trusted/admin endpoint. |
| EXPLAIN or diagnostic lane asked to execute rows | `QueryReadAdmissionCode::DiagnosticLaneDoesNotExecute` | Use EXPLAIN for diagnostics only, then execute through an admitted ordinary or explicit trusted path. |

## Copyable Rejection Examples

The snippets below use `User` and field names as placeholders. They are meant
to be copied into canister-owned code and adapted to the model's real indexed
fields. Ordinary examples intentionally stay on `execute()`, `execute_rows()`,
or `page(limit)` / `next_page(limit, cursor)` so they exercise the default
bounded public-read lane.

### Missing returned-row bound

`QueryReadAdmissionCode::PublicQueryRequiresLimit`

```rust
let err = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .execute_rows();
```

Fix it by choosing the endpoint intent, or by using grouped execution with
explicit grouped budgets when the query is genuinely grouped. For exact
primary-key reads, strict primary-key filters and the explicit key APIs both
produce selected exact-key proofs when the accepted schema can prove the shape:

```rust
let users_page = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .page(10)?;

let user = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("id").eq(user_id))
    .try_one()?;

let same_user = db()?
    .load::<User>()
    .by_id(icydb::Id::<User>::from_key(user_id))
    .try_one()?;
```

### Full scan or missing index proof

`QueryReadAdmissionCode::UnboundedFullScanRejected`
`QueryReadAdmissionCode::PublicQueryRequiresIndex`

```rust
let err = db()?
    .load::<User>()
    .order_term(icydb::asc("age"))
    .partial_window(1)
    .execute_rows();
```

`LIMIT 1` bounds returned rows, not scanned rows. The public lane needs a
route-proven bounded access path.

```rust
let users_page = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .page(10)?;
```

If the broad scan is maintenance work, keep it controller/admin-only:

```rust
require_controller()?;

let users = db()?
    .load::<User>()
    .order_term(icydb::asc("age"))
    .partial_window(1)
    .trusted_read_unchecked()
    .execute_rows()?;
```

### Materialized sort

`QueryReadAdmissionCode::SortRequiresMaterialization`

```rust
let err = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("active").eq(true))
    .filter(icydb::FieldRef::new("tier").eq("gold"))
    .order_term(icydb::asc("age"))
    .order_term(icydb::asc("id"))
    .partial_window(10)
    .execute_rows();
```

Fix it by ordering with the selected index route, adding a suitable composite
index, or keeping the report trusted/admin-only:

```rust
let users_page = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("tier").eq("gold"))
    .order_term(icydb::asc("tier"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .page(10)?;
```

### Returned-row or primary-key input budget

`QueryReadAdmissionCode::ReturnedRowBoundExceedsPolicy`
`QueryReadAdmissionCode::PrimaryKeyInputExceedsPolicy`

```rust
let err = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .partial_window(1_000)
    .execute_rows();
```

Fix it by reducing the row bound or using smaller cursor pages:

For exact primary-key `IN (...)` and typed `by_ids(...)` reads, the returned-row
bound is still the deduplicated key count, but public read admission also caps
key-list input work before deduplication. Large key lists, duplicate-heavy key
lists, or large variable-width key payloads can reject with
`PrimaryKeyInputExceedsPolicy` even when the deduplicated returned-row count is
small.

```rust
let users_page = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .page(25)?;
```

### Missing grouped budgets

`QueryReadAdmissionCode::GroupedQueryRequiresLimits`

```rust
let err = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .group_by("status")?
    .aggregate(icydb::count())
    .execute();
```

Add query-owned grouped limits:

```rust
let groups = db()?
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
let err = db()?
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
let groups = db()?
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
let explain = db()?
    .load::<User>()
    .order_term(icydb::asc("age"))
    .partial_window(1)
    .explain_execution_verbose()?;
```

Use the explanation to fix the route; do not treat EXPLAIN as a row-execution
API. After adding a suitable index or changing the query shape, execute through
the ordinary public lane:

```rust
let users_page = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .page(10)?;
```

## EXPLAIN Admission Diagnostics

EXPLAIN is the fastest way to see why a public read shape would fail, but it
does not authorize execution and does not bypass guarded recovery.

```rust
let explain = db()?
    .load::<User>()
    .order_term(icydb::asc("age"))
    .partial_window(1)
    .explain_execution_verbose()?;
```

For an unindexed `age` order, the verbose admission block should point at a
full-scan or materialized-sort rejection. The production fix is to change the
query to an indexed, bounded shape:

```rust
let users_page = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with("sam"))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .page(10)?;
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
- not pass that SQL to `execute_trusted_sql_query`;
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
- the documented default row, group, group-byte, or distinct
  budgets drift from the source constants.

## Persisted Format

Read admission is a pre-execution runtime policy. It does not change marker,
journal, row, schema, index, cursor, fold watermark, or structural-value
persisted formats.

# Read Intent Guide

IcyDB public reads should describe the endpoint promise, not the arbitrary row
cap that happens to satisfy admission.

Use semantic terminals when the endpoint wants a semantic answer. Use
`partial_window(...)` only when the endpoint is deliberately returning a
partial row window.

## Current Map

| Endpoint promise | Use | Do not use |
| --- | --- | --- |
| One exact row | `by_id(...).try_one()` or canonicalized primary-key equality with `try_one()` | `partial_window(1).execute_rows()` |
| Existence | `exists()` / `not_exists()` | `partial_window(1).execute_rows()?.is_empty()` |
| Exact count | `count_exact()` | partial-window row materialization plus `Response::count()` |
| Exact sum/min/max/average | `sum_exact(field)`, `min_id_exact()`, `min_exact_by(field)`, `max_id_exact()`, `max_exact_by(field)`, `avg_exact(field)` | aggregating a partial row window |
| Complete small set | `collect_complete()` | `partial_window(N).execute_rows()` when the endpoint promises all matches |
| Partial row window | `partial_window(N).execute_rows()` / `partial_window(N).execute()` | A complete-result API that silently truncates |
| First cursor page | `order_term(...).page(N)?` | partial windows or SQL `OFFSET` for public pages |
| Next cursor page | `order_term(...).next_page(N, cursor)?` | optional cursor arguments or SQL `OFFSET` for public pages |
| Trusted maintenance batch | `trusted_read_unchecked().admin_batch(AdminBatchRequest::new())` | Public endpoints with giant limits or caller-selected batch sizes |
| Trusted maintenance scan | `trusted_read_unchecked().execute_rows()` or `trusted_read_unchecked().admin_batch(...)` | Public endpoints with giant limits |

## When Admission Rejects A Read

`PublicQueryRequiresLimit` does not mean "pick a bigger number." It means the
public endpoint has not supplied a bounded intent that IcyDB can admit.

Classify the endpoint promise before changing code:

- public list: use `page(limit)` for the first cursor page and
  `next_page(limit, cursor)` for continuation;
- complete result: use `collect_complete()` only when the set is expected to
  stay small;
- exact aggregate: use `count_exact()`, `sum_exact(field)`,
  `min_id_exact()`, `min_exact_by(field)`, `max_id_exact()`,
  `max_exact_by(field)`, or `avg_exact(field)`;
- exact key read: use `by_id(...)`, `by_ids(...)`, or canonicalized
  primary-key equality;
- partial row window: use `partial_window(N).execute_rows()` only when a
  partial row window is the actual API contract;
- trusted maintenance: keep the endpoint controller/admin-gated and use
  `trusted_read_unchecked().admin_batch(...)`.

## Migration Recipes

Treat each migration as an endpoint-contract review, not a search-and-replace.
The same partial row-window shape can mean several different things.

Exact lookup:

```rust
// Before: partial row window spelling.
let user = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("id").eq(user_id))
    .partial_window(1)
    .execute_rows()?
    .try_entity()?;

// After: exact-key spelling.
let user = db()?
    .load::<User>()
    .by_id(icydb::Id::<User>::from_key(user_id))
    .try_one()?;
```

Public list:

```rust
// Before: a row cap that can look like a complete list.
let users = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("country").eq(country))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .partial_window(100)
    .execute_rows()?;

// After: public cursor page.
let users = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("country").eq(country))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .page(25)?;
```

Complete small set:

```rust
// Before: silent truncation if there are more than N matching rows.
let members = db()?
    .load::<Member>()
    .filter(icydb::FieldRef::new("team_id").eq(team_id))
    .order_term(icydb::asc("id"))
    .partial_window(100)
    .execute_rows()?;

// After: complete or fail with TooManyRows.
let members = db()?
    .load::<Member>()
    .filter(icydb::FieldRef::new("team_id").eq(team_id))
    .order_term(icydb::asc("id"))
    .collect_complete()?;
```

Exact aggregate:

```rust
// Anti-pattern: row count of a partial bounded window.
let active = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("active").eq(true))
    .partial_window(100)
    .execute_rows()?
    .count();

// Preferred: exact count over the admitted shape.
let active = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("active").eq(true))
    .count_exact()?;
```

Trusted maintenance:

```rust
// Before: easy to copy into a public endpoint by accident.
let rows = db()?
    .load::<LedgerEntry>()
    .order_term(icydb::asc("id"))
    .partial_window(500)
    .execute_rows()?;

// After: visibly trusted and cursor-batched.
let rows = db()?
    .load::<LedgerEntry>()
    .order_term(icydb::asc("id"))
    .trusted_read_unchecked()
    .admin_batch(icydb::db::AdminBatchRequest::new())?;
```

## Generated SQL Boundary

Generated SQL endpoints are controller-gated operational infrastructure. They
are useful for diagnostics, local maintenance, DDL, fixture flows, and
administrator-owned SQL. They are not the public read abstraction for
application users.

Do not expose `icydb_query` or a wrapper around it to arbitrary callers.
Also do not:

- add a generated public SQL config key;
- use generated readonly SQL as the implementation of a public list endpoint;
- treat SQL `LIMIT` as the public endpoint contract when the endpoint promises
  a complete set, exact aggregate, or cursor page.

Do:

- keep generated SQL behind controller/admin authorization;
- use typed/fluent public endpoints for caller-facing exact rows, pages,
  complete small sets, and exact aggregates;
- use application-owned SQL allowlists only when the canister author controls
  the query shape and still applies caller authorization first;
- document whether a public endpoint returns a page, a complete set, an exact
  aggregate, an exact key, or a trusted admin batch.

## Public Endpoint Templates

Generated SQL endpoints are not substitutes for hand-written public read endpoints.
`icydb_query` is a controller-gated admin surface. For caller-facing queries,
write typed/fluent endpoints that state the endpoint promise directly.

Exact row:

```rust
#[ic_cdk::query]
fn get_user(user_id: UserKey) -> Result<Option<User>, icydb::Error> {
    db()?
        .load::<User>()
        .by_id(icydb::Id::<User>::from_key(user_id))
        .try_one()
}
```

Public page:

```rust
#[ic_cdk::query]
fn list_users(prefix: String, cursor: Option<String>) -> Result<icydb::db::PagedResponse<User>, icydb::Error> {
    let query = db()?
        .load::<User>()
        .filter(icydb::FieldRef::new("username").text_starts_with(prefix))
        .order_term(icydb::asc("username"))
        .order_term(icydb::asc("id"));

    match cursor {
        Some(cursor) => query.next_page(25, cursor),
        None => query.page(25),
    }
}
```

Complete small set:

```rust
#[ic_cdk::query]
fn matching_users(prefix: String) -> Result<Vec<User>, icydb::Error> {
    db()?
        .load::<User>()
        .filter(icydb::FieldRef::new("username").text_starts_with(prefix))
        .order_term(icydb::asc("username"))
        .order_term(icydb::asc("id"))
        .collect_complete()
}
```

Exact aggregate:

```rust
#[ic_cdk::query]
fn count_users(country: String) -> Result<u32, icydb::Error> {
    db()?
        .load::<User>()
        .filter(icydb::FieldRef::new("country").eq(country))
        .count_exact()
}
```

Trusted maintenance batch:

```rust
#[ic_cdk::query]
fn admin_user_batch(cursor: Option<String>) -> Result<icydb::db::PagedResponse<User>, icydb::Error> {
    ensure_controller()?;

    let request = cursor.map_or_else(
        icydb::db::AdminBatchRequest::new,
        icydb::db::AdminBatchRequest::next,
    );

    db()?
        .load::<User>()
        .order_term(icydb::asc("id"))
        .trusted_read_unchecked()
        .admin_batch(request)
}
```

These shapes are application endpoints. They are intentionally separate from
generated controller-gated SQL diagnostics and from any future codegen work.

## Exact Lookup

Exact lookup should be proved by key access, not by a partial row window.

```rust
let user = db()?
    .load::<User>()
    .by_id(icydb::Id::<User>::from_key(user_id))
    .try_one()?;
```

When accepted-schema primary-key canonicalization can prove the shape, strict
primary-key equality is also exact-key access:

```rust
let user = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("id").eq(user_id))
    .try_one()?;
```

Avoid:

```rust
let user = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("id").eq(user_id))
    .partial_window(1)
    .try_one()?;
```

The extra limit is not the proof. The selected exact-key route is the proof.

## Existence

Use `exists()` or `not_exists()` for boolean existence checks.

```rust
let exists = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("email").eq(email))
    .exists()?;
```

`exists()` owns the existence intent. A prior `partial_window(...)` is rejected
because it makes the caller contract ambiguous.

```rust
let err = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("email").eq(email))
    .partial_window(1)
    .exists();
```

Use `execute_rows()` only when returning a partial row window is the endpoint
contract.

Diagnostics-only terminal attribution reports `ReadIntentKind::ExistenceCheck`
for attributed existence terminals, which helps perf and observability tools
separate existence checks from low-level row-window reads.
`explain_exists()` reports the same read-intent metadata without executing the
terminal.

## Exact Aggregates

Use exact aggregate terminals when the endpoint promises an exact answer.

```rust
let count = db()?
    .load::<Token>()
    .filter(icydb::FieldRef::new("collection_id").eq(collection_id))
    .count_exact()?;

let total = db()?
    .load::<LedgerEntry>()
    .filter(icydb::FieldRef::new("account_id").eq(account_id))
    .sum_exact("amount")?;

let oldest = db()?
    .load::<LedgerEntry>()
    .filter(icydb::FieldRef::new("account_id").eq(account_id))
    .min_exact_by("created_at")?;

let average = db()?
    .load::<LedgerEntry>()
    .filter(icydb::FieldRef::new("account_id").eq(account_id))
    .avg_exact("amount")?;
```

Exact aggregate terminals reject prior `partial_window(...)`. Exact aggregates
must not mean "aggregate the first N rows."

Diagnostics-only terminal attribution reports `ReadIntentKind::ExactAggregate`
for attributed exact aggregate terminals.
`explain_count_exact()`, `explain_sum_exact(field)`, `explain_min_id_exact()`,
`explain_min_exact_by(field)`, `explain_max_id_exact()`,
`explain_max_exact_by(field)`, and `explain_avg_exact(field)` report
exact-aggregate read-intent metadata without executing the terminal.

## Public Pages

Use `page(limit)` for the first public page and `next_page(limit, cursor)` for
continuation. `partial_window(...)` is rejected before page terminals because
page size belongs to the cursor-page intent, not to the low-level row-window
modifier.

```rust
let page = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with(prefix))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .page(25)?;
```

For public endpoints:

- use deterministic ordering;
- avoid SQL `OFFSET` for caller-facing pages;
- keep page sizes small;
- expose continuation cursors instead of pretending the result is complete.

Continue with the cursor through the next request:

```rust
let page = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with(prefix))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .next_page(25, cursor)?;
```

## Complete Small Sets

Do not use `partial_window(N).execute_rows()` for endpoints that claim to return
all matching rows. That returns a partial window, not a complete set.

Use `collect_complete()` when the endpoint promises every matching row and the
complete result must fit under the default public-read small-set cap.

```rust
let users = db()?
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with(prefix))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .collect_complete()?;
```

`collect_complete()` rejects a prior `partial_window(...)`. It internally executes
with one lookahead row so it can fail when the set is too large instead of
silently truncating the response.

Diagnostics-only complete-read attribution reports
`ReadIntentKind::CompleteSmallSet`. The value is observability metadata only
and does not change admission or row results.

If the set is not known to be small, choose one of:

- exact key reads for exact lookup;
- `exists()` for boolean existence;
- `count_exact()`, `sum_exact(field)`, `min_id_exact()`, `min_exact_by(field)`,
  `max_id_exact()`, `max_exact_by(field)`, or `avg_exact(field)` for supported
  exact aggregates;
- cursor paging for public list endpoints;
- explicit trusted maintenance reads for controller/admin-only workflows.

## Trusted Reads

`trusted_read_unchecked()` is an explicit bypass for controller/admin or
maintenance code that owns its own authorization and resource policy.

```rust
let rows = db()?
    .load::<LedgerEntry>()
    .order_term(icydb::asc("id"))
    .partial_window(100)
    .trusted_read_unchecked()
    .execute_rows()?;
```

Use `admin_batch(...)` when trusted maintenance code needs cursor-batched
processing with an engine-owned batch size:

```rust
let batch = db()?
    .load::<LedgerEntry>()
    .order_term(icydb::asc("id"))
    .trusted_read_unchecked()
    .admin_batch(icydb::db::AdminBatchRequest::new())?;
```

Continue the batch with the returned cursor:

```rust
let batch = db()?
    .load::<LedgerEntry>()
    .order_term(icydb::asc("id"))
    .trusted_read_unchecked()
    .admin_batch(icydb::db::AdminBatchRequest::next(cursor))?;
```

`admin_batch(...)` rejects prior `partial_window(...)`; IcyDB owns the batch size.
It also rejects calls that did not first opt into `trusted_read_unchecked()`.

Paged responses report diagnostic read-intent metadata. Public page terminals
return `ReadIntentKind::PublicPage`; trusted admin batches return
`ReadIntentKind::TrustedAdminBatch`. The value is observability metadata only
and does not change admission, cursor encoding, or execution semantics.

Do not use trusted reads to make arbitrary public endpoints pass admission.

## Migration Checklist

For every raw high-limit call site, classify intent first:

- exact lookup: use `by_id(...)`, `by_ids(...)`, or canonicalized primary-key
  equality with `try_one()`;
- existence: use `exists()` / `not_exists()`;
- exact count: use `count_exact()`;
- exact sum/min/max/average: use `sum_exact(field)`, `min_id_exact()`,
  `min_exact_by(field)`, `max_id_exact()`, `max_exact_by(field)`, or
  `avg_exact(field)`;
- public list: use deterministic cursor paging;
- complete small set: use `collect_complete()` or redesign the endpoint as a
  page;
- maintenance/admin batch: use `trusted_read_unchecked().admin_batch(...)`;
- maintenance/admin broad read: keep it trusted and controller-gated.

Do not mechanically replace every `partial_window(N).execute_rows()` with an exact or
complete terminal. The right terminal depends on the endpoint promise.

Endpoint review checklist:

- Does the function perform caller authorization before entering IcyDB?
- Does the return type expose whether the result is a page, a complete set, an
  optional row, an aggregate, or an admin batch?
- Does the query have deterministic ordering before cursor pagination or
  complete collection?
- Does a complete endpoint fail when too many rows exist instead of silently
  truncating?
- Does a broad maintenance endpoint stay controller/admin-gated and use a
  trusted read lane?
- Does generated SQL stay operational and controller-gated instead of becoming
  a public read gateway?

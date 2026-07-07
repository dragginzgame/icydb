# Read Intent Guide

IcyDB public reads should describe the endpoint promise, not the arbitrary row
cap that happens to satisfy admission.

Use semantic terminals when the endpoint wants a semantic answer. Use raw
`limit(...)` only when the endpoint is deliberately returning a bounded row
window.

## Current Map

| Endpoint promise | Use | Do not use |
| --- | --- | --- |
| One exact row | `by_id(...).try_one()` or canonicalized primary-key equality with `try_one()` | `limit(1).execute_rows()` |
| Existence | `exists()` / `not_exists()` | `limit(1).execute_rows()?.is_empty()` or `limit(1).exists()` |
| Exact count | `count_exact()` | `limit(N).count()` unless the limited window is intended |
| Exact sum | `sum_exact(field)` | `limit(N).sum_by(field)` unless the limited window is intended |
| Complete small set | `collect_complete()` | `limit(N).execute_rows()` when the endpoint promises all matches |
| Bounded row window | `limit(N).execute_rows()` / `limit(N).execute()` | A complete-result API that silently truncates |
| Cursor page | `order_term(...).execute_paged(PageRequest::first(N))` | `limit(N).execute_paged(...)` or non-zero `offset(...)` for public pages |
| Trusted maintenance batch | `trusted_read_unchecked().admin_batch(AdminBatchRequest::new())` | Public endpoints with giant limits or caller-selected batch sizes |
| Trusted maintenance scan | `trusted_read_unchecked().execute_rows()` or trusted execution helpers | Public endpoints with giant limits |

## When Admission Rejects A Read

`PublicQueryRequiresLimit` does not mean "pick a bigger number." It means the
public endpoint has not supplied a bounded intent that IcyDB can admit.

Classify the endpoint promise before changing code:

- public list: use request-owned `PageRequest` cursor paging;
- complete result: use `collect_complete()` only when the set is expected to
  stay small;
- exact aggregate: use `count_exact()` or `sum_exact(field)`;
- exact key read: use `by_id(...)`, `by_ids(...)`, or canonicalized
  primary-key equality;
- bounded row window: keep `limit(N).execute_rows()` only when a partial row
  window is the actual API contract;
- trusted maintenance: keep the endpoint controller/admin-gated and use
  `trusted_read_unchecked().admin_batch(...)` or another trusted helper.

## Exact Lookup

Exact lookup should be proved by key access, not by a raw limit.

```rust
let user = db()
    .load::<User>()
    .by_id(icydb::Id::<User>::from_key(user_id))
    .try_one()?;
```

When accepted-schema primary-key canonicalization can prove the shape, strict
primary-key equality is also exact-key access:

```rust
let user = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("id").eq(user_id))
    .try_one()?;
```

Avoid:

```rust
let user = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("id").eq(user_id))
    .limit(1)
    .try_one()?;
```

The extra limit is not the proof. The selected exact-key route is the proof.

## Existence

Use `exists()` or `not_exists()` for boolean existence checks.

```rust
let exists = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("email").eq(email))
    .exists()?;
```

`exists()` owns the existence intent. A prior raw `limit(...)` is rejected
because it makes the caller contract ambiguous.

```rust
let err = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("email").eq(email))
    .limit(1)
    .exists();
```

Use `execute_rows()` only when returning a bounded row window is the endpoint
contract.

Diagnostics-only terminal attribution reports `ReadIntentKind::ExistenceCheck`
for attributed existence terminals, which helps perf and observability tools
separate existence checks from low-level row-window reads.

## Exact Aggregates

Use exact aggregate terminals when the endpoint promises an exact answer.

```rust
let count = db()
    .load::<Token>()
    .filter(icydb::FieldRef::new("collection_id").eq(collection_id))
    .count_exact()?;

let total = db()
    .load::<LedgerEntry>()
    .filter(icydb::FieldRef::new("account_id").eq(account_id))
    .sum_exact("amount")?;
```

`count_exact()` and `sum_exact(field)` reject prior raw `limit(...)`. Exact
aggregates must not mean "aggregate the first N rows."

Diagnostics-only terminal attribution reports `ReadIntentKind::ExactAggregate`
for attributed exact count terminals and `ReadIntentKind::BoundedRowWindow` for
the lower-level bounded count terminal.

Use the older aggregate terminals only when the window is explicitly part of
the endpoint promise:

```rust
let page_total = db()
    .load::<LedgerEntry>()
    .filter(icydb::FieldRef::new("account_id").eq(account_id))
    .order_term(icydb::asc("created_at"))
    .order_term(icydb::asc("id"))
    .limit(25)
    .sum_by("amount")?;
```

## Public Pages

Use `PageRequest` for public page size and cursor continuation. Raw
`limit(...)` is rejected before request-owned page terminals because page size
belongs to the request, not to the low-level row-window modifier.

```rust
let page = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with(prefix))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .execute_paged(icydb::db::PageRequest::first(25))?;
```

For public endpoints:

- use deterministic ordering;
- avoid non-zero `offset(...)`;
- keep page sizes small;
- expose continuation cursors instead of pretending the result is complete.

Continue with the cursor through the next request:

```rust
let page = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with(prefix))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .execute_paged(icydb::db::PageRequest::next(25, cursor))?;
```

## Complete Small Sets

Do not use `limit(N).execute_rows()` for endpoints that claim to return all
matching rows. That returns a bounded window, not a complete set.

Use `collect_complete()` when the endpoint promises every matching row and the
complete result must fit under the default public-read small-set cap.

```rust
let users = db()
    .load::<User>()
    .filter(icydb::FieldRef::new("username").text_starts_with(prefix))
    .order_term(icydb::asc("username"))
    .order_term(icydb::asc("id"))
    .collect_complete()?;
```

`collect_complete()` rejects a prior raw `limit(...)`. It internally executes
with one lookahead row so it can fail when the set is too large instead of
silently truncating the response.

If the set is not known to be small, choose one of:

- exact key reads for exact lookup;
- `exists()` for boolean existence;
- `count_exact()` or `sum_exact(field)` for supported exact aggregates;
- cursor paging for public list endpoints;
- explicit trusted maintenance reads for controller/admin-only workflows.

## Trusted Reads

`trusted_read_unchecked()` is an explicit bypass for controller/admin or
maintenance code that owns its own authorization and resource policy.

```rust
let rows = db()
    .load::<LedgerEntry>()
    .order_term(icydb::asc("id"))
    .limit(100)
    .trusted_read_unchecked()
    .execute_rows()?;
```

Use `admin_batch(...)` when trusted maintenance code needs cursor-batched
processing with an engine-owned batch size:

```rust
let batch = db()
    .load::<LedgerEntry>()
    .order_term(icydb::asc("id"))
    .trusted_read_unchecked()
    .admin_batch(icydb::db::AdminBatchRequest::new())?;
```

Continue the batch with the returned cursor:

```rust
let batch = db()
    .load::<LedgerEntry>()
    .order_term(icydb::asc("id"))
    .trusted_read_unchecked()
    .admin_batch(icydb::db::AdminBatchRequest::next(cursor))?;
```

`admin_batch(...)` rejects prior raw `limit(...)`; IcyDB owns the batch size.
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
- exact sum: use `sum_exact(field)`;
- public list: use deterministic cursor paging;
- complete small set: use `collect_complete()` or redesign the endpoint as a
  page;
- maintenance/admin batch: use `trusted_read_unchecked().admin_batch(...)`;
- maintenance/admin broad read: keep it trusted and controller-gated.

Do not mechanically replace every `limit(N).execute_rows()` with an exact or
complete terminal. The right terminal depends on the endpoint promise.

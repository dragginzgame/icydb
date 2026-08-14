# Read Intent Guide

IcyDB has two maintained read authorities:

- ordinary typed/dynamic reads use the built-in `PublicRead` admission policy;
- trusted dynamic or SQL reads are explicit admin lanes whose authorization
  belongs to the caller while the active request scope supplies finite IcyDB
  resource policy.

Generated Rust adapters never choose admission or execution semantics.

## Current Map

| Intent | Maintained surface |
| --- | --- |
| Typed live page | `db.query::<E>()?.execute_live_page(continuation)` |
| Dynamic live page | `execute_live_page(&request, continuation)` |
| Bounded typed grouped page | `db.query::<E>()?...execute_grouped()` |
| Bounded dynamic grouped page | `execute_public_dynamic_grouped_query(&request)` |
| Trusted dynamic maintenance page | `execute_trusted_live_page(&request, continuation)` |
| Trusted dynamic grouped maintenance read | `execute_trusted_dynamic_grouped_query(&request)` |
| Trusted SQL read | `execute_trusted_sql_query(sql)` |
| Query diagnostics | SQL `EXPLAIN` through a trusted/admin surface |

Public scalar typed and dynamic reads return bounded live pages. A non-null
continuation means traversal is not yet proven exhausted; it does not promise
that another matching row exists. Grouped reads retain their separate opaque
cursor and explicit engine limits.

## When Admission Rejects A Read

The public lane is deliberately fail-closed. A request must carry a positive
row limit no greater than 100 and select a planner-proven bounded/index-backed
route. It rejects unbounded scans, materialized ordering, unsupported grouped
shapes, and excessive primary-key input work before row execution.

The returned typed error preserves the stable `QueryReadAdmissionCode`; see
[READ_ADMISSION.md](../contracts/READ_ADMISSION.md) for the complete table.

## Public Endpoint Template

```rust
#[icydb::request_execution]
#[ic_cdk::query]
fn active_users(continuation: Option<String>) -> Result<LivePage<User>, String> {
    db!()
        .map_err(|error| error.to_string())?
        .query::<User>()
        .map_err(|error| error.to_string())?
        .filter(FieldRef::new("active").eq(true))
        .order_by(asc("id"))
        .execute_live_page(continuation.as_deref())
        .map_err(|error| error.to_string())
}
```

The endpoint must authorize the caller before entering IcyDB and establish
exactly one request boundary. Nested helpers use `db!()` without an argument
and share that request's counters. Async endpoints use the same attribute:
IcyDB installs the scope only while their future is polled and retains its
counters across `.await`. The endpoint must still enforce its final
response-byte budget after shaping the typed result.

The explicit `with_request_execution_root` plus `db!(&request_root)` form is
for low-level framework integration that already owns the root. It is not
required merely because an endpoint awaits. The other canister always has its
own IcyDB instance and request scope.

## Exact Lookup

Use a strict accepted primary-key predicate and a small limit. Admission
recognizes planner-proven exact primary-key access; it never trusts the
spelling of the predicate alone.

```rust
let row = db!()?.get::<User>(user_id)?;
```

`get` returns `None` for a missing key. Use `get_many` for a finite exact key
set; both operations carry their own bounded exhaustion proof.

## Bounded Lists

Use an accepted indexed order and an explicit limit:

```rust
let page = db!()?
    .query::<User>()?
    .filter(FieldRef::new("active").eq(true))
    .order_by(asc("id"))
    .execute_live_page(continuation.as_deref())?;
```

Return `page.continuation` to the caller and pass it back unchanged. The token
binds the complete order, query window, page envelope, database incarnation,
and accepted schema authority. It is authenticated and opaque, but not
encrypted.

## Bounded Grouped Pages

Grouped typed and dynamic reads use the same engine-neutral lane as scalar
reads. Declare group keys and aggregates in output order, provide hard grouped
limits, and bound each returned page:

```rust
let page = db!()?
    .query::<User>()?
    .group_by("country")
    .aggregate(count())
    .grouped_limits(100, 64 * 1024)
    .limit(25)
    .execute_grouped()?;
```

Continue by rebuilding the same request and passing `page.next_cursor` through
`.cursor(...)`. The token is opaque and remains bound to the accepted plan and
schema authority. Grouped queries reject `.select(...)`; their output is
defined by the ordered group keys and aggregate declarations.

## Trusted Reads

Trusted reads are visible method choices, not ambient flags:

```rust
let request = DynamicQuery::new("LedgerEntry")
    .select(["id", "amount"])
    .order_by(asc("id"))
    .limit(100);

let page = db!()?.execute_trusted_live_page(&request, continuation.as_deref())?;
```

Only controller/admin code with an explicit resource policy should use this
lane. `execute_trusted_sql_query` has the same authorization posture.

## Generated SQL Boundary

Generated `icydb_query` is controller-gated by default. A declaration may
instead use `authorization = guard(path)` to replace controller authority for
the complete admitted SQL read lane. Guarded mode rejects anonymous callers
before application code and does not add a controller fallback. It remains a
trusted SQL surface rather than a narrow public-read template; hand-written
public endpoints should use the ordinary typed/dynamic lane.

The dedicated `icydb_schema` declaration separately accepts
`authorization = public`, `controller`, or `guard(path)`. A schema guard sees
only the `Schema` surface and does not authorize `SHOW`, `DESCRIBE`, or
`EXPLAIN`; those remain statements on the guarded SQL lane. Both guarded
surfaces reject anonymous callers before application code and replace rather
than union with controller authority.

## Endpoint Review Checklist

- Authorize before dispatch.
- Use the public lane for caller-facing reads.
- Use the returned continuation until exhaustion, or declare a deliberate
  total query limit.
- Ensure filtering and ordering can select an accepted bounded/index route.
- For grouped reads, include positive group limits within the public ceilings.
- Treat admission rejection as a typed failure, never as an empty result.
- Bound the final encoded response.
- Use trusted methods only for explicit admin work.
- Do not decode, modify, or emulate continuation with offsets.

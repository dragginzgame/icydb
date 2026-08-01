# Read Intent Guide

IcyDB has two maintained read authorities:

- ordinary typed/dynamic reads use the built-in `PublicRead` admission policy;
- trusted dynamic or SQL reads are explicit admin lanes whose authorization
  and resource policy belong to the caller.

Generated Rust adapters never choose admission or execution semantics.

## Current Map

| Intent | Maintained surface |
| --- | --- |
| Bounded typed rows | `db.query::<E>()?.limit(n).execute_rows()` |
| Bounded dynamic rows | `execute_public_dynamic_query(&request)` |
| Trusted dynamic maintenance read | `execute_trusted_dynamic_query(&request)` |
| Trusted SQL read | `execute_trusted_sql_query(sql)` |
| Query diagnostics | SQL `EXPLAIN` through a trusted/admin surface |

Public typed and dynamic reads return one bounded result. Callers do not
provide continuation state or internal admission controls.

## When Admission Rejects A Read

The public lane is deliberately fail-closed. A request must carry a positive
row limit no greater than 100 and select a planner-proven bounded/index-backed
route. It rejects unbounded scans, materialized ordering, unsupported grouped
shapes, and excessive primary-key input work before row execution.

The returned typed error preserves the stable `QueryReadAdmissionCode`; see
[READ_ADMISSION.md](../contracts/READ_ADMISSION.md) for the complete table.

## Public Endpoint Template

```rust
#[ic_cdk::query]
fn active_users() -> Result<Vec<User>, String> {
    db()
        .map_err(|error| error.to_string())?
        .query::<User>()
        .map_err(|error| error.to_string())?
        .filter(FieldRef::new("active").eq(true))
        .order_by(asc("id"))
        .limit(25)
        .execute_rows()
        .map_err(|error| error.to_string())
}
```

The endpoint must authorize the caller before entering IcyDB and must still
enforce its final response-byte budget after shaping the typed result.

## Exact Lookup

Use a strict accepted primary-key predicate and a small limit. Admission
recognizes planner-proven exact primary-key access; it never trusts the
spelling of the predicate alone.

```rust
let rows = db()?
    .query::<User>()?
    .filter(FieldRef::new("id").eq(user_id))
    .limit(1)
    .execute_rows()?;
```

If application semantics require exactly one row, check the returned vector
explicitly and return an application-level typed error for zero or multiple
rows.

## Bounded Lists

Use an accepted indexed order and an explicit limit:

```rust
let rows = db()?
    .query::<User>()?
    .filter(FieldRef::new("active").eq(true))
    .order_by(asc("id"))
    .limit(50)
    .execute_rows()?;
```

The current scalar typed surface intentionally has no cursor or offset.
Endpoints that require scalable continuation need a separately designed
continuation contract; do not emulate one with hidden offsets.

## Trusted Reads

Trusted reads are visible method choices, not ambient flags:

```rust
let request = DynamicQuery::new("LedgerEntry")
    .select(["id", "amount"])
    .order_by(asc("id"))
    .limit(100);

let rows = db()?.execute_trusted_dynamic_query(&request)?;
```

Only controller/admin code with an explicit resource policy should use this
lane. `execute_trusted_sql_query` has the same authorization posture.

## Generated SQL Boundary

Generated `icydb_query` is controller-gated admin SQL. It is not a public read
endpoint template. Hand-written public endpoints should use the ordinary
typed/dynamic lane.

## Endpoint Review Checklist

- Authorize before dispatch.
- Use the public lane for caller-facing reads.
- Include a positive limit at or below 100.
- Ensure filtering and ordering can select an accepted bounded/index route.
- Treat admission rejection as a typed failure, never as an empty result.
- Bound the final encoded response.
- Use trusted methods only for explicit admin work.
- Do not emulate continuation with offsets; the maintained typed/dynamic
  surface does not yet expose scalar continuation.

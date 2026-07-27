# Read Admission

This contract defines IcyDB's maintained read lanes after the 0.213 hard cut.
Accepted schema, the planner result, and the built-in admission policy are the
only authorities. Generated models and application callbacks never decide
whether a read may execute.

## Core Rule

Ordinary caller-facing reads use `PublicRead`. The admission owner evaluates
the selected plan before row execution and rejects unsafe shapes with a typed
`QueryReadAdmissionCode`. Rejection is never converted into an empty result.

The default policy has these frozen ceilings:

- maximum returned rows: 100;
- primary-key predicate input: 1024 terms and 64 KiB;
- grouped engine budget: 100 groups, 64 KiB per group, and 1024 distinct
  entries, although the current typed/dynamic facade does not expose grouped
  construction.

`PublicRead` also rejects unbounded full scans and materialized ordering. A
caller-supplied `LIMIT` does not prove safe access by itself: the selected
route must be bounded/index-backed.

`DiagnosticExplain` observes planning but cannot execute rows.

Trusted bypass surfaces are explicit method choices. They retain accepted
schema, planning, execution, and result-shape validation, but application code
owns authorization and the resource policy.

## Read Surface Inventory

| Surface | Lane | Contract |
| --- | --- | --- |
| `DbSession::query::<E>()?.execute_rows()` | `PublicRead` | Generated binding and decode around the ordinary accepted structural lane. |
| `execute_public_dynamic_query` | `PublicRead` | Entity/field names resolve against accepted schema; built-in bounded admission applies. |
| `execute_trusted_dynamic_query` | trusted bypass | Explicit maintenance/admin dynamic read. |
| `execute_trusted_sql_query` | trusted bypass | Trusted/admin SQL; caller-controlled SQL is not public-safe. |
| generated `icydb_query` | trusted bypass | Controller-gated admin SQL using `execute_trusted_sql_query_with_perf_attribution`. |
| SQL `EXPLAIN` | `DiagnosticExplain` | Observational planning only on its diagnostic route. |

Public callers cannot provide scalar continuation state, offsets, or admission
policy controls.

## Which API should I use?

- Known generated row type: `query::<E>()`, with `typed_adapters` enabled.
- Runtime entity/field names: `DynamicQuery` plus
  `execute_public_dynamic_query`.
- Controller/admin maintenance: `execute_trusted_dynamic_query`.
- Authorized SQL tooling: `execute_trusted_sql_query`.
- Planner inspection: trusted SQL `EXPLAIN`.

Ordinary typed and dynamic calls require a positive limit at or below 100 and
a safe selected route. The current scalar typed surface intentionally has no
continuation contract. Do not emulate continuation with hidden offsets.

## Generated SQL Query Surface

Generated `icydb_query` remains controller-gated. It calls
`icydb_sql_surface_require_controller("query")` before dispatch and uses
`execute_trusted_sql_query_with_perf_attribution`.

`icydb.toml` has no `sql.public_read` configuration, and a non-controller
generated SQL query endpoint is forbidden. Generated SQL is an admin surface,
not a public endpoint template.

## Public Endpoint Guidance

Authorize the caller before entering IcyDB. Use the ordinary typed or dynamic
lane, supply a small explicit limit, and shape a bounded response:

```rust
let rows = db()?
    .query::<User>()?
    .filter(FieldRef::new("active").eq(true))
    .order_by(asc("id"))
    .limit(25)
    .execute_rows()?;
```

Filtering and ordering must map to accepted indexed access. A public endpoint
must also enforce its final encoded-response budget after IcyDB returns.

See [the read-intent guide](../guides/read-intent.md) for maintained examples.

## Common Rejections And Fixes

| Diagnostic | Meaning | Correction |
| --- | --- | --- |
| `QueryReadAdmissionCode::PublicQueryRequiresLimit` | No proven finite returned-row bound. | Add a positive limit or use exact selected primary-key access. |
| `QueryReadAdmissionCode::PublicQueryRequiresIndex` | The selected route is not index-backed/bounded. | Add or select an accepted index, or move authorized maintenance to a trusted lane. |
| `QueryReadAdmissionCode::UnboundedFullScanRejected` | Planning selected a full entity scan. | Use indexed filtering or an explicit trusted lane. |
| `QueryReadAdmissionCode::SortRequiresMaterialization` | Ordering would materialize rows. | Use accepted index order or a trusted lane with its own budget. |
| `QueryReadAdmissionCode::GroupedQueryRequiresLimits` | Grouped execution lacks hard budgets. | Use a supported surface with explicit group and memory bounds. |
| `QueryReadAdmissionCode::GroupedQueryExceedsBudget` | Group limits exceed the built-in public policy. | Reduce the bounds or use authorized trusted execution. |
| `QueryReadAdmissionCode::DiagnosticLaneDoesNotExecute` | An explain-only lane was asked to execute. | Execute through a row-owning lane. |
| `QueryReadAdmissionCode::ReturnedRowBoundExceedsPolicy` | The row bound exceeds 100. | Reduce the public response bound. |
| `QueryReadAdmissionCode::PrimaryKeyInputExceedsPolicy` | Primary-key input count or bytes exceed policy. | Split the request or use authorized bounded maintenance. |

## Regression Guard

`scripts/ci/check-read-admission-invariants.sh` verifies:

- internal and public rejection enums remain one-to-one;
- default budgets remain synchronized with this contract;
- typed execution enters `execute_public_dynamic_query`;
- trusted SQL documentation and generated-controller ownership remain intact;
- custom policies, old fluent surfaces, and legacy consumer crates do not
  reappear.

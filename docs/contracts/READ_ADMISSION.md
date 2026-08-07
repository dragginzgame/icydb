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
  entries.

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
| `DbSession::query::<E>()?.execute_live_page(...)` | `PublicRead` | Generated binding and decode around an authenticated bounded live page. |
| `execute_live_page` | `PublicRead` | Entity/field names resolve against accepted schema; built-in bounded admission and explicit continuation apply. |
| `DbSession::query::<E>()?.execute_exhaustive_page(...)` | `PublicRead` | Generated binding and decode around a revision-strict page; resume requires its complete source proof. |
| `execute_exhaustive_page` | `PublicRead` | Bounded scalar execution plus pre/post comparison of the canonical participating-store proof. |
| `DbSession::query::<E>()?.execute_grouped()` | `PublicRead` | Generated binding selects accepted entity identity; the engine-neutral grouped result remains structural. |
| `execute_public_dynamic_grouped_query` | `PublicRead` | Grouped dynamic execution requires explicit limits and exposes an opaque continuation cursor. |
| `execute_trusted_live_page` | trusted bypass | Explicit maintenance/admin dynamic page. |
| `execute_trusted_exhaustive_page` | trusted bypass | Authorized maintenance page with the same revision-strict proof contract. |
| `execute_trusted_dynamic_grouped_query` | trusted bypass | Explicit grouped maintenance/admin read with caller-owned authorization and explicit engine limits. |
| `execute_trusted_sql_query` | trusted bypass | Trusted/admin SQL; caller-controlled SQL is not public-safe. |
| generated `icydb_query` | trusted bypass | Controller-gated admin SQL using `execute_trusted_sql_query_with_perf_attribution`. |
| SQL `EXPLAIN` | `DiagnosticExplain` | Observational planning only on its diagnostic route. |

Public scalar and grouped callers may provide only the opaque cursor issued by
the preceding page of the same accepted plan. They cannot provide offsets or
admission-policy controls.

## Which API should I use?

- Known generated row type: `query::<E>()`; runtime adapters are automatic.
- Runtime entity/field names: `DynamicQuery` plus
  `execute_live_page`.
- Complete unchanged-set traversal: typed or dynamic
  `execute_exhaustive_page`, retaining both continuation and proof.
- Grouped typed/dynamic rows: ordered `.group_by(...)` and `.aggregate(...)`
  declarations, explicit `.grouped_limits(...)`, and the grouped terminal.
- Controller/admin maintenance: `execute_trusted_live_page`.
- Authorized SQL tooling: `execute_trusted_sql_query`.
- Planner inspection: trusted SQL `EXPLAIN`.

Ordinary typed and dynamic pages require a safe selected route and return at
most 100 rows. A query `LIMIT`, when supplied, is the total traversal limit,
not a per-page limit. Do not emulate continuation with hidden offsets.

Live pages tolerate source mutations and provide forward keyset progress, not
snapshot completeness. Exhaustive pages compare the complete bounded physical
store proof before and after every page. A non-null continuation means only
that exhaustion has not been proved; completion requires a null continuation
under one unchanged proof.

Grouped calls additionally require positive `max_groups` and
`max_group_bytes` values. Public calls must remain within the frozen ceilings;
trusted calls bypass public admission but not their declared hard limits.
Group keys and aggregates define grouped output, so `.select(...)` is rejected
for grouped execution.

## Generated SQL Query Surface

Generated `icydb_query` remains controller-gated. It calls
the generated SQL controller authorization helper before dispatch and uses
`execute_trusted_sql_query_with_perf_attribution`.

The maintained `icydb_sql_query` declaration always produces a controller-gated
admin surface, not a public endpoint template.

## Public Endpoint Guidance

Authorize the caller before entering IcyDB. Use the ordinary typed or dynamic
lane and shape a bounded response; add a small query limit only when the whole
logical traversal needs a smaller cap than the built-in page envelope:

Generated IcyDB endpoints establish the request scope automatically. A manual
IC-CDK, Canic, lifecycle, or timer entry uses
`#[icydb::request_execution]` and keeps using `db!()` in nested helpers. The
same root is installed per poll across async suspension. The explicit root
argument is reserved for low-level framework integration that already owns
the root; it is never shared with the called canister and cannot replace a
different active root.

```rust
let page = db!()?
    .query::<User>()?
    .filter(FieldRef::new("active").eq(true))
    .order_by(asc("id"))
    .execute_live_page(continuation.as_deref())?;
```

Filtering and ordering must map to accepted indexed access. A public endpoint
must return the opaque continuation and enforce its final encoded-response
budget after IcyDB returns.

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
- typed execution enters the identity-bound
  live or exhaustive page boundary;
- trusted SQL documentation and generated-controller ownership remain intact;
- public reads enter through the maintained typed or dynamic admission
  boundaries.

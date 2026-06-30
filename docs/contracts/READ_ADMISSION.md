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
  `OFFSET`, and require explicit grouped budgets for grouped queries.
- `AdminAdHoc`: trusted/controller-gated operational reads. These may use the
  broad SQL query helper, but the endpoint must remain visibly controller
  gated and must not be mistaken for a public read surface.
- `DiagnosticExplain`: EXPLAIN-only diagnostics. This lane may parse, lower,
  plan, and evaluate admission, but it must not execute data rows.
- `DevTest`: local tests and harnesses only.

Estimates may be reported by diagnostics, but estimates do not authorize
`PublicRead` execution.

## Read Surface Inventory

| Surface | Lane | Guard | Query execution authority |
| --- | --- | --- | --- |
| `DbSession::execute_sql_query::<E>` | `AdminAdHoc` by caller contract | caller-owned | Trusted single-entity SQL query helper. It is not public-safe by itself. |
| `DbSession::execute_query::<E>` / `FluentLoadQuery::execute` / `execute_rows` / terminal execution / paged `execute` | `PublicRead` default policy | built-in plus caller auth | Ordinary typed/fluent execution. It rejects unsafe full scans, non-zero offset, materialized sorts, missing row bounds, and grouped reads without query hard limits. |
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

The default typed/fluent policy is intentionally conservative:

- maximum returned rows: 100;
- maximum plan-level response bytes: 128 KiB where the surface can prove it;
- full scans are rejected;
- an index-backed access proof is required;
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

If a public endpoint accepts caller-provided SQL, it must:

- reject anonymous callers and perform any application authorization before
  entering IcyDB;
- not pass that SQL to `execute_sql_query`;
- use an application-owned SQL parser/allowlist or a typed/fluent endpoint
  instead;
- keep generated SQL endpoints controller-gated.

## Persisted Format

Read admission is a pre-execution runtime policy. It does not change marker,
journal, row, schema, index, cursor, fold watermark, or structural-value
persisted formats.

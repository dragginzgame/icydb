# IcyDB Read Admission Contract

This document defines the operational lane contract for read execution
surfaces. Query semantics remain documented in `QUERY_CONTRACT.md`,
`QUERY_PRACTICE.md`, and `SQL_SUBSET.md`; this document answers which surfaces
may execute reads and which admission lane they use.

## Core Rule

Any production canister surface that executes caller-controlled read work must
make its lane explicit.

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
| `DbSession::execute_sql_query_with_read_admission_policy::<E>` | supplied by `QueryAdmissionPolicy` | caller-owned | Explicit policy-bound SQL read seam for custom public endpoints. |
| generated `icydb_query` | `AdminAdHoc` | controller-gated | Generated SQL query endpoint. It uses the trusted perf-attributed SQL helper and remains admin-only. |
| generated `icydb_ddl` | not a read-admission lane | controller-gated | Schema mutation frontend, governed by DDL admission and schema authority. |
| generated `icydb_update` | not a read-admission lane | controller-gated | SQL write endpoint, governed by explicit write policy. |
| generated `icydb_schema` / `icydb_schema_check` | diagnostic/admin | controller-gated | Accepted-schema diagnostics, not row-query execution. |
| generated `icydb_snapshot` | diagnostic/admin | build-option gated | Storage report diagnostics, not row-query execution. |
| generated `icydb_metrics` / `icydb_metrics_extended` | diagnostic | build-option gated | Metrics diagnostics, not row-query execution. |

IcyDB does not generate non-controller public SQL read endpoints. A canister
that wants caller-facing SQL must define an application-owned endpoint and call
`execute_sql_query_with_read_admission_policy` with an explicit finite
`QueryAdmissionPolicy::public_read(...)`. Generated `icydb.toml` SQL settings
intentionally have no `sql.public_read` key.

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
known to the canister author.

If a public endpoint accepts caller-provided SQL, it must:

- reject anonymous callers and perform any application authorization before
  entering IcyDB;
- call `execute_sql_query_with_read_admission_policy`;
- use finite returned-row and response-byte budgets;
- attach grouped budgets before admitting grouped SQL;
- keep introspection disabled unless the endpoint has a separate public
  redaction policy;
- return read-admission diagnostics rather than falling back to the trusted SQL
  helper.

## Persisted Format

Read admission is a pre-execution runtime policy. It does not change marker,
journal, row, schema, index, cursor, fold watermark, or structural-value
persisted formats.

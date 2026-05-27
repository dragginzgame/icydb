# 0.165 Public Surface Naming

## Status

Complete.

## Accepted Renames

None.

Public names audited in this family already match user-facing database or
diagnostics vocabulary closely enough to keep. 0.165 does not rename public
facade/result DTOs for internal consistency alone.

## Kept Names

### `QueryResponse`

Kept because it is the public fluent-query response envelope that distinguishes
scalar entity rows from grouped rows. `Response` alone would lose the query
surface context, and `QueryResult` would collide conceptually with `Result<T,
Error>` and the SQL endpoint result vocabulary.

### `ProjectionResponse`

Kept because it is a public facade over projection-shaped query rows with
cardinality helpers and projection-row iteration. The name is conventional and
matches the `ProjectedRow` payload.

### `MutationResult`

Kept because it is the public write-result family for authored write
operations. The name is conventional database vocabulary and accurately covers
count-only, single-entity, and batch-entity mutation outputs.

### `MutationMode`

Kept because it is the explicit public/structural write-mode selector for
insert, replace, and update row-existence semantics. `Mode` is correct because
the enum selects behavior rather than carrying evidence or diagnostics.

### `SqlProjectionRows`

Kept because it is the render-ready row payload used before endpoint packaging.
The name distinguishes raw projection rows from endpoint output envelopes.

### `SqlQueryRowsOutput`

Kept because it is the structured SQL endpoint projection payload. `Output`
is acceptable here because the type is serialized across the public SQL
endpoint boundary, not an internal planner artifact.

### `SqlGroupedRowsOutput`

Kept because it is the structured SQL endpoint grouped-row payload, including
continuation cursor data. It mirrors `SqlQueryRowsOutput` while preserving the
grouped-query distinction.

### `SqlQueryResult`

Kept because it is the unified SQL endpoint result envelope for count,
projection, grouped, explain, describe, show, and DDL results. This public name
has longstanding endpoint and CLI meaning.

### `QueryExecutionAttribution`

Kept because it is a public diagnostics DTO that attributes query execution
costs across compile, execution, cache, and response phases. `Attribution`
is more accurate than `Stats` because the fields partition observed work by
phase.

### `SqlQueryExecutionAttribution`

Kept because it is the SQL-specific public diagnostics DTO for reduced SQL
query cost attribution. The `Sql` prefix is needed because the field groups
differ from typed/fluent query attribution.

### `ExplainExecutionDescriptor`

Kept because it is an observable EXPLAIN descriptor consumed by renderers.
This is one of the allowed `Descriptor` uses: renderable/observable description,
not execution authority.

### `ExplainPropertyMap`

Kept because it is the stable ordered key/value map for EXPLAIN metadata.
`Map` is accurate for the public behavior, and the type intentionally preserves
deterministic order without exposing a `BTreeMap` implementation detail.

## Deferred Candidates

None for 0.165.

If a future public API cleanup proposes a rename, it needs a user-facing
benefit, migration wording, and changelog entry. Internal naming consistency is
not enough justification for public surface churn.

## Old-Vocabulary Scan Terms

Live-code scans for this slice:

```bash
rg -n "QueryResponse|ProjectionResponse|MutationResult|MutationMode|SqlProjectionRows|SqlQueryRowsOutput|SqlGroupedRowsOutput|SqlQueryResult|QueryExecutionAttribution|SqlQueryExecutionAttribution|ExplainExecutionDescriptor|ExplainPropertyMap" crates docs
rg -n "Response|Result|Output|Attribution|Descriptor|PropertyMap" crates/icydb/src crates/icydb-core/src/db docs/design/0.165-naming-audit-and-role-alignment
```

Remaining hits are intentional public/facade vocabulary, active 0.165 audit
notes, or historical changelog/archive references.

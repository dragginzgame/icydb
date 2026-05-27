# 0.165 Public Surface Naming

## Status

Complete.

## Accepted Renames

### `PersistedRelationDescriptionParts` -> `PersistedRelationDescriptionMetadata`

Role proof:

- Owning module: `db::schema::describe`
- Payload: private metadata view extracted from persisted accepted relation
  field contracts while building public DESCRIBE relation rows
- Main consumers: accepted-schema DESCRIBE relation rendering
- Chosen family: `*Metadata`
- Rejected alternatives:
  - `*Parts`: too weak because the value is not a general decomposition; it is
    the persisted relation metadata needed for one describe row
  - `*Context`: wrong because it is returned metadata, not owner-local
    traversal/input state
  - `*Descriptor`: wrong because the public renderable row is
    `EntityRelationDescription`; this private value only feeds that description
- Public-surface impact: none; the public `EntityRelationDescription` surface
  is unchanged
- Hard-cut rule: remove the old private type and helper vocabulary from live
  schema describe code

Companion helper rename:

- `persisted_relation_description_parts(...)` ->
  `persisted_relation_description_metadata(...)`

### SQL Projection Payload Components

Role proof:

- Owning modules: `db::session::sql::compiled` and
  `db::session::sql::projection::payload`
- Payload: private component unpacking for SQL projection contracts and
  projection payloads crossing SQL execution/write boundaries
- Main consumers: SQL SELECT, diagnostics response shaping, INSERT SELECT, and
  UPDATE selector execution
- Chosen family: component vocabulary for explicit unpacking helpers
- Rejected alternatives:
  - `*Parts`: too weak because these are stable SQL projection boundary
    components, not ad hoc decompositions
  - `*Context`: wrong because the values are returned/unpacked payload
    components, not owner-local traversal state
  - `*Descriptor`: wrong because these values are execution payloads, not
    renderable descriptions
- Public-surface impact: none; visibility remains session-internal
- Hard-cut rule: remove the private `Parts` alias and `into_parts` helper names
  from live SQL projection payload/contract code

Accepted renames:

```text
SqlProjectionPayloadParts -> SqlProjectionPayloadComponents
SqlProjectionPayload::into_parts() -> into_components()
SqlProjectionContract::into_parts() -> into_components()
```

### `describe_entity_model_with_parts` -> `describe_entity_model_from_description_rows`

Role proof:

- Owning module: `db::schema::describe`
- Payload: private DESCRIBE assembler that consumes completed field, index, and
  relation description rows from either generated-model or accepted persisted
  schema authority
- Main consumers: generated-model DESCRIBE and accepted-schema DESCRIBE
  construction
- Chosen family: description-row assembly vocabulary
- Rejected alternatives:
  - `with_parts`: too weak because the helper assembles public DESCRIBE output
    from already built row descriptions, not arbitrary parts
  - `from_metadata`: too broad because fields, indexes, and relations are
    renderable description rows by this point
  - `from_context`: wrong because no owner-local traversal context is passed
- Public-surface impact: none; `EntitySchemaDescription` is unchanged
- Hard-cut rule: remove the old private helper name from live schema describe
  code

No public names are renamed in this family. Public names audited here already
match user-facing database or diagnostics vocabulary closely enough to keep.
0.165 does not rename public facade/result DTOs for internal consistency alone.

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
rg -n "PersistedRelationDescriptionParts|persisted_relation_description_parts|PersistedRelationDescriptionMetadata|persisted_relation_description_metadata" crates/icydb-core/src/db/schema/describe.rs docs/design/0.165-naming-audit-and-role-alignment
rg -n "SqlProjectionPayloadParts|SqlProjectionPayloadComponents|SqlProjectionPayload::into_parts|SqlProjectionContract::into_parts|into_components\\(" crates/icydb-core/src/db/session/sql docs/design/0.165-naming-audit-and-role-alignment
rg -n "describe_entity_model_with_parts|describe_entity_model_from_description_rows" crates/icydb-core/src/db/schema/describe.rs docs/design/0.165-naming-audit-and-role-alignment
```

Remaining hits are intentional public/facade vocabulary, active 0.165 audit
notes, or historical changelog/archive references.

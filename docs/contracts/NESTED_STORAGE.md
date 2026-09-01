# IcyDB Nested Storage Contract

This document defines which stored composite and collection shapes are
queryable, indexable, and mutable. Accepted schema is the sole runtime
authority for these capabilities. SQL, fluent and dynamic query construction,
generated adapters, index DDL, writes, recovery, and diagnostics must not infer
additional behavior from the runtime shape of a value.

The query evaluator remains governed by [Query Practice](QUERY_PRACTICE.md),
SQL syntax by the [SQL Subset](SQL_SUBSET.md), and complete-row mutation by
[Write Admission](WRITE_ADMISSION.md).

## Product Boundary

IcyDB is relational with exact typed composite values. A stored record is not
an open document, and a stored collection does not create child rows.

- A scalar member path traverses named members of required or optional records
  and terminates at one accepted scalar, such as `profile.score`.
- A repeated aggregate is a list, set, or map owned by its containing row.
- Whole-field mutation supplies one complete accepted value for a persisted
  root field.
- Subpath mutation targets a member or element without supplying that complete
  root value and is unsupported.

When repeated children need independent queries, indexes, relations, or
mutation, model them as entities with an owner key. Do not maintain an embedded
aggregate and shadow child entities as competing authorities.

## Shape Matrix

| Persisted shape | Query and index contract | Mutation contract |
| --- | --- | --- |
| Direct scalar | Existing capabilities of the accepted scalar kind. | Existing root-field behavior. |
| Required or optional named record | Complete root projection plus admitted dotted scalar member paths. Eligible scalar paths may be index keys. | Complete root-field replacement. An optional root also accepts omission, default, or null according to its accepted policy. |
| Tuple | Complete root value only; no positional member path or member index. | Complete root-field replacement. |
| Newtype | Existing root behavior; no independently addressable wrapper-member path or member index. | Complete root-field replacement. |
| Named or anonymous list | Complete root value and the whole-collection predicates below; no element path or multikey index. | Complete aggregate replacement. |
| Set | Complete root value and the whole-collection predicates below; no element path or multikey index. | Complete aggregate replacement. |
| Map | Storage and whole-field writes only; no map query or map-key/value index. | Complete aggregate replacement. |
| Opaque blob | Whole-blob scalar behavior only. IcyDB has no JSON field kind or member path. | Complete blob replacement; the application owns its encoding and payload version. |

Only named record members produce accepted dotted nested-leaf metadata.
Cardinality and shape are therefore part of path admission; “nested” alone is
not a capability.

## Scalar Record Paths

An admitted scalar path through named records supports exactly these cells,
subject to the terminal scalar kind and the ordinary clause contract:

| Context | Contract |
| --- | --- |
| Projection | Supported. |
| Filtering | Supported for operators admitted by the terminal kind. |
| Ordering | Supported when the terminal kind is orderable. |
| Projection `DISTINCT` | Supported on the materialized scalar result. |
| `GROUP BY` | Unsupported; grouped keys remain direct fields. |
| Aggregate input | Supported when the aggregate accepts the terminal kind, including aggregate `DISTINCT`. |
| `HAVING` | Supported only inside an admitted aggregate expression; a raw member path is not a grouped key. |
| `CASE`, arithmetic, and scalar functions | Supported where the selected expression and clause accept the terminal kind. |
| Internal prepared/cache execution | Supported; this does not add public SQL placeholders or `PREPARE`. |
| Single or composite index | Supported when every component is an accepted indexable scalar source. |

For example, `SELECT profile.score FROM Player WHERE profile.score >= 10
ORDER BY profile.score` is admitted when `score` has the required scalar
capabilities. `GROUP BY profile.score` and raw `HAVING profile.score > 10`
reject, while `HAVING SUM(profile.score) > 100` may be admitted.

## Whole-Collection Predicates

The maintained fluent/structural list and set predicates operate on the root
collection. They do not traverse record-valued elements:

| Predicate | Meaning |
| --- | --- |
| `eq` / `ne` | Compare one complete collection value. |
| `eq_field` / `ne_field` | Compare compatible complete collection fields. |
| `in_list` / `not_in` | Compare the complete field with finite complete collection literals. |
| `contains` | Test for one admitted top-level element. |
| `is_empty` / `is_not_empty` | Test the cardinality of the complete collection. |
| `is_null` / `is_not_null` | Test a nullable collection root. |

For example:

```rust
use icydb::prelude::{FilterExpr, InputValue};

let exact = FilterExpr::eq(
    "tags",
    InputValue::list(vec!["mage".into(), "healer".into()]),
);
let member = FilterExpr::contains("tags", "mage");
let finite = FilterExpr::in_list(
    "tags",
    [
        InputValue::list(vec!["mage".into()]),
        InputValue::list(vec!["healer".into()]),
    ],
);
```

`finite` compares complete list values; it is not element membership. Ordered
range predicates, existential or universal member predicates, row expansion,
and multikey behavior are unsupported.

## Optional Record Paths

Missing descendants and explicit terminal nulls remain distinct during
predicate admission, although some result operations intentionally collapse
them:

- a null optional record makes its descendant path missing;
- projection materializes a missing descendant as null;
- comparison predicates do not match a missing descendant, including
  descendant `IS NULL`;
- an explicit null terminal follows the normal null predicate rules;
- ordering and projection `DISTINCT` use the same null key for missing and
  explicit-null descendants;
- an accepted omission-capable non-unique nested index omits null or missing
  terminals, so non-null comparisons may use it but null or missing-path tests
  cannot recover omitted rows; and
- a unique index below a nullable ancestor, or with an omit-capable nested
  terminal, rejects while the maintained predicate binder cannot prove the
  required dotted membership guard.

These rules do not make scalar member paths valid grouped keys.

## Bounds And Aggregate Ownership

All recursive values remain subject to the physical row, request, and bounded
decode ceilings. Those limits are not application-visible collection
capacities.

A durable repeated-item maximum belongs to a named collection through
`length_range_inclusive`. Anonymous `value(many, ...)` has no inline item-count
rule and does not gain a second `max_items` spelling. See the maintained
[named-list example](../guides/schema-authoring.md#bounded-owner-local-collections).

A smaller state-dependent capacity, element uniqueness, or cross-entity
existence rule remains application-owned unless it is represented by an
existing accepted relational or constraint contract.

## Write, Atomicity, And Recovery Boundary

Typed and structural writes replace the complete owning composite or
collection field. Accepted admission validates the complete final row image
and every applicable durable rule before commit preparation.

IcyDB has no member patch, positional or keyed element patch,
predicate-selected patch, element compare-and-swap identity, partial aggregate
journal record, or separate aggregate recovery route. Applications needing an
element-like edit read the bounded aggregate, validate the change, and replace
the complete field under their existing row-revision and atomic-batch
protocol.

Generated record and collection values cross the structural boundary through
`DbSession::bind_typed_input`. The session first proves that the opaque entity
binding is current, then the existing `TypedInputValue` adapter resolves
record-member and enum-variant source identities through accepted schema. The
result is one ordinary `InputValue` cell; complete-field admission, atomicity,
constraints, persistence, and recovery remain unchanged. This adapter does not
make a nested path independently mutable.

## Rejection And Diagnostics

Unsupported record or repeated-member paths must fail before execution. They
must never enter a scan fallback, produce a multikey candidate, or be inferred
from decoded `InputValue` or runtime value shape.

Each public surface retains its typed validation boundary and exact error
variant; callers must use diagnostic codes and structured facts rather than
matching messages. SQL, dynamic queries, DDL, and `EXPLAIN` already reject an
unaccepted path before execution or catalog mutation. Generated fluent queries
do not expose rooted repeated-member references.

Structural mutation accepts root field names, not paths. The 0.252 entry audit
found that a dotted or otherwise unknown structural field reached an executor
invariant. The 0.252 hard cut now routes that failed accepted-root lookup to
the existing executor-origin `RuntimeUnsupported` diagnostic. It adds no field
payload, diagnostic variant, or subpath mutation route.

## Explicit Non-Goals

This contract adds no collection expansion, repeated-member predicate,
multikey index, nested relation or uniqueness, subpath mutation, capability
tier, runtime mode, planner route, cursor format, persisted state, or legacy
compatibility path.

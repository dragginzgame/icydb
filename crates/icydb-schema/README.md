# IcyDB Schema

Public, bounded, versioned schema-proposal vocabulary for standalone IcyDB.

This leaf package owns canonical scalar atoms, immutable source keys, reusable
entity/type fragments with exact scalar widths and bounds, source-keyed enum
literals, explicit removals, deterministic cross-fragment closure, and
database-scoped proposal transport. References absent from the proposal are
admitted only when an exact expected accepted head can resolve them during
application. This crate does not own accepted schema, runtime planning,
storage, application callbacks, clocks, or generated values.

Canonical scalar representation is exposed here through `ScalarKind`,
`ScalarMetadata`, `ScalarCoercionFamily`, `ALL_SCALAR_KINDS`, and the scalar
`scalar_kind_registry!` macro. Runtime value operations, coercion policy,
codecs, and storage behavior remain in `icydb-core`.

## Rule Authority Boundary

One model-authored durable rule reaches this contract as a bounded
`TargetedRuleFragment`: persisted root field, nominal target type, local rule
key, and one closed typed operation. Accepted-schema publication resolves
those source keys to accepted IDs and exact operands. From that point, the
accepted snapshot is the sole runtime authority for writes, integrity, and
recovery; the authored Rust model and its generated types are not a fallback.

Application normalizers and validators do not enter the schema proposal at
all. They remain explicitly invoked Rust behavior and cannot substitute for a
durable constraint.

The 0.216 proposal retains contract version 1 with the current closed rule
operation shape. The accepting runtime uses the current `ICYU` accepted
snapshot profile. Pre-1.0 development stores carrying the retired `ICYT`
profile must be recreated rather than translated.

See the
[0.213 design](../../docs/design/0.213-schema-authority-and-application-model-separation/0.213-design.md)
for the package and authority boundary.

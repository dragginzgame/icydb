# IcyDB Schema

Public, bounded, versioned schema-proposal vocabulary for standalone IcyDB.

This leaf package owns canonical scalar atoms, immutable source keys, reusable
entity/type fragments with exact scalar widths and bounds, source-keyed enum
literals, explicit removals, deterministic cross-fragment closure, and
database-scoped proposal transport. References absent from the proposal are
admitted only when an exact expected accepted head can resolve them during
application. This crate does not own accepted schema, runtime planning,
storage, application callbacks, clocks, or generated values.

See the
[0.213 design](../../docs/design/0.213-schema-authority-and-application-model-separation/0.213-design.md)
for the package and authority boundary.

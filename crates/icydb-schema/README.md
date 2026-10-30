# IcyDB Schema

Public, bounded, versioned schema-proposal vocabulary for standalone IcyDB.

This leaf package owns canonical scalar atoms, bounded current source keys, reusable
entity/type fragments with exact scalar widths and bounds, source-keyed enum
literals, explicit removals, deterministic cross-fragment closure, and
database-scoped proposal transport. References absent from the proposal are
admitted only when an exact expected accepted head can resolve them during
application. This crate does not own accepted schema, runtime planning,
storage, application callbacks, clocks, or generated values.

Canonical scalar representation is exposed here through `ScalarKind`,
`ScalarMetadata`, `ScalarCoercionFamily`, `ALL_SCALAR_KINDS`, and the scalar
`scalar_kind_registry!` macro. Runtime value operations, coercion policy,
database formats, and storage behavior remain in `icydb-core`.

IC Wire owns artifact serialization and bounded allocation admission. Schema's
public fragment/proposal/migration encoders and decoders retain field limits,
semantic validation and canonical byte checks. Use those APIs instead of raw
Serde decoding for untrusted artifacts. Their bytes determine retained source
lineage and migration/application digest authority; rebuilding an artifact does
not migrate that retained authority. Current identities and installation
obligations live in the
[persisted-format inventory](../../docs/contracts/PERSISTED_FORMAT_INVENTORY.md#0291-authoring-and-deep-job-codec-cut).

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

The accepting runtime owns accepted snapshot formats and stable identities.
Schema's source keys describe current authored names; explicit migration resolves
renames without making generated declarations runtime authority.

See the
[0.213 design](../../docs/design/archive/0.213-schema-authority-and-application-model-separation/0.213-design.md)
for the package and authority boundary.

Application authors should start with the
[schema authoring guide](../../docs/guides/schema-authoring.md), which derives
the maintained primary-key primitive set from this crate's scalar metadata and
explains the host-build/runtime authority boundary.

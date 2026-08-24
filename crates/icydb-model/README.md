# IcyDB Model

Application-model authoring, validation, and code generation for IcyDB.

This package owns the host-side declaration graph, reusable application types,
validators and normalizers, and lowering into the bounded `icydb-schema`
proposal contract. It does not own accepted schema, storage, planning,
execution, or recovery authority.

Schema-only consumers depend on this package without depending on the IcyDB
database runtime. Generated typed adapters are explicit opt-in output for
consumers that also depend directly on `icydb`.

Runtime-enabled entity fields and record members also receive schema-authored
`FieldRef` constants, and entities implement `EntitySource`. The collision-safe
source spelling is `<Entity as icydb::traits::EntitySource>::ENTITY`. They
remove downstream string vocabularies but remain proposal/model convenience:
accepted schema snapshots alone decide whether the referenced source is
current.

The three behavior families are intentionally separate:

- durable rules are bounded proposal metadata that become accepted
  root-field-plus-nominal-type constraints and are enforced by IcyDB across
  direct, nested, repeated, and finite recursive occurrences;
- validators are explicitly invoked Rust application checks; and
- normalizers are explicitly invoked Rust application transformations.

Generated adapters never execute validators or normalizers implicitly, and
the database runtime never loads the authored model as enforcement authority.

## Durable Rules

Durable rules use one closed typed operation with named operands:

```rust
#[newtype(
    item(prim = "Text", unbounded),
    ty(rule(
        name = "length",
        length_range_inclusive(min = 1, max = 40)
    ))
)]
pub struct DisplayName {}
```

The model compiler instantiates that rule for every persisted root field that
can reach `DisplayName`. Proposal lowering then replaces the authored rule
with an accepted targeted constraint. Typed, structural, SQL, batch,
integrity, and recovery paths use the accepted snapshot; they never load this
Rust declaration as runtime authority.

The complete operation grammar is documented in the
[`icydb-model-macros` README](../icydb-model-macros/README.md).

## Explicit Application Behavior

Normalization and validation run only when application code asks for them:

```rust
use icydb_model::{NormalizeAndValidate as _, normalize, validate};
use icydb_model::{base::types::web::MimeType, visitor::VisitorError};

fn prepare_explicitly(mut value: MimeType) -> Result<MimeType, VisitorError> {
    normalize(&mut value)?;
    validate(&value)?;
    Ok(value)
}

fn prepare_conveniently(value: MimeType) -> Result<MimeType, VisitorError> {
    value.normalize_and_validate()
}
```

Direct `validate` checks the supplied value without normalizing it first. The
consuming convenience always normalizes before validation. Neither operation
persists behavior or changes database admission.

## 0.216 Hard Cut

The former rule `kind = "..."` plus positional rule `args(...)` spelling is
removed. There is no alias or compatibility parser. Development stores whose
accepted snapshots use the retired `ICYT` profile must be recreated; the sole
current accepted profile is `ICYU`.

References:

- Workspace overview: `../../README.md`
- [Schema authoring guide](../../docs/guides/schema-authoring.md)
- Design:
  `../../docs/design/archive/0.213-schema-authority-and-application-model-separation/0.213-design.md`
- Release notes: `../../CHANGELOG.md`

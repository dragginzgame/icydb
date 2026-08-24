# IcyDB Model Macros

Procedural-macro compiler for `icydb-model` declarations and application
helper derives.

Applications normally consume these macros through `icydb-model`. The compiler
lowers declarations into that package's host graph and the public
`icydb-schema` proposal vocabulary; it has no dependency on `icydb` or
`icydb-core`.

The package also transports the two thin request-entry attributes re-exported
only by the `icydb` runtime facade. They expand to facade calls resolved through
the consuming crate's direct dependency and do not own execution policy or
depend on runtime internals.

When a consumer also depends directly on `icydb`, entity and record macros
emit runtime `FieldRef` constants from their authored field/member names, and
entities implement `EntitySource`. The collision-safe source spelling is
`<Entity as icydb::traits::EntitySource>::ENTITY`; `Entity::ENTITY` is only
shorthand when no inherent field constant has that name. Schema-only consumers
receive no runtime-owned references. The constants carry source spelling into
accepted-schema-bound APIs; they never establish schema authority or freshness.

## Durable Rule Grammar

A `ty(...)` declaration may contain multiple uniquely named durable rules.
Each `rule(...)` requires exactly one of these typed operations:

```text
rule(name = "length", length_range_inclusive(min = 1, max = 40))
rule(name = "minimum", numeric_minimum_inclusive(value = 0))
rule(name = "maximum", numeric_maximum_inclusive(value = 100))
rule(name = "range", numeric_range_inclusive(min = 0, max = 100))
rule(name = "step", multiple_of(divisor = 5))
```

`length_range_inclusive` takes nonnegative `u64` bounds and requires
`min <= max`. Numeric operands are exact literals admitted later against the
declared target kind. `multiple_of` accepts only a nonzero exact integer or
fixed-scale decimal divisor; it has no floating-point form. Target-kind
incompatibility rejects during model-to-proposal lowering.

The grammar rejects missing or multiple operations, unknown operation or
operand names, missing or repeated operands, duplicate rule names, reversed
ranges, zero divisors, and invalid rule names. The retired string `kind` and
positional rule `args(...)` fields are not accepted. Positional `args(...)`
remain available only where explicitly documented for application normalizer
and validator constructors; those callbacks are not durable rules.

Changing an operation or operand while retaining the local rule name keeps
the catalog identity and uses normal accepted-constraint evolution. Changing
the rule name is an explicit removal plus addition.

## Application Behavior

Generated types implement the required traversal traits automatically.
Application code invokes `icydb_model::normalize`, `icydb_model::validate`, or
the consuming `NormalizeAndValidate::normalize_and_validate` method
explicitly. Generated persistence adapters and database writes do not call
normalizers or validators.

## Generated Rust Ergonomics

`traits(add(...), remove(...))` selects supported Rust trait implementations;
it is not a free-form list. Required traits are compiler-owned, while removal
remains available for supported generated implementations that applications
need to replace manually. The compiler resolves the complete node and shape
baseline before applying either directive and rejects any selected trait that
has no derive or generated implementation strategy.

`From` is generated only for newtype and collection wrappers.
`NormalizeCustom` and `ValidateCustom` are generated for every application
value and may be removed for a manual implementation. `Inner` is newtype-only.
`NumericValue` is generated or explicitly opt-in only for newtypes.

Collection wrappers generate `Default`, `Deref`, `DerefMut`, `FromIterator`,
and `IntoIterator`; other application values opt into `Default`, while only
newtypes may opt into dereference and display. `Copy`, `Hash`, `Ord`, and
`PartialOrd` are available as standard derives when the node shape does not
already generate them. Arithmetic, assignment, `Sum`, `Product`, and signed
`Neg` helpers are newtype-only and follow the wrapped primitive capability
baseline. Finite floats do not generate `Neg`, because negating canonical
positive zero would recreate negative zero.

Enum declaration-order checking is an enum option rather than a trait:

```rust,ignore
#[enum_(
    sorted,
    variant(name = "First"),
    variant(name = "Second")
)]
pub struct Ordered {}
```

Generated list, set, and map wrappers implement `Deref` and `DerefMut` to
their standard containers. Methods such as `iter`, `len`, and `is_empty` are
therefore available directly without an IcyDB-specific collection trait. They
also support standard owned and shared-reference iteration and exact
`FromIterator` collection. Lists and maps support mutable-reference iteration;
sets preserve the standard `BTreeSet` rule that elements cannot be mutated in
place. Either iterator protocol may be removed when an application supplies a
manual implementation.

References:

- Workspace overview: `../../README.md`
- [Schema authoring guide](../../docs/guides/schema-authoring.md)
- Release notes: `../../CHANGELOG.md`

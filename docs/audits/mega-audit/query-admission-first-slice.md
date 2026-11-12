# First implementation slice: one public input-value contract

Prepared on 2026-11-12 following authorization to pursue the
[query admission programme](query-admission-design.md). The maintainer authorized 0.310 on 2026-11-12 after the focused-ready
0.310 closeout. Slice 1 is implemented; package versions remain unchanged.

## Selected contract

Use the existing `InputValue` in FilterExpr literal positions and
`Into<InputValue>` in FieldRef/filter constructors. Delete `FilterValue`, its
conversion implementation and its public re-exports. Do not introduce an alias,
compatibility decoder, typed-only query route or another value wrapper.

InputValue already owns public input literals and delegates Serde to PublicValue.
It supports the scalar, enum, list and map shapes needed for a lossless authored
value boundary. The replacement therefore removes a parallel public literal
representation rather than adding one.

Concrete effects:

- `FieldRef::new("score").eq(42_u64)` retains Nat64 rather than formatting `"42"`.
- Blob, Date, Duration, Subaccount and Timestamp retain their types instead of
  being converted into hex/date/decimal text.
- Unit remains Unit rather than silently becoming Null.
- Enum values retain their optional type path and payload; map values are never
  converted through Debug formatting. Preserving these inputs does not imply
  every comparison/operator accepts them; schema/operator admission decides.
- Rust callers constructing FilterValue variants migrate to InputValue
  constructors. Serialized payloads use the existing PublicValue representation
  (including Text rather than the old String variant). This is a hard cut.

## Semantic decisions to enforce

A typed value must reach schema admission without display/parse conversion.
Text remains an authored text value. Existing intentional schema-aware text
admission (for example enum variant names) must be inventoried and either retained
as an explicit conversion rule or rejected as part of the documented hard cut;
it must not be accidentally changed by a generic runtime conversion fallback.
SQL's numeric token inference remains SQL-owned during this slice.

Use accepted field/element contracts for enum-bearing values. The existing
`try_into_runtime_non_enum` deliberately cannot lower enums without authority;
returning an error or stripping their identity is not an acceptable substitute.
Compare, membership and collection-element admission must use the correct
expected contract and preserve coercion/null behavior. Invalid operator/type
combinations must fail with typed diagnostics before planning/execution.

## Complete propagation required

- FilterExpr literal fields and FieldRef constructors in Core; exports through
  Core and the facade/prelude; facade compile fixtures.
- Request admission traverses all PublicValue children, including map keys and
  values and enum payloads. Preserve depth/node/payload bounds before recursion.
- Dynamic request cleanup detaches all recursive public-value children after
  early refusal; list-only cleanup is insufficient for the new contract.
- Lowering copies under the existing preparation budget and resolves enum
  identity through accepted schema. Do not use uncharged deep clones.
- Filter serialization fixtures, literal conversion tests, schema coercion tests,
  dynamic/typed callers and current documentation adopt the single new contract.

## Qualification and reviewable outcome

Run focused Core/facade caller checks and tests for scalar and recursive literal
round trips, exact scalar values, typed/dynamic equivalence, enum path/payload,
map/list structure, membership, collection containment and operator refusals.
Preserve over-depth cleanup, exact-limit/limit-plus-one input admission and
preparation-watermark tests. Include old stringification failure cases as positive
tests of maintained typed behavior, not tests banning a retired symbol.

The first slice is done only when no production FilterValue representation or
stringification route remains, callers compile, and the selected boundary tests
pass. Report net production/test code delta and removed mechanisms. Do not claim
Wasm/IC savings until matched measurements exist. Broad release gates remain
explicitly selected user-owned validation.

## Implementation outcome

Slice 1 removes FilterValue and its stringification conversions; InputValue is
now used by filter constructors, serialized filters and facade callers. Typed
scalars retain their kind, Unit stays distinct from Null, and enum identity is
resolved against accepted field or collection-element authority. Existing
schema-aware text admission remains explicit. SQL token inference is unchanged.
Request admission and iterative rejection cleanup now cover maps and enum
payloads. Preparation charges recursive copying and map canonicalization before
materialization. Serialized filters use PublicValue's representation, with no
legacy decoder; callers must regenerate old serialized filter requests.

Focused validation: 505 query tests and 52 schema-info tests pass. Strict Core
Clippy passes with SQL, migration and metrics enabled. The facade and its dynamic
query compile fixture are checked separately. Broad release gates are skipped
user-owned validation; Wasm bytes, cycles and instructions are unmeasured.

The slice adds 47 production and 82 test Rust code lines (net +129, measured by
cloc against the pre-slice versions of affected files; unrelated edits excluded).
Complexity decreases by one public literal representation and its lossy
formatting conversion route. New bounded map/enum traversal accounts for the
production increase; this is not claimed as a size reduction. There is no new
generic abstraction. The larger duplicated validation and reconstruction flows
remain for slices 2–4 in the [status tracker](../../design/0.310-query-admission/status.md).

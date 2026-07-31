# IcyDB Model Macros

Procedural-macro compiler for `icydb-model` declarations and application
helper derives.

Applications normally consume these macros through `icydb-model`. The compiler
lowers declarations into that package's host graph and the public
`icydb-schema` proposal vocabulary; it has no dependency on `icydb` or
`icydb-core`.

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

References:

- Workspace overview: `../../README.md`
- Release notes: `../../CHANGELOG.md`

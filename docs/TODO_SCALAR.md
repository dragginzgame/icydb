# Scalar Semantics Unification Audit (icydb)

## Summary
Scalar semantics are duplicated across `ScalarType`, `Value`, and planner/validator logic. The highest drift risks are numeric classification and keyability/orderability lists, which are not compiler‑enforced to remain in sync. A single registry can generate the core mappings with zero runtime cost, but it must preserve current behavior, including known inconsistencies (e.g., `Date` is “numeric” by family but excluded from `Value::is_numeric` and `cmp_numeric`).

## Phase 1 — Inventory & Cross-References

### Scalar semantics definitions
- `ScalarType` definition, family, matches, orderable, keyable:
  - `crates/icydb-core/src/db/query/predicate/validate.rs:27-199`
- `field_type_from_model_kind` mapping:
  - `crates/icydb-core/src/db/query/predicate/validate.rs:808-843`
- `Value::is_numeric`:
  - `crates/icydb-core/src/value/mod.rs:131-148`
- `CoercionFamily` + `CoercionFamilyExt` mapping:
  - `crates/icydb-core/src/value/family.rs:17-67`
  - `crates/icydb-core/src/value/mod.rs:531-573`
- Numeric comparison support (`to_decimal`, `to_f64_lossless`, `cmp_numeric`):
  - `crates/icydb-core/src/value/mod.rs:223-277`
- Strict ordering semantics:
  - `crates/icydb-core/src/db/query/predicate/coercion.rs:214-246`
- `Value::partial_cmp` ordering list:
  - `crates/icydb-core/src/value/mod.rs:596-622`
- Keyability sets:
  - `FieldType::is_keyable`: `crates/icydb-core/src/db/query/predicate/validate.rs:185-199`
  - `Value::as_storage_key`: `crates/icydb-core/src/value/mod.rs:186-200`

### Predicate validation logic branching on scalar type/family
- `IsEmpty`/`IsNotEmpty`: `FieldType::is_text`/`is_collection`
  - `crates/icydb-core/src/db/query/predicate/validate.rs:388-401`
- `Eq`/`Ne` list vs map vs scalar:
  - `crates/icydb-core/src/db/query/predicate/validate.rs:454-468`
- Ordering checks:
  - `crates/icydb-core/src/db/query/predicate/validate.rs:471-492`
- `In` rejects collection field types:
  - `crates/icydb-core/src/db/query/predicate/validate.rs:494-516`
- `Contains` rejects text, requires list/set:
  - `crates/icydb-core/src/db/query/predicate/validate.rs:518-551`
- Text ops require text:
  - `crates/icydb-core/src/db/query/predicate/validate.rs:553-639`
- Coercion family checks:
  - `crates/icydb-core/src/db/query/predicate/validate.rs:713-739`

### Call sites relying on numeric classification
- Numeric coercion gating by family:
  - `crates/icydb-core/src/db/query/predicate/coercion.rs:94-121`
- Numeric comparisons via `cmp_numeric`:
  - `crates/icydb-core/src/db/query/predicate/coercion.rs:148-175`
  - `crates/icydb-core/src/value/mod.rs:270-277`
- `Value::is_numeric` currently has no call sites outside its definition.

### Call sites relying on scalar ↔ Value compatibility
- `literal_matches_type` uses `ScalarType::matches_value`:
  - `crates/icydb-core/src/db/query/predicate/validate.rs:785-799`
- `literal_matches_type` call sites:
  - Predicate validation: `crates/icydb-core/src/db/query/predicate/validate.rs:741-781`
  - Plan validation: `crates/icydb-core/src/db/query/plan/validate.rs:420-513`
  - Plan invariants: `crates/icydb-core/src/db/query/plan/invariants.rs:106-188`, `:446-474`
  - Planner: `crates/icydb-core/src/db/query/plan/planner.rs:186-258`

### Call sites relying on scalar family grouping
- `FieldType::coercion_family` (`ScalarType::coercion_family`) and `Value::coercion_family` in coercion validation:
  - `crates/icydb-core/src/db/query/predicate/validate.rs:713-739`
- Coercion family rules:
  - `crates/icydb-core/src/db/query/predicate/coercion.rs:94-114`

### Scalar semantics coverage table

Columns:
- Family and matches from `ScalarType::coercion_family` and `ScalarType::matches_value`.
- Numeric? from `Value::is_numeric`.
- “Used by” refers to the literal/type validation chain used by predicates/plans.

| ScalarType | Family | Numeric? | Matches Value variant | Value::is_numeric? | Used by (files:lines) |
| --- | --- | --- | --- | --- | --- |
| Account | Identifier | No | `Value::Account` | No | `validate.rs:713-799`, `plan/validate.rs:420-513`, `plan/planner.rs:186-258` |
| Blob | Blob | No | `Value::Blob` | No | same as above |
| Bool | Bool | No | `Value::Bool` | No | same as above |
| Date | Numeric | Yes | `Value::Date` | No | same as above |
| Decimal | Numeric | Yes | `Value::Decimal` | Yes | same as above |
| Duration | Numeric | Yes | `Value::Duration` | Yes | same as above |
| Enum | Enum | No | `Value::Enum` | No | same as above |
| E8s | Numeric | Yes | `Value::E8s` | Yes | same as above |
| E18s | Numeric | Yes | `Value::E18s` | Yes | same as above |
| Float32 | Numeric | Yes | `Value::Float32` | Yes | same as above |
| Float64 | Numeric | Yes | `Value::Float64` | Yes | same as above |
| Int | Numeric | Yes | `Value::Int` | Yes | same as above |
| Int128 | Numeric | Yes | `Value::Int128` | Yes | same as above |
| IntBig | Numeric | Yes | `Value::IntBig` | No | same as above |
| Principal | Identifier | No | `Value::Principal` | No | same as above |
| Subaccount | Blob | No | `Value::Subaccount` | No | same as above |
| Text | Textual | No | `Value::Text` | No | same as above |
| Timestamp | Numeric | Yes | `Value::Timestamp` | Yes | same as above |
| Uint | Numeric | Yes | `Value::Uint` | Yes | same as above |
| Uint128 | Numeric | Yes | `Value::Uint128` | Yes | same as above |
| UintBig | Numeric | Yes | `Value::UintBig` | No | same as above |
| Ulid | Identifier | No | `Value::Ulid` | No | same as above |
| Unit | Unit | No | `Value::Unit` | No | same as above |

### Asymmetries
- `Date`, `IntBig`, `UintBig` are Numeric family in `ScalarType::coercion_family` and `Value::coercion_family`, but excluded from `Value::is_numeric` (`value/mod.rs:131-148`).
- Numeric coercion is gated by family, but numeric comparison uses `cmp_numeric` which excludes `Date` (`value/mod.rs:223-277`).
- `CoercionFamily` docs list Identifier includes Subaccount, but implementation classifies Subaccount as Blob (`value/family.rs:19-25`, `value/mod.rs:562-564`).

## Phase 2 — Risk & Drift Analysis

### Implicit invariants
- Every scalar must be updated in:
  - `ScalarType::coercion_family`
  - `ScalarType::matches_value`
  - `field_type_from_model_kind`
  - `CoercionFamilyExt for Value`
  - numeric classification paths if applicable
- `Value::coercion_family` and `ScalarType::coercion_family` must remain aligned for equivalent variants.
- `Value::is_numeric` must be kept consistent with the intended numeric list.

### Highest-risk duplication points
1. Numeric classification drift:
   - `Value::is_numeric` vs `CoercionFamily::Numeric` vs `numeric_repr`.
2. Keyability split:
   - `FieldType::is_keyable` vs `Value::as_storage_key`.
3. Orderability split:
   - `ScalarType::is_orderable` vs strict ordering in `coercion.rs` and `Value::partial_cmp`.

### Subtle semantic differences to preserve
- `Date` is Numeric by family but not by `Value::is_numeric` and not supported by `cmp_numeric`.
- `Unit` is not orderable by `ScalarType::is_orderable` but is orderable in strict ordering.
- Documentation mismatch for Subaccount family.

## Phase 3 — Refactor Design (no code)

### Single-source-of-truth registry
Define a registry list that includes, per scalar:
- `ScalarType` variant
- `Value` variant tag for compatibility
- `CoercionFamily`
- `is_numeric` flag (for `Value::is_numeric`)
- (optional later) `is_keyable`, `is_orderable`, `storage_keyable`

### Generation targets (no runtime cost)
Generate from registry:
- `ScalarType::coercion_family`
- `ScalarType::matches_value`
- `field_type_from_model_kind` for scalar kinds
- `Value::is_numeric`
- (optional) `CoercionFamilyExt for Value` for scalar variants

### Minimal API changes
- Keep `ScalarType` visibility and location; only its internals are generated.
- Keep `Value::is_numeric` signature/behavior identical.
- Keep `field_type_from_model_kind` signature intact.

### Placement tradeoffs
- Avoid coupling `value/` to `model/` by keeping the registry in a neutral module or via shared macros.
- Use `macro_rules!` to avoid runtime cost and share the single list.

### Suggested refactor order
1. Add registry + generate `ScalarType::coercion_family` and `ScalarType::matches_value`.
2. Generate `field_type_from_model_kind` for scalar kinds.
3. Generate `Value::is_numeric` from registry.
4. Optionally generate `CoercionFamilyExt::coercion_family` for scalar variants.

## Phase 4 — Safety & Verification

### Tests / checks to add
- Registry includes every `ScalarType` exactly once (length + uniqueness).
- For each scalar:
  - `ScalarType::matches_value` matches the intended `Value` variant.
  - `ScalarType::coercion_family` matches `Value::coercion_family` for the same variant.
- `Value::is_numeric` matches the current list exactly.

### Behavior that must remain identical
- `ScalarType::coercion_family` and `ScalarType::matches_value` semantics.
- `field_type_from_model_kind` mapping for all scalar kinds.
- `Value::coercion_family` output per variant.
- `Value::is_numeric` list (including current exclusions).
- `literal_matches_type` and all predicate/plan validation behavior.
- Numeric coercion behavior (family gating + `cmp_numeric` limitations).
- Keyability semantics in `FieldType::is_keyable` and `Value::as_storage_key`.

### Behavior that could be normalized later
- Align `Value::is_numeric` with `CoercionFamily::Numeric` by adding `Date`, `IntBig`, `UintBig`.
- Align `ScalarType::is_orderable` with strict ordering for `Unit`.
- Fix Subaccount family doc or classification.

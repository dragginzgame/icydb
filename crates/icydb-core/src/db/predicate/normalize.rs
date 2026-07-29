//! Module: predicate::normalize
//! Responsibility: deterministic predicate normalization and enum-literal adjustment.
//! Does not own: runtime evaluation or schema field-slot resolution.
//! Boundary: normalize before validation/planning/fingerprinting.

use crate::{
    db::predicate::{
        CoercionId, CompareOp, MembershipCompareLeaf, Predicate,
        collapse_membership_compare_leaves, encoding::encode_predicate_sort_key,
        simplify::simplify_and_compare_constraints,
    },
    value::Value,
};
#[cfg(any(test, feature = "query"))]
use crate::{
    db::{
        predicate::{CoercionSpec, ComparePredicate, canonical_membership_value_list},
        schema::{
            AcceptedFieldKind, AcceptedValueAdmissionContract, SchemaInfo,
            SchemaLiteralValidationReason, ValidateError, classify_accepted_field_kind,
            enum_catalog::{ValueAdmissionBudget, ValueAdmissionError},
        },
    },
    types::{IntBig, NatBig, NumericValue},
    value::{InputValue, InputValueEnum, canonicalize_value_set},
};

/// Normalize a predicate into a canonical, deterministic form.
///
/// Normalization guarantees:
/// - Logical equivalence is preserved
/// - Nested AND / OR nodes are flattened
/// - Neutral elements are removed (True / False)
/// - Double negation is eliminated
/// - Child predicates are deterministically ordered
///
/// Note: this pass does not normalize literal values (numeric width, collation).
/// Ordering uses the structural `Value` representation.
///
/// This is used to ensure:
/// - stable planner output
/// - consistent caching / equality checks
/// - predictable test behavior
#[must_use]
pub(in crate::db) fn normalize(predicate: &Predicate) -> Predicate {
    // Normalize recursively while preserving logical equivalence.
    match predicate {
        Predicate::True => Predicate::True,
        Predicate::False => Predicate::False,

        Predicate::And(children) => normalize_and(children),
        Predicate::Or(children) => normalize_or(children),
        Predicate::Not(inner) => normalize_not(inner),

        Predicate::Compare(cmp) => Predicate::Compare(cmp.clone()),
        Predicate::CompareFields(cmp) => Predicate::CompareFields(cmp.clone()),

        Predicate::IsNull { field } => Predicate::IsNull {
            field: field.clone(),
        },
        Predicate::IsNotNull { field } => Predicate::IsNotNull {
            field: field.clone(),
        },
        Predicate::IsMissing { field } => Predicate::IsMissing {
            field: field.clone(),
        },
        Predicate::IsEmpty { field } => Predicate::IsEmpty {
            field: field.clone(),
        },
        Predicate::IsNotEmpty { field } => Predicate::IsNotEmpty {
            field: field.clone(),
        },
        Predicate::TextContains { field, value } => Predicate::TextContains {
            field: field.clone(),
            value: value.clone(),
        },
        Predicate::TextContainsCi { field, value } => Predicate::TextContainsCi {
            field: field.clone(),
            value: value.clone(),
        },
    }
}

/// Normalize an already-owned predicate into the same canonical,
/// deterministic form as [`normalize`] without cloning leaf payloads.
#[must_use]
#[cfg(test)]
fn normalize_owned(predicate: Predicate) -> Predicate {
    // Normalize recursively while preserving logical equivalence.
    match predicate {
        Predicate::True => Predicate::True,
        Predicate::False => Predicate::False,

        Predicate::And(children) => normalize_and_owned(children),
        Predicate::Or(children) => normalize_or_owned(children),
        Predicate::Not(inner) => normalize_not_owned(*inner),

        Predicate::Compare(cmp) => Predicate::Compare(cmp),
        Predicate::CompareFields(cmp) => Predicate::CompareFields(cmp),

        Predicate::IsNull { field } => Predicate::IsNull { field },
        Predicate::IsNotNull { field } => Predicate::IsNotNull { field },
        Predicate::IsMissing { field } => Predicate::IsMissing { field },
        Predicate::IsEmpty { field } => Predicate::IsEmpty { field },
        Predicate::IsNotEmpty { field } => Predicate::IsNotEmpty { field },
        Predicate::TextContains { field, value } => Predicate::TextContains { field, value },
        Predicate::TextContainsCi { field, value } => Predicate::TextContainsCi { field, value },
    }
}

///
/// Normalize enum literals in predicates against schema enum metadata.
///
/// Contract:
/// - strict enum literals (`path = Some`) must match the schema enum path
/// - loose enum literals (`path = None`) are resolved once at filter construction
/// - predicate semantics stay strict at runtime (`Eq` is unchanged)
///
#[cfg(any(test, feature = "query"))]
pub(in crate::db) fn normalize_enum_literals(
    schema: &SchemaInfo,
    predicate: &Predicate,
) -> Result<Predicate, ValidateError> {
    // Enum literal normalization only rewrites enum payload shape, not operators.
    match predicate {
        Predicate::True => Ok(Predicate::True),
        Predicate::False => Ok(Predicate::False),
        Predicate::And(children) => {
            let mut normalized = Vec::with_capacity(children.len());
            for child in children {
                normalized.push(normalize_enum_literals(schema, child)?);
            }

            Ok(Predicate::And(normalized))
        }
        Predicate::Or(children) => {
            let mut normalized = Vec::with_capacity(children.len());
            for child in children {
                normalized.push(normalize_enum_literals(schema, child)?);
            }

            Ok(Predicate::Or(normalized))
        }
        Predicate::Not(inner) => Ok(Predicate::Not(Box::new(normalize_enum_literals(
            schema, inner,
        )?))),
        Predicate::Compare(cmp) => Ok(Predicate::Compare(normalize_compare_with_schema(
            schema, cmp,
        )?)),
        Predicate::CompareFields(cmp) => Ok(Predicate::CompareFields(
            normalize_compare_fields_with_schema(schema, cmp),
        )),
        Predicate::IsNull { field } => Ok(Predicate::IsNull {
            field: field.clone(),
        }),
        Predicate::IsNotNull { field } => Ok(Predicate::IsNotNull {
            field: field.clone(),
        }),
        Predicate::IsMissing { field } => Ok(Predicate::IsMissing {
            field: field.clone(),
        }),
        Predicate::IsEmpty { field } => Ok(Predicate::IsEmpty {
            field: field.clone(),
        }),
        Predicate::IsNotEmpty { field } => Ok(Predicate::IsNotEmpty {
            field: field.clone(),
        }),
        Predicate::TextContains { field, value } => Ok(Predicate::TextContains {
            field: field.clone(),
            value: value.clone(),
        }),
        Predicate::TextContainsCi { field, value } => Ok(Predicate::TextContainsCi {
            field: field.clone(),
            value: value.clone(),
        }),
    }
}

#[cfg(any(test, feature = "query"))]
fn normalize_compare_with_schema(
    schema: &SchemaInfo,
    cmp: &ComparePredicate,
) -> Result<ComparePredicate, ValidateError> {
    if let Some(contract) = schema.accepted_field_contract(&cmp.field) {
        let value = if contract.kind().contains_enum() {
            normalize_compare_value_for_accepted_contract(
                &cmp.field, cmp.op, &cmp.value, &contract,
            )?
        } else {
            normalize_compare_value_for_accepted_kind(
                &cmp.field,
                cmp.op,
                &cmp.value,
                contract.kind(),
                cmp.coercion(),
            )?
        };
        return Ok(ComparePredicate {
            field: cmp.field.clone(),
            op: cmp.op,
            value,
            coercion: cmp.coercion.clone(),
        });
    }

    Ok(cmp.clone())
}

#[cfg(any(test, feature = "query"))]
fn normalize_compare_value_for_accepted_contract(
    field: &str,
    op: CompareOp,
    value: &Value,
    contract: &AcceptedValueAdmissionContract<'_>,
) -> Result<Value, ValidateError> {
    let mut budget = ValueAdmissionBudget::standard();
    match op {
        CompareOp::In | CompareOp::NotIn => {
            let Value::List(values) = value else {
                return Ok(value.clone());
            };
            let mut normalized = Vec::with_capacity(values.len());
            for value in values {
                normalized.push(normalize_accepted_predicate_value(
                    field,
                    value,
                    contract,
                    &mut budget,
                )?);
            }
            let normalized = canonical_membership_value_list(normalized);
            #[cfg(all(feature = "sql", feature = "diagnostics"))]
            if let Value::List(values) = &normalized {
                crate::db::diagnostics::record_sql_membership_normalized(values);
            }
            Ok(normalized)
        }
        CompareOp::Contains => {
            let Some(element_contract) = contract.collection_element_contract() else {
                return Ok(value.clone());
            };
            normalize_accepted_predicate_value(field, value, &element_contract, &mut budget)
        }
        _ => normalize_accepted_predicate_value(field, value, contract, &mut budget),
    }
}

#[cfg(any(test, feature = "query"))]
fn normalize_accepted_predicate_value(
    field: &str,
    value: &Value,
    contract: &AcceptedValueAdmissionContract<'_>,
    budget: &mut ValueAdmissionBudget,
) -> Result<Value, ValidateError> {
    if value.contains_enum() {
        contract
            .with_validated(value, budget, |_| ())
            .map_err(|error| predicate_admission_error(field, error))?;
        return Ok(value.clone());
    }
    let input = match (contract.kind(), value) {
        (AcceptedFieldKind::Enum { .. }, Value::Text(variant)) => {
            InputValue::Enum(InputValueEnum::loose(variant.clone()))
        }
        _ => InputValue::try_from_runtime_non_enum(value).ok_or_else(|| {
            ValidateError::invalid_literal(
                field,
                SchemaLiteralValidationReason::LiteralTypeMismatch,
            )
        })?,
    };
    contract
        .normalize_input_to_runtime(input, budget)
        .map_err(|error| predicate_admission_error(field, error))
}

#[cfg(any(test, feature = "query"))]
fn predicate_admission_error(field: &str, error: ValueAdmissionError) -> ValidateError {
    let reason = match error {
        ValueAdmissionError::EnumPathMismatch => SchemaLiteralValidationReason::EnumPathMismatch,
        ValueAdmissionError::UnknownEnumVariant => {
            SchemaLiteralValidationReason::UnknownEnumVariant
        }
        ValueAdmissionError::EnumBodyMismatch => SchemaLiteralValidationReason::EnumBodyMismatch,
        ValueAdmissionError::DepthExceeded
        | ValueAdmissionError::SizeExceeded
        | ValueAdmissionError::TypeMismatch
        | ValueAdmissionError::ScalarConstraint
        | ValueAdmissionError::EnumTypeMismatch
        | ValueAdmissionError::UnknownEnumType
        | ValueAdmissionError::UnknownCompositeType
        | ValueAdmissionError::CompositeShapeMismatch
        | ValueAdmissionError::CompositeFieldMismatch
        | ValueAdmissionError::DuplicateSetItem
        | ValueAdmissionError::DuplicateMapKey
        | ValueAdmissionError::InvalidAcceptedContract
        | ValueAdmissionError::MissingSchemaRevision => {
            SchemaLiteralValidationReason::LiteralTypeMismatch
        }
    };
    ValidateError::invalid_literal(field, reason)
}

#[cfg(any(test, feature = "query"))]
fn normalize_compare_fields_with_schema(
    schema: &SchemaInfo,
    cmp: &crate::db::predicate::CompareFieldsPredicate,
) -> crate::db::predicate::CompareFieldsPredicate {
    if let (Some(left), Some(right)) = (
        schema.accepted_field_contract(&cmp.left_field),
        schema.accepted_field_contract(&cmp.right_field),
    ) {
        return crate::db::predicate::CompareFieldsPredicate::with_coercion(
            cmp.left_field.clone(),
            cmp.op,
            cmp.right_field.clone(),
            normalize_accepted_compare_fields_coercion(
                cmp.op,
                left.kind(),
                right.kind(),
                cmp.coercion.id,
            ),
        );
    }

    cmp.clone()
}

#[cfg(any(test, feature = "query"))]
const fn normalize_accepted_compare_fields_coercion(
    op: CompareOp,
    left_kind: &AcceptedFieldKind,
    right_kind: &AcceptedFieldKind,
    current: CoercionId,
) -> CoercionId {
    if op.is_equality_family() {
        if classify_accepted_field_kind(left_kind).supports_predicate_numeric_widen()
            && classify_accepted_field_kind(right_kind).supports_predicate_numeric_widen()
        {
            CoercionId::NumericWiden
        } else {
            current
        }
    } else if op.is_ordering_family() {
        if matches!(left_kind, AcceptedFieldKind::Text { .. })
            && matches!(right_kind, AcceptedFieldKind::Text { .. })
        {
            CoercionId::Strict
        } else {
            current
        }
    } else {
        current
    }
}

#[cfg(any(test, feature = "query"))]
fn normalize_compare_value_for_accepted_kind(
    field: &str,
    op: CompareOp,
    value: &Value,
    field_kind: &AcceptedFieldKind,
    coercion: &CoercionSpec,
) -> Result<Value, ValidateError> {
    match op {
        CompareOp::In | CompareOp::NotIn => {
            let Value::List(values) = value else {
                return Ok(value.clone());
            };
            let normalized = normalize_accepted_list_value_for_kind(
                field,
                values.as_slice(),
                field_kind,
                coercion,
                op,
            )?;
            let normalized = canonical_membership_value_list(normalized);
            #[cfg(all(feature = "sql", feature = "diagnostics"))]
            if let Value::List(values) = &normalized {
                crate::db::diagnostics::record_sql_membership_normalized(values);
            }
            Ok(normalized)
        }
        CompareOp::Contains => {
            let element_kind = match field_kind {
                AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => inner.as_ref(),
                _ => return Ok(value.clone()),
            };
            normalize_value_for_accepted_kind(field, value, element_kind, coercion, op)
        }
        _ => normalize_value_for_accepted_kind(field, value, field_kind, coercion, op),
    }
}

#[cfg(any(test, feature = "query"))]
fn normalize_value_for_accepted_kind(
    field: &str,
    value: &Value,
    expected_kind: &AcceptedFieldKind,
    coercion: &CoercionSpec,
    op: CompareOp,
) -> Result<Value, ValidateError> {
    match expected_kind {
        AcceptedFieldKind::Relation { key_kind, .. } => {
            normalize_value_for_accepted_kind(field, value, key_kind, coercion, op)
        }
        AcceptedFieldKind::List(inner) => {
            let Value::List(values) = value else {
                return Ok(value.clone());
            };
            normalize_accepted_list_value_for_kind(field, values.as_slice(), inner, coercion, op)
                .map(Value::List)
        }
        AcceptedFieldKind::Set(inner) => {
            let Value::List(values) = value else {
                return Ok(value.clone());
            };
            let mut normalized = normalize_accepted_list_value_for_kind(
                field,
                values.as_slice(),
                inner,
                coercion,
                op,
            )?;
            canonicalize_value_set(&mut normalized);
            Ok(Value::List(normalized))
        }
        AcceptedFieldKind::Map {
            key,
            value: map_value,
        } => {
            let Value::Map(entries) = value else {
                return Ok(value.clone());
            };
            let mut normalized = Vec::with_capacity(entries.len());
            for (entry_key, entry_value) in entries {
                normalized.push((
                    normalize_value_for_accepted_kind(field, entry_key, key, coercion, op)?,
                    normalize_value_for_accepted_kind(field, entry_value, map_value, coercion, op)?,
                ));
            }
            Ok(Value::Map(normalized))
        }
        AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64
        | AcceptedFieldKind::Int128
        | AcceptedFieldKind::IntBig { .. }
        | AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64
        | AcceptedFieldKind::Nat128
        | AcceptedFieldKind::NatBig { .. } => Ok(normalize_numeric_value_for_accepted_kind(
            value,
            expected_kind,
            coercion,
            op,
        )),
        AcceptedFieldKind::Account
        | AcceptedFieldKind::Blob { .. }
        | AcceptedFieldKind::Bool
        | AcceptedFieldKind::Date
        | AcceptedFieldKind::Decimal { .. }
        | AcceptedFieldKind::Duration
        | AcceptedFieldKind::Enum { .. }
        | AcceptedFieldKind::Float32
        | AcceptedFieldKind::Float64
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Text { .. }
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Ulid
        | AcceptedFieldKind::Unit
        | AcceptedFieldKind::Composite { .. } => Ok(value.clone()),
    }
}

#[cfg(any(test, feature = "query"))]
fn normalize_accepted_list_value_for_kind(
    field: &str,
    values: &[Value],
    expected_kind: &AcceptedFieldKind,
    coercion: &CoercionSpec,
    op: CompareOp,
) -> Result<Vec<Value>, ValidateError> {
    let mut normalized = Vec::with_capacity(values.len());
    for item in values {
        normalized.push(normalize_value_for_accepted_kind(
            field,
            item,
            expected_kind,
            coercion,
            op,
        )?);
    }
    Ok(normalized)
}

// Canonicalize equality-like numeric literals onto the runtime field kind so
// planner identity does not depend on parser-chosen integer wrappers. Ordered
// NumericWiden comparisons keep their original transport shape because their
// literal wrapper is still part of the current planner contract.
#[cfg(any(test, feature = "query"))]
fn normalize_numeric_value_for_accepted_kind(
    value: &Value,
    expected_kind: &AcceptedFieldKind,
    coercion: &CoercionSpec,
    op: CompareOp,
) -> Value {
    let target = match expected_kind {
        AcceptedFieldKind::Int64 => Some(PredicateNumericTarget::Int64),
        AcceptedFieldKind::Int128 => Some(PredicateNumericTarget::Int128),
        AcceptedFieldKind::IntBig { .. } => Some(PredicateNumericTarget::IntBig),
        AcceptedFieldKind::Nat64 => Some(PredicateNumericTarget::Nat64),
        AcceptedFieldKind::Nat128 => Some(PredicateNumericTarget::Nat128),
        AcceptedFieldKind::NatBig { .. } => Some(PredicateNumericTarget::NatBig),
        _ => None,
    };
    normalize_numeric_value_for_target(value, target, coercion, op)
}

#[derive(Clone, Copy)]
#[cfg(any(test, feature = "query"))]
enum PredicateNumericTarget {
    Int64,
    Int128,
    IntBig,
    Nat64,
    Nat128,
    NatBig,
}

#[cfg(any(test, feature = "query"))]
fn normalize_numeric_value_for_target(
    value: &Value,
    target: Option<PredicateNumericTarget>,
    coercion: &CoercionSpec,
    op: CompareOp,
) -> Value {
    if matches!(coercion.id, CoercionId::NumericWiden)
        && matches!(
            op,
            CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte
        )
    {
        return value.clone();
    }

    if !value.supports_numeric_coercion() {
        return value.clone();
    }

    let normalized = match target {
        Some(PredicateNumericTarget::Int64) => value
            .to_numeric_decimal()
            .and_then(<i64 as NumericValue>::try_from_decimal)
            .map(Value::Int64),
        Some(PredicateNumericTarget::Int128) => value
            .to_numeric_decimal()
            .and_then(<i128 as NumericValue>::try_from_decimal)
            .map(Value::Int128),
        Some(PredicateNumericTarget::IntBig) => value
            .to_numeric_decimal()
            .and_then(<IntBig as NumericValue>::try_from_decimal)
            .map(Value::IntBig),
        Some(PredicateNumericTarget::Nat64) => value
            .to_numeric_decimal()
            .and_then(<u64 as NumericValue>::try_from_decimal)
            .map(Value::Nat64),
        Some(PredicateNumericTarget::Nat128) => value
            .to_numeric_decimal()
            .and_then(<u128 as NumericValue>::try_from_decimal)
            .map(Value::Nat128),
        Some(PredicateNumericTarget::NatBig) => value
            .to_numeric_decimal()
            .and_then(<NatBig as NumericValue>::try_from_decimal)
            .map(Value::NatBig),
        None => None,
    };

    normalized.unwrap_or_else(|| value.clone())
}

///
/// Normalize a NOT expression.
///
/// Eliminates double negation:
///     NOT (NOT x)  →  x
///
fn normalize_not(inner: &Predicate) -> Predicate {
    normalize_not_from_normalized(normalize(inner), |predicate| normalize(&predicate))
}

#[cfg(test)]
fn normalize_not_owned(inner: Predicate) -> Predicate {
    normalize_not_from_normalized(normalize_owned(inner), normalize_owned)
}

fn normalize_not_from_normalized(
    normalized: Predicate,
    normalize_double_inner: impl FnOnce(Predicate) -> Predicate,
) -> Predicate {
    if let Predicate::Not(double) = normalized {
        return normalize_double_inner(*double);
    }
    Predicate::Not(Box::new(normalized))
}

///
/// Normalize an AND expression.
///
/// Rules:
/// - AND(True, x)        → x
/// - AND(False, x)       → False
/// - AND(AND(a, b), c)   → AND(a, b, c)
/// - AND()               → True
///
/// Children are sorted deterministically.
///
fn normalize_and(children: &[Predicate]) -> Predicate {
    normalize_and_from_normalized(children.iter().map(normalize))
}

#[cfg(test)]
fn normalize_and_owned(children: Vec<Predicate>) -> Predicate {
    normalize_and_from_normalized(children.into_iter().map(normalize_owned))
}

fn normalize_and_from_normalized(
    normalized_children: impl IntoIterator<Item = Predicate>,
) -> Predicate {
    let mut out = Vec::new();

    for normalized in normalized_children {
        match normalized {
            Predicate::True => {}
            Predicate::False => return Predicate::False,
            Predicate::And(grandchildren) => out.extend(grandchildren),
            other => out.push(other),
        }
    }

    if out.is_empty() {
        return Predicate::True;
    }

    // Compare-pair simplification scans all conjunction children directly, so
    // it does not require a pre-sorted shape to preserve semantics.
    let Some(mut out) = simplify_and_compare_constraints(out) else {
        return Predicate::False;
    };

    // Canonicalize after simplification because compare folding can replace or
    // remove children and therefore change deterministic evaluation order.
    canonicalize_predicate_children_for_eval(&mut out);

    if out.len() == 1 {
        return out.remove(0);
    }

    Predicate::And(out)
}

///
/// Normalize an OR expression.
///
/// Rules:
/// - OR(False, x)       → x
/// - OR(True, x)        → True
/// - OR(OR(a, b), c)    → OR(a, b, c)
/// - OR()               → False
///
/// Children are sorted deterministically.
///
fn normalize_or(children: &[Predicate]) -> Predicate {
    normalize_or_from_normalized(children.iter().map(normalize))
}

#[cfg(test)]
fn normalize_or_owned(children: Vec<Predicate>) -> Predicate {
    normalize_or_from_normalized(children.into_iter().map(normalize_owned))
}

fn normalize_or_from_normalized(
    normalized_children: impl IntoIterator<Item = Predicate>,
) -> Predicate {
    let mut out = Vec::new();

    for normalized in normalized_children {
        match normalized {
            Predicate::False => {}
            Predicate::True => return Predicate::True,
            Predicate::Or(grandchildren) => out.extend(grandchildren),
            other => out.push(other),
        }
    }

    if out.is_empty() {
        return Predicate::False;
    }

    // Canonicalize disjunction children once before OR-specific rewrites so the
    // collapse-to-IN check sees one deterministic shape.
    canonicalize_predicate_children_for_eval(&mut out);

    // Collapse canonical same-field equality disjunctions into one IN compare
    // at the predicate authority boundary.
    if let Some(collapsed) = collapse_same_field_or_eq_to_in(out.as_slice()) {
        return collapsed;
    }

    if out.len() == 1 {
        return out.remove(0);
    }

    Predicate::Or(out)
}

// Collapse `field = a OR field = b ...` into `field IN [a, b, ...]` when:
// - all children are equality compares
// - all children target the same field
// - all children share one supported coercion family
// - all equality literals are scalar-ish (not list/map payloads)
fn collapse_same_field_or_eq_to_in(children: &[Predicate]) -> Option<Predicate> {
    if children.len() < 2 {
        return None;
    }

    let mut leaves = Vec::with_capacity(children.len());

    for child in children {
        let Predicate::Compare(compare) = child else {
            return None;
        };
        if compare.op != CompareOp::Eq {
            return None;
        }
        if !matches!(
            compare.coercion.id,
            CoercionId::Strict | CoercionId::TextCasefold
        ) {
            return None;
        }
        if !or_eq_compare_value_is_in_safe(&compare.value) {
            return None;
        }
        leaves.push(MembershipCompareLeaf::new(
            compare.field.as_str(),
            compare.value.clone(),
            compare.coercion.id,
        ));
    }

    collapse_membership_compare_leaves(leaves, CompareOp::In).map(Predicate::Compare)
}

// Keep OR->IN canonicalization fail-closed for collection/map literals because
// list-like equality remains a distinct validation/runtime surface from `IN`.
const fn or_eq_compare_value_is_in_safe(value: &Value) -> bool {
    !matches!(value, Value::List(_) | Value::Map(_))
}

// Return a stable heuristic rank for predicate evaluation cost. Lower ranks
// are evaluated first after normalization.
const fn predicate_eval_cost_rank(predicate: &Predicate) -> u8 {
    match predicate {
        Predicate::True | Predicate::False => 0,
        Predicate::Compare(compare) => compare_eval_cost_rank(compare.op),
        Predicate::CompareFields(_)
        | Predicate::IsNull { .. }
        | Predicate::IsNotNull { .. }
        | Predicate::IsMissing { .. }
        | Predicate::IsEmpty { .. }
        | Predicate::IsNotEmpty { .. } => 1,
        Predicate::Not(_) => 4,
        Predicate::TextContains { .. } | Predicate::TextContainsCi { .. } => 3,
        Predicate::And(_) | Predicate::Or(_) => 5,
    }
}

const fn compare_eval_cost_rank(op: CompareOp) -> u8 {
    match op {
        CompareOp::Eq
        | CompareOp::Ne
        | CompareOp::Lt
        | CompareOp::Lte
        | CompareOp::Gt
        | CompareOp::Gte => 1,
        CompareOp::In | CompareOp::NotIn => 2,
        CompareOp::Contains | CompareOp::StartsWith | CompareOp::EndsWith => 3,
    }
}

// Canonicalize predicate child ordering for deterministic normalization and
// cheap-first short-circuit behavior.
fn canonicalize_predicate_children_for_eval(out: &mut Vec<Predicate>) {
    out.sort_by(canonical_cmp_predicate_for_eval);
    out.dedup();
}

// Compare predicate children with the same deterministic rank-first ordering
// used by normalization, without routing through the cached-key tuple surface.
fn canonical_cmp_predicate_for_eval(left: &Predicate, right: &Predicate) -> std::cmp::Ordering {
    let rank = predicate_eval_cost_rank(left).cmp(&predicate_eval_cost_rank(right));
    if rank != std::cmp::Ordering::Equal {
        return rank;
    }

    sort_key(left).cmp(&sort_key(right))
}

///
/// Generate a deterministic, length-prefixed key for a predicate.
///
/// This key is used **only for sorting**, not for display.
/// Ordering ensures:
/// - planner determinism
/// - stable normalization
/// - predictable equality
///
fn sort_key(predicate: &Predicate) -> Vec<u8> {
    encode_predicate_sort_key(predicate)
}

///
/// TESTS
///

#[cfg(test)]
mod tests;

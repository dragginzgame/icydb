//! Module: predicate::normalize
//! Responsibility: deterministic predicate normalization and enum-literal adjustment.
//! Does not own: runtime evaluation or schema field-slot resolution.
//! Boundary: normalize before validation/planning/fingerprinting.

mod admission;

use crate::{
    db::{
        QueryError,
        predicate::{
            CoercionId, CoercionSpec, CompareOp, ComparePredicate, Predicate,
            canonical_membership_value_list,
            encoding::write_predicate_sort_key,
            membership::{membership_compare_domain, membership_compare_from_values},
            normalize::admission::admit_enum_input_construction,
            simplify::simplify_and_compare_constraints,
        },
        query::preparation::PreparationWork,
        schema::{
            AcceptedFieldKind, AcceptedValueAdmissionContract, SchemaInfo,
            SchemaLiteralValidationReason, ValidateError, classify_accepted_field_kind,
            enum_catalog::{ValueAdmissionBudget, ValueAdmissionError},
        },
    },
    types::{IntBig, NatBig, NumericValue},
    value::{InputValue, Value, canonicalize_value_set},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

/// Consume a predicate into canonical, deterministic form without copying leaves.
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
pub(in crate::db) fn normalize(predicate: Predicate) -> Predicate {
    // Normalize recursively while preserving logical equivalence.
    match predicate {
        Predicate::True => Predicate::True,
        Predicate::False => Predicate::False,

        Predicate::And(children) => normalize_and(children),
        Predicate::Or(children) => normalize_or(children),
        Predicate::Not(inner) => normalize_not(inner),

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

/// Materialize schema-normalized operands under current request admission.
///
/// Accepted catalog identity and numeric/coercion rules remain authoritative.
/// Visit every supplied child before boolean simplification can discard it.
/// Input depth must be admitted before entering this recursive owner.
pub(in crate::db) fn normalize_enum_literals(
    schema: &SchemaInfo,
    predicate: &Predicate,
    work: &PreparationWork<'_>,
) -> Result<Predicate, QueryError> {
    work.charge(Resource::PredicateExpressionSteps, 1)?;
    match predicate {
        Predicate::And(children) => work
            .copy_slice(children, |child| {
                normalize_enum_literals(schema, child, work)
            })
            .map(Predicate::And),
        Predicate::Or(children) => work
            .copy_slice(children, |child| {
                normalize_enum_literals(schema, child, work)
            })
            .map(Predicate::Or),
        Predicate::Not(inner) => {
            work.charge(Resource::TemporaryBytes, size_of::<Predicate>() as u64)?;
            Ok(Predicate::Not(Box::new(normalize_enum_literals(
                schema, inner, work,
            )?)))
        }
        Predicate::Compare(cmp) => {
            let value = if let Some(contract) = schema.accepted_field_contract(&cmp.field)
                && let Some(kind) = schema.accepted_query_field_kind(&cmp.field)
            {
                if kind.contains_enum() {
                    normalize_compare_value_for_accepted_contract(
                        &cmp.field, cmp.op, &cmp.value, &contract, kind, work,
                    )?
                } else {
                    normalize_compare_value_for_accepted_kind(
                        &cmp.field,
                        cmp.op,
                        &cmp.value,
                        kind,
                        cmp.coercion(),
                        work,
                    )?
                }
            } else {
                work.copy_value(&cmp.value)?
            };
            Ok(Predicate::Compare(ComparePredicate {
                field: work.copy_text(&cmp.field)?,
                op: cmp.op,
                value,
                coercion: work.copy_coercion(&cmp.coercion)?,
            }))
        }
        Predicate::CompareFields(cmp) => Ok(Predicate::CompareFields(
            normalize_compare_fields_with_schema(schema, cmp, work)?,
        )),
        // Unchanged leaves share the admitted syntax-copy owner. It owns their
        // visit charge; the structural dispatch charge above is separate.
        _ => work.copy_predicate(predicate),
    }
}

fn normalize_compare_value_for_accepted_contract(
    field: &str,
    op: CompareOp,
    value: &Value,
    contract: &AcceptedValueAdmissionContract<'_>,
    query_kind: &AcceptedFieldKind,
    work: &PreparationWork<'_>,
) -> Result<Value, QueryError> {
    let mut budget = ValueAdmissionBudget::standard();
    match op {
        CompareOp::In | CompareOp::NotIn => {
            let Value::List(values) = value else {
                return work.copy_value(value);
            };
            work.charge(Resource::NestedValueSteps, 1)?;
            let mut normalized = work.vec_with_capacity(values.len())?;
            for value in values {
                normalized.push(normalize_accepted_predicate_value(
                    field,
                    value,
                    contract,
                    query_kind,
                    &mut budget,
                    work,
                )?);
            }
            let normalized = canonical_membership_value_list(normalized);
            Ok(normalized)
        }
        CompareOp::Contains => {
            let Some(element_contract) = contract.collection_element_contract() else {
                return work.copy_value(value);
            };
            let element_kind = match query_kind {
                AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => inner.as_ref(),
                _ => return work.copy_value(value),
            };
            normalize_accepted_predicate_value(
                field,
                value,
                &element_contract,
                element_kind,
                &mut budget,
                work,
            )
        }
        _ => normalize_accepted_predicate_value(
            field,
            value,
            contract,
            query_kind,
            &mut budget,
            work,
        ),
    }
}

fn normalize_accepted_predicate_value(
    field: &str,
    value: &Value,
    contract: &AcceptedValueAdmissionContract<'_>,
    query_kind: &AcceptedFieldKind,
    budget: &mut ValueAdmissionBudget,
    work: &PreparationWork<'_>,
) -> Result<Value, QueryError> {
    if admit_enum_input_construction(value, work)? {
        contract
            .with_validated(value, budget, |_| ())
            .map_err(|error| predicate_admission_error(field, error))?;
        return work.copy_value(value);
    }
    let input = match (query_kind, value) {
        (AcceptedFieldKind::Enum { .. }, Value::Text(variant)) => {
            InputValue::loose_enum(variant.clone())
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
        .map_err(|error| QueryError::from(predicate_admission_error(field, error)))
}

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

fn normalize_compare_fields_with_schema(
    schema: &SchemaInfo,
    cmp: &crate::db::predicate::CompareFieldsPredicate,
    work: &PreparationWork<'_>,
) -> Result<crate::db::predicate::CompareFieldsPredicate, QueryError> {
    if let (Some(left), Some(right)) = (
        schema.accepted_query_field_kind(&cmp.left_field),
        schema.accepted_query_field_kind(&cmp.right_field),
    ) {
        return Ok(crate::db::predicate::CompareFieldsPredicate::with_coercion(
            work.copy_text(&cmp.left_field)?,
            cmp.op,
            work.copy_text(&cmp.right_field)?,
            normalize_accepted_compare_fields_coercion(cmp.op, left, right, cmp.coercion.id),
        ));
    }

    Ok(crate::db::predicate::CompareFieldsPredicate {
        left_field: work.copy_text(&cmp.left_field)?,
        op: cmp.op,
        right_field: work.copy_text(&cmp.right_field)?,
        coercion: work.copy_coercion(&cmp.coercion)?,
    })
}

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

fn normalize_compare_value_for_accepted_kind(
    field: &str,
    op: CompareOp,
    value: &Value,
    field_kind: &AcceptedFieldKind,
    coercion: &CoercionSpec,
    work: &PreparationWork<'_>,
) -> Result<Value, QueryError> {
    match op {
        CompareOp::In | CompareOp::NotIn => {
            let Value::List(values) = value else {
                return work.copy_value(value);
            };
            work.charge(Resource::NestedValueSteps, 1)?;
            let normalized = normalize_accepted_list_value_for_kind(
                field,
                values.as_slice(),
                field_kind,
                coercion,
                op,
                work,
            )?;
            let normalized = canonical_membership_value_list(normalized);
            Ok(normalized)
        }
        CompareOp::Contains => {
            let element_kind = match field_kind {
                AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => inner.as_ref(),
                _ => return work.copy_value(value),
            };
            normalize_value_for_accepted_kind(field, value, element_kind, coercion, op, work)
        }
        _ => normalize_value_for_accepted_kind(field, value, field_kind, coercion, op, work),
    }
}

fn normalize_value_for_accepted_kind(
    field: &str,
    value: &Value,
    expected_kind: &AcceptedFieldKind,
    coercion: &CoercionSpec,
    op: CompareOp,
    work: &PreparationWork<'_>,
) -> Result<Value, QueryError> {
    work.charge(Resource::NestedValueSteps, 1)?;
    match expected_kind {
        AcceptedFieldKind::Relation { key_kind, .. } => {
            normalize_value_for_accepted_kind(field, value, key_kind, coercion, op, work)
        }
        AcceptedFieldKind::List(inner) => {
            let Value::List(values) = value else {
                return work.copy_value(value);
            };
            normalize_accepted_list_value_for_kind(
                field,
                values.as_slice(),
                inner,
                coercion,
                op,
                work,
            )
            .map(Value::List)
        }
        AcceptedFieldKind::Set(inner) => {
            let Value::List(values) = value else {
                return work.copy_value(value);
            };
            let mut normalized = normalize_accepted_list_value_for_kind(
                field,
                values.as_slice(),
                inner,
                coercion,
                op,
                work,
            )?;
            canonicalize_value_set(&mut normalized);
            Ok(Value::List(normalized))
        }
        AcceptedFieldKind::Map {
            key,
            value: map_value,
        } => {
            let Value::Map(entries) = value else {
                return work.copy_value(value);
            };
            let mut normalized = work.vec_with_capacity(entries.len())?;
            for (entry_key, entry_value) in entries {
                normalized.push((
                    normalize_value_for_accepted_kind(field, entry_key, key, coercion, op, work)?,
                    normalize_value_for_accepted_kind(
                        field,
                        entry_value,
                        map_value,
                        coercion,
                        op,
                        work,
                    )?,
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
        | AcceptedFieldKind::NatBig { .. } => {
            normalize_numeric_value_for_accepted_kind(value, expected_kind, coercion, op, work)
        }
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
        | AcceptedFieldKind::U256
        | AcceptedFieldKind::Composite { .. } => work.copy_value(value),
    }
}

fn normalize_accepted_list_value_for_kind(
    field: &str,
    values: &[Value],
    expected_kind: &AcceptedFieldKind,
    coercion: &CoercionSpec,
    op: CompareOp,
    work: &PreparationWork<'_>,
) -> Result<Vec<Value>, QueryError> {
    let mut normalized = work.vec_with_capacity(values.len())?;
    for item in values {
        normalized.push(normalize_value_for_accepted_kind(
            field,
            item,
            expected_kind,
            coercion,
            op,
            work,
        )?);
    }
    Ok(normalized)
}

// Canonicalize equality-like numeric literals onto the runtime field kind so
// planner identity does not depend on parser-chosen integer wrappers. Ordered
// NumericWiden comparisons keep their original transport shape because their
// literal wrapper is still part of the current planner contract.
fn normalize_numeric_value_for_accepted_kind(
    value: &Value,
    expected_kind: &AcceptedFieldKind,
    coercion: &CoercionSpec,
    op: CompareOp,
    work: &PreparationWork<'_>,
) -> Result<Value, QueryError> {
    let target = match expected_kind {
        AcceptedFieldKind::Int64 => Some(PredicateNumericTarget::Int64),
        AcceptedFieldKind::Int128 => Some(PredicateNumericTarget::Int128),
        AcceptedFieldKind::IntBig { .. } => Some(PredicateNumericTarget::IntBig),
        AcceptedFieldKind::Nat64 => Some(PredicateNumericTarget::Nat64),
        AcceptedFieldKind::Nat128 => Some(PredicateNumericTarget::Nat128),
        AcceptedFieldKind::NatBig { .. } => Some(PredicateNumericTarget::NatBig),
        _ => None,
    };
    normalize_numeric_value_for_target(value, target, coercion, op, work)
}

#[derive(Clone, Copy)]
enum PredicateNumericTarget {
    Int64,
    Int128,
    IntBig,
    Nat64,
    Nat128,
    NatBig,
}

fn normalize_numeric_value_for_target(
    value: &Value,
    target: Option<PredicateNumericTarget>,
    coercion: &CoercionSpec,
    op: CompareOp,
    work: &PreparationWork<'_>,
) -> Result<Value, QueryError> {
    if matches!(coercion.id, CoercionId::NumericWiden)
        && matches!(
            op,
            CompareOp::Lt | CompareOp::Lte | CompareOp::Gt | CompareOp::Gte
        )
    {
        return work.copy_value(value);
    }

    if !value.supports_numeric_coercion() {
        return work.copy_value(value);
    }

    // Decimal conversion is fixed-width. A successful bigint result is at most
    // 128 bits; allow small-Vec limb backing before constructing it.
    if matches!(
        target,
        Some(PredicateNumericTarget::IntBig | PredicateNumericTarget::NatBig)
    ) {
        work.charge(Resource::TemporaryBytes, 64)?;
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

    match normalized {
        Some(value) => Ok(value),
        None => work.copy_value(value),
    }
}

///
/// Normalize a NOT expression.
///
/// Eliminates double negation:
///     NOT (NOT x)  →  x
///
fn normalize_not(mut inner: Box<Predicate>) -> Predicate {
    // Keep the owned shell when NOT survives. Moving its child through the
    // normalizer must not require allocating a replacement box.
    *inner = normalize(*inner);
    if let Predicate::Not(double) = *inner {
        // The inner subtree is already canonical; removing both NOT nodes
        // must not traverse, sort or rebuild it a second time.
        return *double;
    }
    Predicate::Not(inner)
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
fn normalize_and(children: Vec<Predicate>) -> Predicate {
    let mut out = Vec::new();

    for normalized in children.into_iter().map(normalize) {
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
fn normalize_or(children: Vec<Predicate>) -> Predicate {
    let mut out = Vec::new();

    for normalized in children.into_iter().map(normalize) {
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

    // Eligible equalities only need the membership owner's value ordering, not
    // a preceding sort of complete predicate keys. Other OR shapes still sort.
    if let Some(collapsed) = collapse_same_field_or_equalities(&mut out) {
        return collapsed;
    }

    canonicalize_predicate_children_for_eval(&mut out);

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
fn collapse_same_field_or_equalities(children: &mut Vec<Predicate>) -> Option<Predicate> {
    if children.len() < 2 {
        return None;
    }

    // Prove compatibility without allocating or consuming any child. Failed
    // collapse must leave the authored order and operands intact for sorting.
    let (_, coercion) = membership_compare_domain(children.iter().map(|child| {
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
        Some((compare.field.as_str(), compare.coercion.id))
    }))?;

    // The ordinary sort/dedup would retain the first identical leaf. Keep that
    // equality (and its complete coercion metadata), not a singleton IN list.
    if children.windows(2).all(|pair| pair[0] == pair[1]) {
        return children.drain(..).next();
    }

    let Predicate::Compare(first) = children.first_mut()? else {
        return None;
    };
    let field = std::mem::take(&mut first.field);
    // Preflight established every variant and the minimum count. Draining
    // moves values; no comparison shells or duplicate payloads are retained.
    let mut values = Vec::with_capacity(children.len());
    for child in children.drain(..) {
        if let Predicate::Compare(compare) = child {
            values.push(compare.value);
        }
    }
    Some(Predicate::Compare(membership_compare_from_values(
        field,
        CompareOp::In,
        values,
        coercion,
    )))
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
    // Reuse two buffers for this sort, not one allocation pair per comparison
    // or retained keys for every child. Different ranks need no encoded keys.
    let mut left_key = Vec::new();
    let mut right_key = Vec::new();
    out.sort_by(|left, right| {
        let rank = predicate_eval_cost_rank(left).cmp(&predicate_eval_cost_rank(right));
        if rank != std::cmp::Ordering::Equal {
            return rank;
        }

        left_key.clear();
        right_key.clear();
        write_predicate_sort_key(&mut left_key, left);
        write_predicate_sort_key(&mut right_key, right);
        left_key.cmp(&right_key)
    });
    out.dedup();
}

///
/// TESTS
///

#[cfg(test)]
mod tests;

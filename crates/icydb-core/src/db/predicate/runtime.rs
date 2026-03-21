//! Module: predicate::runtime
//! Responsibility: compile/evaluate slot-resolved predicates against entities.
//! Does not own: schema validation or normalization policy.
//! Boundary: executor row filtering uses this runtime program.

use crate::{
    db::{
        data::{ScalarSlotValueRef, ScalarValueRef, SlotReader, decode_slot_value_by_contract},
        predicate::{
            CoercionSpec, CompareOp, ComparePredicate, Predicate, PredicateExecutionModel,
            ResolvedComparePredicate, ResolvedPredicate, TextOp, compare_eq, compare_order,
            compare_text,
        },
    },
    model::{
        entity::{EntityModel, resolve_field_slot},
        field::LeafCodec,
    },
    traits::EntityKind,
    value::{TextMode, Value},
};
use std::cmp::Ordering;

///
/// PredicateProgram
///
/// Slot-resolved predicate program for runtime row filtering.
/// Field names are resolved once during setup; evaluation is slot-only.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct PredicateProgram {
    resolved: ResolvedPredicate,
}

impl PredicateProgram {
    /// Compile a predicate into a slot-based executable form.
    #[must_use]
    pub(in crate::db) fn compile<E: EntityKind>(predicate: &PredicateExecutionModel) -> Self {
        let resolved = compile_predicate_program(E::MODEL, predicate);

        Self { resolved }
    }

    /// Compile a predicate into a slot-based executable form using structural model data only.
    #[must_use]
    pub(in crate::db) fn compile_with_model(
        model: &'static EntityModel,
        predicate: &PredicateExecutionModel,
    ) -> Self {
        let resolved = compile_predicate_program(model, predicate);

        Self { resolved }
    }

    /// Evaluate one precompiled predicate program against one slot-reader callback.
    #[must_use]
    pub(in crate::db) fn eval_with_slot_reader(
        &self,
        read_slot: &mut dyn FnMut(usize) -> Option<Value>,
    ) -> bool {
        eval_with_resolved_slots(&self.resolved, read_slot)
    }

    /// Evaluate one precompiled predicate program against one structural slot reader.
    pub(in crate::db) fn eval_with_structural_slot_reader(
        &self,
        slots: &mut dyn SlotReader,
    ) -> Result<bool, crate::error::InternalError> {
        eval_with_structural_slots(&self.resolved, slots)
    }

    /// Borrow the resolved predicate tree used by runtime evaluators.
    #[must_use]
    pub(in crate::db) const fn resolved(&self) -> &ResolvedPredicate {
        &self.resolved
    }
}

///
/// FieldPresence
///
/// Result of attempting to read a field from an entity during slot-based
/// predicate evaluation.
///

enum FieldPresence {
    Present(Value),
    Missing,
}

/// Compile field-name predicates to stable field-slot predicates once per query.
fn compile_predicate_program(
    model: &'static EntityModel,
    predicate: &PredicateExecutionModel,
) -> ResolvedPredicate {
    fn resolve_field(model: &'static EntityModel, field_name: &str) -> Option<usize> {
        resolve_field_slot(model, field_name)
    }

    // Compile field-name predicates into slot-index predicates once per query.
    match predicate {
        Predicate::True => ResolvedPredicate::True,
        Predicate::False => ResolvedPredicate::False,
        Predicate::And(children) => ResolvedPredicate::And(
            children
                .iter()
                .map(|child| compile_predicate_program(model, child))
                .collect::<Vec<_>>(),
        ),
        Predicate::Or(children) => ResolvedPredicate::Or(
            children
                .iter()
                .map(|child| compile_predicate_program(model, child))
                .collect::<Vec<_>>(),
        ),
        Predicate::Not(inner) => {
            ResolvedPredicate::Not(Box::new(compile_predicate_program(model, inner)))
        }
        Predicate::Compare(ComparePredicate {
            field,
            op,
            value,
            coercion,
        }) => ResolvedPredicate::Compare(ResolvedComparePredicate {
            field_slot: resolve_field(model, field),
            op: *op,
            value: value.clone(),
            coercion: coercion.clone(),
        }),
        Predicate::IsNull { field } => ResolvedPredicate::IsNull {
            field_slot: resolve_field(model, field),
        },
        Predicate::IsNotNull { field } => ResolvedPredicate::IsNotNull {
            field_slot: resolve_field(model, field),
        },
        Predicate::IsMissing { field } => ResolvedPredicate::IsMissing {
            field_slot: resolve_field(model, field),
        },
        Predicate::IsEmpty { field } => ResolvedPredicate::IsEmpty {
            field_slot: resolve_field(model, field),
        },
        Predicate::IsNotEmpty { field } => ResolvedPredicate::IsNotEmpty {
            field_slot: resolve_field(model, field),
        },
        Predicate::TextContains { field, value } => ResolvedPredicate::TextContains {
            field_slot: resolve_field(model, field),
            value: value.clone(),
        },
        Predicate::TextContainsCi { field, value } => ResolvedPredicate::TextContainsCi {
            field_slot: resolve_field(model, field),
            value: value.clone(),
        },
    }
}

/// Read one field by pre-resolved slot through one runtime slot reader.
fn field_from_slot(
    field_slot: Option<usize>,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> FieldPresence {
    let value = field_slot.and_then(read_slot);

    match value {
        Some(value) => FieldPresence::Present(value),
        None => FieldPresence::Missing,
    }
}

/// Evaluate one slot-based field predicate only when the field is present.
fn on_present_slot(
    field_slot: Option<usize>,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
    f: impl FnOnce(&Value) -> bool,
) -> bool {
    match field_from_slot(field_slot, read_slot) {
        FieldPresence::Present(value) => f(&value),
        FieldPresence::Missing => false,
    }
}

/// Evaluate one slot-resolved predicate against one runtime slot reader.
fn eval_with_resolved_slots(
    predicate: &ResolvedPredicate,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> bool {
    // Evaluate recursively against slot-resolved predicates.
    match predicate {
        ResolvedPredicate::True => true,
        ResolvedPredicate::False => false,
        ResolvedPredicate::And(children) => {
            for child in children {
                if !eval_with_resolved_slots(child, read_slot) {
                    return false;
                }
            }

            true
        }
        ResolvedPredicate::Or(children) => {
            for child in children {
                if eval_with_resolved_slots(child, read_slot) {
                    return true;
                }
            }

            false
        }
        ResolvedPredicate::Not(inner) => !eval_with_resolved_slots(inner, read_slot),
        ResolvedPredicate::Compare(cmp) => eval_compare_with_resolved_slots(cmp, read_slot),
        ResolvedPredicate::IsNull { field_slot } => {
            matches!(
                field_from_slot(*field_slot, read_slot),
                FieldPresence::Present(Value::Null)
            )
        }
        ResolvedPredicate::IsNotNull { field_slot } => {
            matches!(field_from_slot(*field_slot, read_slot), FieldPresence::Present(value) if !matches!(value, Value::Null))
        }
        ResolvedPredicate::IsMissing { field_slot } => {
            matches!(
                field_from_slot(*field_slot, read_slot),
                FieldPresence::Missing
            )
        }
        ResolvedPredicate::IsEmpty { field_slot } => {
            on_present_slot(*field_slot, read_slot, is_empty_value)
        }
        ResolvedPredicate::IsNotEmpty { field_slot } => {
            on_present_slot(*field_slot, read_slot, |value| !is_empty_value(value))
        }
        ResolvedPredicate::TextContains { field_slot, value } => {
            on_present_slot(*field_slot, read_slot, |actual| {
                actual.text_contains(value, TextMode::Cs).unwrap_or(false)
            })
        }
        ResolvedPredicate::TextContainsCi { field_slot, value } => {
            on_present_slot(*field_slot, read_slot, |actual| {
                actual.text_contains(value, TextMode::Ci).unwrap_or(false)
            })
        }
    }
}

/// Evaluate a slot-resolved comparison predicate against one runtime slot reader.
fn eval_compare_with_resolved_slots(
    cmp: &ResolvedComparePredicate,
    read_slot: &mut dyn FnMut(usize) -> Option<Value>,
) -> bool {
    let FieldPresence::Present(actual) = field_from_slot(cmp.field_slot, read_slot) else {
        return false;
    };

    eval_compare_values(&actual, cmp.op, &cmp.value, &cmp.coercion)
}

// Evaluate one slot-resolved predicate against one structural slot reader.
fn eval_with_structural_slots(
    predicate: &ResolvedPredicate,
    slots: &mut dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    match predicate {
        ResolvedPredicate::True => Ok(true),
        ResolvedPredicate::False => Ok(false),
        ResolvedPredicate::And(children) => {
            for child in children {
                if !eval_with_structural_slots(child, slots)? {
                    return Ok(false);
                }
            }

            Ok(true)
        }
        ResolvedPredicate::Or(children) => {
            for child in children {
                if eval_with_structural_slots(child, slots)? {
                    return Ok(true);
                }
            }

            Ok(false)
        }
        ResolvedPredicate::Not(inner) => Ok(!eval_with_structural_slots(inner, slots)?),
        ResolvedPredicate::Compare(cmp) => eval_compare_with_structural_slots(cmp, slots),
        ResolvedPredicate::IsNull { field_slot } => {
            eval_is_null_with_structural_slots(*field_slot, slots)
        }
        ResolvedPredicate::IsNotNull { field_slot } => {
            eval_is_not_null_with_structural_slots(*field_slot, slots)
        }
        ResolvedPredicate::IsMissing { field_slot } => {
            Ok(field_slot.is_none_or(|slot| !slots.has(slot)))
        }
        ResolvedPredicate::IsEmpty { field_slot } => {
            eval_is_empty_with_structural_slots(*field_slot, slots)
        }
        ResolvedPredicate::IsNotEmpty { field_slot } => {
            eval_is_not_empty_with_structural_slots(*field_slot, slots)
        }
        ResolvedPredicate::TextContains { field_slot, value } => {
            eval_text_contains_with_structural_slots(*field_slot, value, TextMode::Cs, slots)
        }
        ResolvedPredicate::TextContainsCi { field_slot, value } => {
            eval_text_contains_with_structural_slots(*field_slot, value, TextMode::Ci, slots)
        }
    }
}

// Evaluate one comparison predicate through the structural slot seam.
fn eval_compare_with_structural_slots(
    cmp: &ResolvedComparePredicate,
    slots: &mut dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    let Some(field_slot) = cmp.field_slot else {
        return Ok(false);
    };
    let Some(field) = slots.model().fields().get(field_slot) else {
        return Ok(false);
    };

    if matches!(field.leaf_codec(), LeafCodec::Scalar(_))
        && let Some(actual) = slots.get_scalar(field_slot)?
        && let Some(result) = eval_compare_scalar_slot(actual, cmp.op, &cmp.value, &cmp.coercion)
    {
        return Ok(result);
    }

    let Some(actual) = decode_slot_value_by_contract(slots, field_slot)? else {
        return Ok(false);
    };

    Ok(eval_compare_values(
        &actual,
        cmp.op,
        &cmp.value,
        &cmp.coercion,
    ))
}

// Evaluate `IS NULL` through the structural slot seam.
fn eval_is_null_with_structural_slots(
    field_slot: Option<usize>,
    slots: &mut dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    let Some(field_slot) = field_slot else {
        return Ok(false);
    };
    let Some(field) = slots.model().fields().get(field_slot) else {
        return Ok(false);
    };

    if matches!(field.leaf_codec(), LeafCodec::Scalar(_)) {
        return Ok(matches!(
            slots.get_scalar(field_slot)?,
            Some(ScalarSlotValueRef::Null)
        ));
    }

    Ok(matches!(
        decode_slot_value_by_contract(slots, field_slot)?,
        Some(Value::Null)
    ))
}

// Evaluate `IS NOT NULL` through the structural slot seam.
fn eval_is_not_null_with_structural_slots(
    field_slot: Option<usize>,
    slots: &mut dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    let Some(field_slot) = field_slot else {
        return Ok(false);
    };
    let Some(field) = slots.model().fields().get(field_slot) else {
        return Ok(false);
    };

    if matches!(field.leaf_codec(), LeafCodec::Scalar(_)) {
        return Ok(matches!(
            slots.get_scalar(field_slot)?,
            Some(ScalarSlotValueRef::Value(_))
        ));
    }

    Ok(matches!(
        decode_slot_value_by_contract(slots, field_slot)?,
        Some(value) if !matches!(value, Value::Null)
    ))
}

// Evaluate `IS EMPTY` through the structural slot seam.
fn eval_is_empty_with_structural_slots(
    field_slot: Option<usize>,
    slots: &mut dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    let Some(field_slot) = field_slot else {
        return Ok(false);
    };
    let Some(field) = slots.model().fields().get(field_slot) else {
        return Ok(false);
    };

    if matches!(field.leaf_codec(), LeafCodec::Scalar(_))
        && let Some(actual) = slots.get_scalar(field_slot)?
    {
        return Ok(match actual {
            ScalarSlotValueRef::Null => false,
            ScalarSlotValueRef::Value(ScalarValueRef::Text(text)) => text.is_empty(),
            ScalarSlotValueRef::Value(ScalarValueRef::Blob(bytes)) => bytes.is_empty(),
            ScalarSlotValueRef::Value(_) => false,
        });
    }

    Ok(decode_slot_value_by_contract(slots, field_slot)?
        .is_some_and(|value| is_empty_value(&value)))
}

// Evaluate `IS NOT EMPTY` through the structural slot seam.
fn eval_is_not_empty_with_structural_slots(
    field_slot: Option<usize>,
    slots: &mut dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    let Some(field_slot) = field_slot else {
        return Ok(false);
    };
    if !slots.has(field_slot) {
        return Ok(false);
    }

    eval_is_empty_with_structural_slots(Some(field_slot), slots).map(|empty| !empty)
}

// Evaluate `TEXT CONTAINS` through the structural slot seam.
fn eval_text_contains_with_structural_slots(
    field_slot: Option<usize>,
    value: &Value,
    mode: TextMode,
    slots: &mut dyn SlotReader,
) -> Result<bool, crate::error::InternalError> {
    let Some(field_slot) = field_slot else {
        return Ok(false);
    };
    let Some(field) = slots.model().fields().get(field_slot) else {
        return Ok(false);
    };

    if matches!(field.leaf_codec(), LeafCodec::Scalar(_))
        && let Some(actual) = slots.get_scalar(field_slot)?
    {
        return Ok(match (actual, value) {
            (ScalarSlotValueRef::Value(ScalarValueRef::Text(actual)), Value::Text(needle)) => {
                text_contains_scalar(actual, needle, mode)
            }
            _ => false,
        });
    }

    Ok(decode_slot_value_by_contract(slots, field_slot)?
        .is_some_and(|actual| actual.text_contains(value, mode).unwrap_or(false)))
}

// Evaluate one compare op directly against one scalar slot value when possible.
fn eval_compare_scalar_slot(
    actual: ScalarSlotValueRef<'_>,
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
) -> Option<bool> {
    match actual {
        ScalarSlotValueRef::Null => Some(eval_compare_values(&Value::Null, op, value, coercion)),
        ScalarSlotValueRef::Value(ScalarValueRef::Text(actual)) => {
            eval_text_scalar_compare(actual, op, value, coercion)
        }
        ScalarSlotValueRef::Value(ScalarValueRef::Blob(_)) => None,
        ScalarSlotValueRef::Value(ScalarValueRef::Bool(actual)) => Some(eval_compare_values(
            &Value::Bool(actual),
            op,
            value,
            coercion,
        )),
        ScalarSlotValueRef::Value(ScalarValueRef::Date(actual)) => Some(eval_compare_values(
            &Value::Date(actual),
            op,
            value,
            coercion,
        )),
        ScalarSlotValueRef::Value(ScalarValueRef::Duration(actual)) => Some(eval_compare_values(
            &Value::Duration(actual),
            op,
            value,
            coercion,
        )),
        ScalarSlotValueRef::Value(ScalarValueRef::Float32(actual)) => Some(eval_compare_values(
            &Value::Float32(actual),
            op,
            value,
            coercion,
        )),
        ScalarSlotValueRef::Value(ScalarValueRef::Float64(actual)) => Some(eval_compare_values(
            &Value::Float64(actual),
            op,
            value,
            coercion,
        )),
        ScalarSlotValueRef::Value(ScalarValueRef::Int(actual)) => Some(eval_compare_values(
            &Value::Int(actual),
            op,
            value,
            coercion,
        )),
        ScalarSlotValueRef::Value(ScalarValueRef::Principal(actual)) => Some(eval_compare_values(
            &Value::Principal(actual),
            op,
            value,
            coercion,
        )),
        ScalarSlotValueRef::Value(ScalarValueRef::Subaccount(actual)) => Some(eval_compare_values(
            &Value::Subaccount(actual),
            op,
            value,
            coercion,
        )),
        ScalarSlotValueRef::Value(ScalarValueRef::Timestamp(actual)) => Some(eval_compare_values(
            &Value::Timestamp(actual),
            op,
            value,
            coercion,
        )),
        ScalarSlotValueRef::Value(ScalarValueRef::Uint(actual)) => Some(eval_compare_values(
            &Value::Uint(actual),
            op,
            value,
            coercion,
        )),
        ScalarSlotValueRef::Value(ScalarValueRef::Ulid(actual)) => Some(eval_compare_values(
            &Value::Ulid(actual),
            op,
            value,
            coercion,
        )),
        ScalarSlotValueRef::Value(ScalarValueRef::Unit) => {
            Some(eval_compare_values(&Value::Unit, op, value, coercion))
        }
    }
}

// Evaluate one scalar text compare without allocating an owned `Value::Text`.
fn eval_text_scalar_compare(
    actual: &str,
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
) -> Option<bool> {
    let mode = match coercion.id {
        crate::db::predicate::CoercionId::Strict => TextMode::Cs,
        crate::db::predicate::CoercionId::TextCasefold => TextMode::Ci,
        _ => return None,
    };

    match op {
        CompareOp::Eq => match value {
            Value::Text(expected) => {
                Some(compare_scalar_text(actual, expected, mode) == Ordering::Equal)
            }
            _ => None,
        },
        CompareOp::Ne => match value {
            Value::Text(expected) => {
                Some(compare_scalar_text(actual, expected, mode) != Ordering::Equal)
            }
            _ => None,
        },
        CompareOp::Lt => match value {
            Value::Text(expected) => Some(compare_scalar_text(actual, expected, mode).is_lt()),
            _ => None,
        },
        CompareOp::Lte => match value {
            Value::Text(expected) => Some(compare_scalar_text(actual, expected, mode).is_le()),
            _ => None,
        },
        CompareOp::Gt => match value {
            Value::Text(expected) => Some(compare_scalar_text(actual, expected, mode).is_gt()),
            _ => None,
        },
        CompareOp::Gte => match value {
            Value::Text(expected) => Some(compare_scalar_text(actual, expected, mode).is_ge()),
            _ => None,
        },
        CompareOp::StartsWith => match value {
            Value::Text(expected) => Some(text_starts_with_scalar(actual, expected, mode)),
            _ => None,
        },
        CompareOp::EndsWith => match value {
            Value::Text(expected) => Some(text_ends_with_scalar(actual, expected, mode)),
            _ => None,
        },
        CompareOp::In => {
            let Value::List(items) = value else {
                return None;
            };
            Some(items.iter().any(|item| {
                matches!(item, Value::Text(expected) if compare_scalar_text(actual, expected, mode) == Ordering::Equal)
            }))
        }
        CompareOp::NotIn => {
            let Value::List(items) = value else {
                return None;
            };
            Some(!items.iter().any(|item| {
                matches!(item, Value::Text(expected) if compare_scalar_text(actual, expected, mode) == Ordering::Equal)
            }))
        }
        CompareOp::Contains => None,
    }
}

fn compare_scalar_text(actual: &str, expected: &str, mode: TextMode) -> Ordering {
    match mode {
        TextMode::Cs => actual.cmp(expected),
        TextMode::Ci => casefold_scalar_text(actual).cmp(&casefold_scalar_text(expected)),
    }
}

fn text_contains_scalar(actual: &str, needle: &str, mode: TextMode) -> bool {
    match mode {
        TextMode::Cs => actual.contains(needle),
        TextMode::Ci => casefold_scalar_text(actual).contains(&casefold_scalar_text(needle)),
    }
}

fn text_starts_with_scalar(actual: &str, prefix: &str, mode: TextMode) -> bool {
    match mode {
        TextMode::Cs => actual.starts_with(prefix),
        TextMode::Ci => casefold_scalar_text(actual).starts_with(&casefold_scalar_text(prefix)),
    }
}

fn text_ends_with_scalar(actual: &str, suffix: &str, mode: TextMode) -> bool {
    match mode {
        TextMode::Cs => actual.ends_with(suffix),
        TextMode::Ci => casefold_scalar_text(actual).ends_with(&casefold_scalar_text(suffix)),
    }
}

fn casefold_scalar_text(input: &str) -> String {
    if input.is_ascii() {
        return input.to_ascii_lowercase();
    }

    input.to_lowercase()
}

/// Shared compare-op semantics for slot-path evaluation.
pub(in crate::db) fn eval_compare_values(
    actual: &Value,
    op: CompareOp,
    value: &Value,
    coercion: &CoercionSpec,
) -> bool {
    // NOTE: Comparison helpers return None when a comparison is invalid; eval treats that as false.
    match op {
        CompareOp::Eq => compare_eq(actual, value, coercion).unwrap_or(false),
        CompareOp::Ne => compare_eq(actual, value, coercion).is_some_and(|v| !v),

        CompareOp::Lt => compare_order(actual, value, coercion).is_some_and(Ordering::is_lt),
        CompareOp::Lte => compare_order(actual, value, coercion).is_some_and(Ordering::is_le),
        CompareOp::Gt => compare_order(actual, value, coercion).is_some_and(Ordering::is_gt),
        CompareOp::Gte => compare_order(actual, value, coercion).is_some_and(Ordering::is_ge),

        CompareOp::In => in_list(actual, value, coercion).unwrap_or(false),
        CompareOp::NotIn => in_list(actual, value, coercion).is_some_and(|matched| !matched),

        CompareOp::Contains => contains(actual, value, coercion),

        CompareOp::StartsWith => {
            compare_text(actual, value, coercion, TextOp::StartsWith).unwrap_or(false)
        }
        CompareOp::EndsWith => {
            compare_text(actual, value, coercion, TextOp::EndsWith).unwrap_or(false)
        }
    }
}

/// Determine whether a value is considered empty for `IsEmpty` checks.
const fn is_empty_value(value: &Value) -> bool {
    match value {
        Value::Text(text) => text.is_empty(),
        Value::List(items) => items.is_empty(),
        _ => false,
    }
}

/// Check whether a value equals any element in a list.
fn in_list(actual: &Value, list: &Value, coercion: &CoercionSpec) -> Option<bool> {
    let Value::List(items) = list else {
        return None;
    };

    let mut saw_valid = false;
    for item in items {
        match compare_eq(actual, item, coercion) {
            Some(true) => return Some(true),
            Some(false) => saw_valid = true,
            None => {}
        }
    }

    saw_valid.then_some(false)
}

/// Check whether a collection contains another value.
///
/// CONTRACT: text substring matching uses TextContains/TextContainsCi only.
fn contains(actual: &Value, needle: &Value, coercion: &CoercionSpec) -> bool {
    if matches!(actual, Value::Text(_)) {
        return false;
    }
    let Value::List(items) = actual else {
        return false;
    };

    items
        .iter()
        .any(|item| compare_eq(item, needle, coercion).unwrap_or(false))
}

use crate::{
    db::predicate::{
        CoercionSpec, CompareOp, ComparePredicate, Predicate, PredicateExecutionModel,
        ResolvedComparePredicate, ResolvedPredicate, TextOp, compare_eq, compare_order,
        compare_text,
    },
    model::entity::resolve_field_slot,
    traits::{EntityKind, EntityValue},
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
        let resolved = compile_predicate_program::<E>(predicate);

        Self { resolved }
    }

    /// Evaluate one precompiled predicate program against one entity.
    #[must_use]
    pub(in crate::db) fn eval<E: EntityValue>(&self, entity: &E) -> bool {
        eval_with_resolved_slots(entity, &self.resolved)
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

// Compile field-name predicates to stable field-slot predicates once per query.
fn compile_predicate_program<E: EntityKind>(
    predicate: &PredicateExecutionModel,
) -> ResolvedPredicate {
    fn resolve_field<E: EntityKind>(field_name: &str) -> Option<usize> {
        resolve_field_slot(E::MODEL, field_name)
    }

    match predicate {
        Predicate::True => ResolvedPredicate::True,
        Predicate::False => ResolvedPredicate::False,
        Predicate::And(children) => ResolvedPredicate::And(
            children
                .iter()
                .map(compile_predicate_program::<E>)
                .collect::<Vec<_>>(),
        ),
        Predicate::Or(children) => ResolvedPredicate::Or(
            children
                .iter()
                .map(compile_predicate_program::<E>)
                .collect::<Vec<_>>(),
        ),
        Predicate::Not(inner) => {
            ResolvedPredicate::Not(Box::new(compile_predicate_program::<E>(inner)))
        }
        Predicate::Compare(ComparePredicate {
            field,
            op,
            value,
            coercion,
        }) => ResolvedPredicate::Compare(ResolvedComparePredicate {
            field_slot: resolve_field::<E>(field),
            op: *op,
            value: value.clone(),
            coercion: coercion.clone(),
        }),
        Predicate::IsNull { field } => ResolvedPredicate::IsNull {
            field_slot: resolve_field::<E>(field),
        },
        Predicate::IsMissing { field } => ResolvedPredicate::IsMissing {
            field_slot: resolve_field::<E>(field),
        },
        Predicate::IsEmpty { field } => ResolvedPredicate::IsEmpty {
            field_slot: resolve_field::<E>(field),
        },
        Predicate::IsNotEmpty { field } => ResolvedPredicate::IsNotEmpty {
            field_slot: resolve_field::<E>(field),
        },
        Predicate::TextContains { field, value } => ResolvedPredicate::TextContains {
            field_slot: resolve_field::<E>(field),
            value: value.clone(),
        },
        Predicate::TextContainsCi { field, value } => ResolvedPredicate::TextContainsCi {
            field_slot: resolve_field::<E>(field),
            value: value.clone(),
        },
    }
}

// Read one field from an entity by pre-resolved slot.
fn field_from_slot<E: EntityValue>(entity: &E, field_slot: Option<usize>) -> FieldPresence {
    let value = field_slot.and_then(|slot| entity.get_value_by_index(slot));

    match value {
        Some(value) => FieldPresence::Present(value),
        None => FieldPresence::Missing,
    }
}

// Evaluate one slot-based field predicate only when the field is present.
fn on_present_slot<E: EntityValue>(
    entity: &E,
    field_slot: Option<usize>,
    f: impl FnOnce(&Value) -> bool,
) -> bool {
    match field_from_slot(entity, field_slot) {
        FieldPresence::Present(value) => f(&value),
        FieldPresence::Missing => false,
    }
}

// Evaluate one slot-resolved predicate against one entity.
fn eval_with_resolved_slots<E: EntityValue>(entity: &E, predicate: &ResolvedPredicate) -> bool {
    match predicate {
        ResolvedPredicate::True => true,
        ResolvedPredicate::False => false,
        ResolvedPredicate::And(children) => children
            .iter()
            .all(|child| eval_with_resolved_slots(entity, child)),
        ResolvedPredicate::Or(children) => children
            .iter()
            .any(|child| eval_with_resolved_slots(entity, child)),
        ResolvedPredicate::Not(inner) => !eval_with_resolved_slots(entity, inner),
        ResolvedPredicate::Compare(cmp) => eval_compare_with_resolved_slots(entity, cmp),
        ResolvedPredicate::IsNull { field_slot } => {
            matches!(
                field_from_slot(entity, *field_slot),
                FieldPresence::Present(Value::Null)
            )
        }
        ResolvedPredicate::IsMissing { field_slot } => {
            matches!(field_from_slot(entity, *field_slot), FieldPresence::Missing)
        }
        ResolvedPredicate::IsEmpty { field_slot } => {
            on_present_slot(entity, *field_slot, is_empty_value)
        }
        ResolvedPredicate::IsNotEmpty { field_slot } => {
            on_present_slot(entity, *field_slot, |value| !is_empty_value(value))
        }
        ResolvedPredicate::TextContains { field_slot, value } => {
            on_present_slot(entity, *field_slot, |actual| {
                actual.text_contains(value, TextMode::Cs).unwrap_or(false)
            })
        }
        ResolvedPredicate::TextContainsCi { field_slot, value } => {
            on_present_slot(entity, *field_slot, |actual| {
                actual.text_contains(value, TextMode::Ci).unwrap_or(false)
            })
        }
    }
}

// Evaluate a slot-resolved comparison predicate against one entity.
fn eval_compare_with_resolved_slots<E: EntityValue>(
    entity: &E,
    cmp: &ResolvedComparePredicate,
) -> bool {
    let FieldPresence::Present(actual) = field_from_slot(entity, cmp.field_slot) else {
        return false;
    };

    eval_compare_values(&actual, cmp.op, &cmp.value, &cmp.coercion)
}

// Shared compare-op semantics for slot-path evaluation.
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

// Determine whether a value is considered empty for `IsEmpty` checks.
const fn is_empty_value(value: &Value) -> bool {
    match value {
        Value::Text(text) => text.is_empty(),
        Value::List(items) => items.is_empty(),
        _ => false,
    }
}

// Check whether a value equals any element in a list.
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

// Check whether a collection contains another value.
//
// CONTRACT: text substring matching uses TextContains/TextContainsCi only.
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

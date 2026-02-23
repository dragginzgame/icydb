use crate::{
    db::query::predicate::{
        CompareOp, ComparePredicate, Predicate,
        coercion::{CoercionSpec, TextOp, compare_eq, compare_order, compare_text},
    },
    model::entity::resolve_field_slot,
    traits::{EntityKind, EntityValue},
    value::{TextMode, Value},
};
use std::cmp::Ordering;

///
/// PredicateFieldSlots
///
/// Slot-resolved predicate program for runtime row filtering.
/// Field names are resolved once during setup; evaluation is slot-only.
///

#[derive(Clone, Debug)]
pub(crate) struct PredicateFieldSlots {
    resolved: ResolvedPredicate,
}

///
/// ResolvedComparePredicate
///
/// One comparison node with a pre-resolved field slot.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct ResolvedComparePredicate {
    field_slot: Option<usize>,
    op: CompareOp,
    value: Value,
    coercion: CoercionSpec,
}

///
/// ResolvedPredicate
///
/// Predicate AST compiled to field slots for execution hot paths.
///

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResolvedPredicate {
    True,
    False,
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Compare(ResolvedComparePredicate),
    IsNull {
        field_slot: Option<usize>,
    },
    IsMissing {
        field_slot: Option<usize>,
    },
    IsEmpty {
        field_slot: Option<usize>,
    },
    IsNotEmpty {
        field_slot: Option<usize>,
    },
    TextContains {
        field_slot: Option<usize>,
        value: Value,
    },
    TextContainsCi {
        field_slot: Option<usize>,
        value: Value,
    },
}

impl PredicateFieldSlots {
    /// Resolve a predicate into a slot-based executable form.
    #[must_use]
    pub(crate) fn resolve<E: EntityKind>(predicate: &Predicate) -> Self {
        Self {
            resolved: resolve_predicate_slots::<E>(predicate),
        }
    }
}

fn resolve_predicate_slots<E: EntityKind>(predicate: &Predicate) -> ResolvedPredicate {
    fn resolve_field<E: EntityKind>(field_name: &str) -> Option<usize> {
        resolve_field_slot(E::MODEL, field_name)
    }

    // Compile field-name predicates to stable field-slot predicates once per query.
    match predicate {
        Predicate::True => ResolvedPredicate::True,
        Predicate::False => ResolvedPredicate::False,
        Predicate::And(children) => ResolvedPredicate::And(
            children
                .iter()
                .map(resolve_predicate_slots::<E>)
                .collect::<Vec<_>>(),
        ),
        Predicate::Or(children) => ResolvedPredicate::Or(
            children
                .iter()
                .map(resolve_predicate_slots::<E>)
                .collect::<Vec<_>>(),
        ),
        Predicate::Not(inner) => {
            ResolvedPredicate::Not(Box::new(resolve_predicate_slots::<E>(inner)))
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

///
/// FieldPresence
///
/// Result of attempting to read a field from a row during predicate
/// evaluation. This distinguishes between a missing field and a
/// present field whose value may be `None`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FieldPresence {
    /// Field exists and has a value (including `Value::Null`).
    Present(Value),
    /// Field is not present on the row.
    Missing,
}

///
/// Row
///
/// Abstraction over a row-like value that can expose fields by name.
/// This decouples predicate evaluation from concrete entity types.
///

#[cfg(test)]
pub(crate) trait Row {
    fn field(&self, name: &str) -> FieldPresence;
}

///
/// Default `Row` implementation for runtime entity values.
///

#[cfg(test)]
impl<T: EntityKind + EntityValue> Row for T {
    fn field(&self, name: &str) -> FieldPresence {
        let value = resolve_field_slot(T::MODEL, name)
            .and_then(|field_index| self.get_value_by_index(field_index));

        match value {
            Some(value) => FieldPresence::Present(value),
            None => FieldPresence::Missing,
        }
    }
}

///
/// FieldLookup
///
/// Runtime field-read capability used by predicate evaluation.
///

#[cfg(test)]
trait FieldLookup {
    fn field(&self, name: &str) -> FieldPresence;
}

#[cfg(test)]
impl<R: Row + ?Sized> FieldLookup for R {
    fn field(&self, name: &str) -> FieldPresence {
        Row::field(self, name)
    }
}

// Evaluate a field predicate only when the field is present.
#[cfg(test)]
fn on_present<R: FieldLookup + ?Sized>(
    row: &R,
    field: &str,
    f: impl FnOnce(&Value) -> bool,
) -> bool {
    match row.field(field) {
        FieldPresence::Present(value) => f(&value),
        FieldPresence::Missing => false,
    }
}

///
/// Evaluate a predicate against a single row.
///
/// This function performs **pure runtime evaluation**:
/// - no schema access
/// - no planning or index logic
/// - no validation
///
/// Any unsupported comparison simply evaluates to `false`.
/// CONTRACT: internal-only; predicates must be validated before evaluation.
///
#[must_use]
#[cfg(test)]
pub(crate) fn eval<R: Row + ?Sized>(row: &R, predicate: &Predicate) -> bool {
    eval_lookup(row, predicate)
}

/// Evaluate one predicate against one entity using pre-resolved field slots.
#[must_use]
pub(crate) fn eval_with_slots<E: EntityValue>(entity: &E, slots: &PredicateFieldSlots) -> bool {
    eval_with_resolved_slots(entity, &slots.resolved)
}

#[must_use]
#[expect(clippy::match_like_matches_macro)]
#[cfg(test)]
fn eval_lookup<R: FieldLookup + ?Sized>(row: &R, predicate: &Predicate) -> bool {
    match predicate {
        Predicate::True => true,
        Predicate::False => false,

        Predicate::And(children) => children.iter().all(|child| eval_lookup(row, child)),
        Predicate::Or(children) => children.iter().any(|child| eval_lookup(row, child)),
        Predicate::Not(inner) => !eval_lookup(row, inner),

        Predicate::Compare(cmp) => eval_compare(row, cmp),

        Predicate::IsNull { field } => match row.field(field) {
            FieldPresence::Present(Value::Null) => true,
            _ => false,
        },

        Predicate::IsMissing { field } => matches!(row.field(field), FieldPresence::Missing),

        Predicate::IsEmpty { field } => on_present(row, field, is_empty_value),

        Predicate::IsNotEmpty { field } => on_present(row, field, |value| !is_empty_value(value)),
        Predicate::TextContains { field, value } => on_present(row, field, |actual| {
            // NOTE: Invalid text comparisons are treated as non-matches.
            actual.text_contains(value, TextMode::Cs).unwrap_or(false)
        }),
        Predicate::TextContainsCi { field, value } => on_present(row, field, |actual| {
            // NOTE: Invalid text comparisons are treated as non-matches.
            actual.text_contains(value, TextMode::Ci).unwrap_or(false)
        }),
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
#[must_use]
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

///
/// Evaluate a single comparison predicate against a row.
///
/// Returns `false` if:
/// - the field is missing
/// - the comparison is not defined under the given coercion
///
#[cfg(test)]
fn eval_compare<R: FieldLookup + ?Sized>(row: &R, cmp: &ComparePredicate) -> bool {
    let ComparePredicate {
        field,
        op,
        value,
        coercion,
    } = cmp;

    let FieldPresence::Present(actual) = row.field(field) else {
        return false;
    };

    eval_compare_values(&actual, *op, value, coercion)
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

// Shared compare-op semantics for test-path and runtime slot-path evaluation.
fn eval_compare_values(
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

///
/// Determine whether a value is considered empty for `IsEmpty` checks.
///
const fn is_empty_value(value: &Value) -> bool {
    match value {
        Value::Text(text) => text.is_empty(),
        Value::List(items) => items.is_empty(),
        _ => false,
    }
}

///
/// Check whether a value equals any element in a list.
///
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

///
/// Check whether a collection contains another value.
///
/// CONTRACT: text substring matching uses TextContains/TextContainsCi only.
///
fn contains(actual: &Value, needle: &Value, coercion: &CoercionSpec) -> bool {
    if matches!(actual, Value::Text(_)) {
        // CONTRACT: text substring matching uses TextContains/TextContainsCi.
        return false;
    }

    let Value::List(items) = actual else {
        return false;
    };

    items
        .iter()
        // Invalid comparisons are treated as non-matches.
        .any(|item| compare_eq(item, needle, coercion).unwrap_or(false))
}

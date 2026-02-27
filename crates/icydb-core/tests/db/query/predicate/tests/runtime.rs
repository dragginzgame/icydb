use crate::{
    db::{
        executor::eval_compare_values as eval_runtime_compare_values,
        query::predicate::{ComparePredicate, Predicate},
    },
    traits::EntityValue,
    value::{TextMode, Value},
};

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
impl<T: crate::traits::EntityKind + EntityValue> Row for T {
    fn field(&self, name: &str) -> FieldPresence {
        let value = crate::model::entity::resolve_field_slot(T::MODEL, name)
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

    eval_runtime_compare_values(&actual, *op, value, coercion)
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

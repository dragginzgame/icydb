use crate::{traits::FieldValues, value::Value};

use super::{
    ast::{CompareOp, ComparePredicate, Predicate},
    coercion::{CoercionSpec, TextOp, compare_eq, compare_order, compare_text},
};
use std::cmp::Ordering;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FieldPresence {
    Present(Value),
    Missing,
}

pub trait Row {
    fn field(&self, name: &str) -> FieldPresence;
}

impl<T: FieldValues> Row for T {
    fn field(&self, name: &str) -> FieldPresence {
        match self.get_value(name) {
            Some(value) => FieldPresence::Present(value),
            None => FieldPresence::Missing,
        }
    }
}

#[must_use]
#[expect(clippy::match_like_matches_macro)]
pub fn eval<R: Row + ?Sized>(row: &R, predicate: &Predicate) -> bool {
    match predicate {
        Predicate::True => true,
        Predicate::False => false,
        Predicate::And(children) => children.iter().all(|child| eval(row, child)),
        Predicate::Or(children) => children.iter().any(|child| eval(row, child)),
        Predicate::Not(inner) => !eval(row, inner),
        Predicate::Compare(cmp) => eval_compare(row, cmp),
        Predicate::IsNull { field } => match row.field(field) {
            FieldPresence::Present(Value::None) => true,
            _ => false,
        },
        Predicate::IsMissing { field } => matches!(row.field(field), FieldPresence::Missing),
        Predicate::IsEmpty { field } => match row.field(field) {
            FieldPresence::Present(value) => is_empty_value(&value),
            FieldPresence::Missing => false,
        },
        Predicate::IsNotEmpty { field } => match row.field(field) {
            FieldPresence::Present(value) => !is_empty_value(&value),
            FieldPresence::Missing => false,
        },
        Predicate::MapContainsKey {
            field,
            key,
            coercion,
        } => match row.field(field) {
            FieldPresence::Present(value) => map_contains_key(&value, key, coercion),
            FieldPresence::Missing => false,
        },
        Predicate::MapContainsValue {
            field,
            value,
            coercion,
        } => match row.field(field) {
            FieldPresence::Present(actual) => map_contains_value(&actual, value, coercion),
            FieldPresence::Missing => false,
        },
        Predicate::MapContainsEntry {
            field,
            key,
            value,
            coercion,
        } => match row.field(field) {
            FieldPresence::Present(actual) => map_contains_entry(&actual, key, value, coercion),
            FieldPresence::Missing => false,
        },
    }
}

fn eval_compare<R: Row + ?Sized>(row: &R, cmp: &ComparePredicate) -> bool {
    let ComparePredicate {
        field,
        op,
        value,
        coercion,
    } = cmp;

    let FieldPresence::Present(actual) = row.field(field) else {
        return false;
    };

    match op {
        CompareOp::Eq => compare_eq(&actual, value, coercion).unwrap_or(false),
        CompareOp::Ne => compare_eq(&actual, value, coercion).is_some_and(|v| !v),
        CompareOp::Lt => compare_order(&actual, value, coercion).is_some_and(Ordering::is_lt),
        CompareOp::Lte => compare_order(&actual, value, coercion).is_some_and(Ordering::is_le),
        CompareOp::Gt => compare_order(&actual, value, coercion).is_some_and(Ordering::is_gt),
        CompareOp::Gte => compare_order(&actual, value, coercion).is_some_and(Ordering::is_ge),
        CompareOp::In => in_list(&actual, value, coercion),
        CompareOp::NotIn => !in_list(&actual, value, coercion),
        CompareOp::AnyIn => any_in(&actual, value, coercion),
        CompareOp::AllIn => all_in(&actual, value, coercion),
        CompareOp::Contains => contains(&actual, value, coercion),
        CompareOp::StartsWith => {
            compare_text(&actual, value, coercion, TextOp::StartsWith).unwrap_or(false)
        }
        CompareOp::EndsWith => {
            compare_text(&actual, value, coercion, TextOp::EndsWith).unwrap_or(false)
        }
    }
}

const fn is_empty_value(value: &Value) -> bool {
    match value {
        Value::Text(text) => text.is_empty(),
        Value::List(items) => items.is_empty(),
        _ => false,
    }
}

fn in_list(actual: &Value, list: &Value, coercion: &CoercionSpec) -> bool {
    let Value::List(items) = list else {
        return false;
    };

    items
        .iter()
        .any(|item| compare_eq(actual, item, coercion).unwrap_or(false))
}

fn any_in(actual: &Value, list: &Value, coercion: &CoercionSpec) -> bool {
    let Value::List(actual_items) = actual else {
        return false;
    };
    let Value::List(needles) = list else {
        return false;
    };

    actual_items.iter().any(|item| {
        needles
            .iter()
            .any(|needle| compare_eq(item, needle, coercion).unwrap_or(false))
    })
}

fn all_in(actual: &Value, list: &Value, coercion: &CoercionSpec) -> bool {
    let Value::List(actual_items) = actual else {
        return false;
    };
    let Value::List(needles) = list else {
        return false;
    };

    actual_items.iter().all(|item| {
        needles
            .iter()
            .any(|needle| compare_eq(item, needle, coercion).unwrap_or(false))
    })
}

fn contains(actual: &Value, needle: &Value, coercion: &CoercionSpec) -> bool {
    if let Some(res) = compare_text(actual, needle, coercion, TextOp::Contains) {
        return res;
    }

    let Value::List(items) = actual else {
        return false;
    };

    items
        .iter()
        .any(|item| compare_eq(item, needle, coercion).unwrap_or(false))
}

fn map_contains_key(map: &Value, key: &Value, coercion: &CoercionSpec) -> bool {
    let Value::List(entries) = map else {
        return false;
    };

    for entry in entries {
        let Value::List(pair) = entry else {
            return false;
        };
        if pair.len() != 2 {
            return false;
        }
        if compare_eq(&pair[0], key, coercion).unwrap_or(false) {
            return true;
        }
    }

    false
}

fn map_contains_value(map: &Value, value: &Value, coercion: &CoercionSpec) -> bool {
    let Value::List(entries) = map else {
        return false;
    };

    for entry in entries {
        let Value::List(pair) = entry else {
            return false;
        };
        if pair.len() != 2 {
            return false;
        }
        if compare_eq(&pair[1], value, coercion).unwrap_or(false) {
            return true;
        }
    }

    false
}

fn map_contains_entry(map: &Value, key: &Value, value: &Value, coercion: &CoercionSpec) -> bool {
    let Value::List(entries) = map else {
        return false;
    };

    for entry in entries {
        let Value::List(pair) = entry else {
            return false;
        };
        if pair.len() != 2 {
            return false;
        }

        let key_match = compare_eq(&pair[0], key, coercion).unwrap_or(false);
        let value_match = compare_eq(&pair[1], value, coercion).unwrap_or(false);

        if key_match && value_match {
            return true;
        }
    }

    false
}

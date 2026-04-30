//! Module: value::ops::collection
//!
//! Responsibility: collection membership and emptiness operations for `Value`.
//! Does not own: text casefolding internals or map canonicalization.
//! Boundary: representation-local list/scalar membership helpers.

use crate::value::{Value, ops::text};

fn normalize_list_ref(value: &Value) -> Vec<&Value> {
    match value {
        Value::List(values) => values.iter().collect(),
        value => vec![value],
    }
}

fn contains_by<F>(value: &Value, needle: &Value, eq: F) -> Option<bool>
where
    F: Fn(&Value, &Value) -> bool,
{
    value
        .as_list()
        .map(|items| items.iter().any(|item| eq(item, needle)))
}

#[expect(clippy::unnecessary_wraps)]
fn contains_any_by<F>(value: &Value, needles: &Value, eq: F) -> Option<bool>
where
    F: Fn(&Value, &Value) -> bool,
{
    let needles = normalize_list_ref(needles);
    match value {
        Value::List(items) => Some(
            needles
                .iter()
                .any(|needle| items.iter().any(|item| eq(item, needle))),
        ),
        scalar => Some(needles.iter().any(|needle| eq(scalar, needle))),
    }
}

#[expect(clippy::unnecessary_wraps)]
fn contains_all_by<F>(value: &Value, needles: &Value, eq: F) -> Option<bool>
where
    F: Fn(&Value, &Value) -> bool,
{
    let needles = normalize_list_ref(needles);
    match value {
        Value::List(items) => Some(
            needles
                .iter()
                .all(|needle| items.iter().any(|item| eq(item, needle))),
        ),
        scalar => Some(needles.len() == 1 && eq(scalar, needles[0])),
    }
}

fn in_list_by<F>(value: &Value, haystack: &Value, eq: F) -> Option<bool>
where
    F: Fn(&Value, &Value) -> bool,
{
    if let Value::List(items) = haystack {
        Some(items.iter().any(|item| eq(item, value)))
    } else {
        None
    }
}

/// Return whether a value is empty when emptiness is defined for its variant.
#[must_use]
pub const fn is_empty(value: &Value) -> Option<bool> {
    match value {
        Value::List(values) => Some(values.is_empty()),
        Value::Map(entries) => Some(entries.is_empty()),
        Value::Text(text) => Some(text.is_empty()),
        Value::Blob(blob) => Some(blob.is_empty()),

        // Fields represented as Value::Null behave as empty values.
        Value::Null => Some(true),

        _ => None,
    }
}

/// Logical negation of [`is_empty`].
#[must_use]
pub fn is_not_empty(value: &Value) -> Option<bool> {
    is_empty(value).map(|empty| !empty)
}

/// Returns true if `value` contains `needle`.
#[must_use]
pub fn contains(value: &Value, needle: &Value) -> Option<bool> {
    contains_by(value, needle, |left, right| left == right)
}

/// Returns true if any item in `needles` matches a member of `value`.
#[must_use]
pub fn contains_any(value: &Value, needles: &Value) -> Option<bool> {
    contains_any_by(value, needles, |left, right| left == right)
}

/// Returns true if every item in `needles` matches a member of `value`.
#[must_use]
pub fn contains_all(value: &Value, needles: &Value) -> Option<bool> {
    contains_all_by(value, needles, |left, right| left == right)
}

/// Returns true if `value` exists inside the provided list.
#[must_use]
pub fn in_list(value: &Value, haystack: &Value) -> Option<bool> {
    in_list_by(value, haystack, |left, right| left == right)
}

/// Case-insensitive `contains` supporting text and identifier variants.
#[must_use]
pub fn contains_ci(value: &Value, needle: &Value) -> Option<bool> {
    match value {
        Value::List(_) => contains_by(value, needle, text::eq_ci),
        _ => Some(text::eq_ci(value, needle)),
    }
}

/// Case-insensitive variant of [`contains_any`].
#[must_use]
pub fn contains_any_ci(value: &Value, needles: &Value) -> Option<bool> {
    contains_any_by(value, needles, text::eq_ci)
}

/// Case-insensitive variant of [`contains_all`].
#[must_use]
pub fn contains_all_ci(value: &Value, needles: &Value) -> Option<bool> {
    contains_all_by(value, needles, text::eq_ci)
}

/// Case-insensitive variant of [`in_list`].
#[must_use]
pub fn in_list_ci(value: &Value, haystack: &Value) -> Option<bool> {
    in_list_by(value, haystack, text::eq_ci)
}

impl Value {
    #[must_use]
    pub const fn is_empty(&self) -> Option<bool> {
        is_empty(self)
    }

    /// Logical negation of [`is_empty`](Self::is_empty).
    #[must_use]
    pub fn is_not_empty(&self) -> Option<bool> {
        is_not_empty(self)
    }

    /// Returns true if `self` contains `needle` (or equals it for scalars).
    #[must_use]
    pub fn contains(&self, needle: &Self) -> Option<bool> {
        contains(self, needle)
    }

    /// Returns true if any item in `needles` matches a member of `self`.
    #[must_use]
    pub fn contains_any(&self, needles: &Self) -> Option<bool> {
        contains_any(self, needles)
    }

    /// Returns true if every item in `needles` matches a member of `self`.
    #[must_use]
    pub fn contains_all(&self, needles: &Self) -> Option<bool> {
        contains_all(self, needles)
    }

    /// Returns true if `self` exists inside the provided list.
    #[must_use]
    pub fn in_list(&self, haystack: &Self) -> Option<bool> {
        in_list(self, haystack)
    }

    /// Case-insensitive `contains` supporting text and identifier variants.
    #[must_use]
    pub fn contains_ci(&self, needle: &Self) -> Option<bool> {
        contains_ci(self, needle)
    }

    /// Case-insensitive variant of [`contains_any`](Self::contains_any).
    #[must_use]
    pub fn contains_any_ci(&self, needles: &Self) -> Option<bool> {
        contains_any_ci(self, needles)
    }

    /// Case-insensitive variant of [`contains_all`](Self::contains_all).
    #[must_use]
    pub fn contains_all_ci(&self, needles: &Self) -> Option<bool> {
        contains_all_ci(self, needles)
    }

    /// Case-insensitive variant of [`in_list`](Self::in_list).
    #[must_use]
    pub fn in_list_ci(&self, haystack: &Self) -> Option<bool> {
        in_list_ci(self, haystack)
    }
}

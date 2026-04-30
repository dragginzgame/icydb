//! Module: value::ops::text
//!
//! Responsibility: text and casefolded identifier operations for `Value`.
//! Does not own: collection membership or predicate-level coercion policy.
//! Boundary: representation-local text helpers used by query operators.

use crate::value::{TextMode, Value};
use std::borrow::Cow;

pub(crate) fn fold_ci(s: &str) -> Cow<'_, str> {
    if s.is_ascii() {
        return Cow::Owned(s.to_ascii_lowercase());
    }
    // NOTE: Unicode fallback - temporary to_lowercase for non-ASCII.
    // Future: replace with proper NFKC + full casefold when available.
    Cow::Owned(s.to_lowercase())
}

fn text_with_mode(s: &'_ str, mode: TextMode) -> Cow<'_, str> {
    match mode {
        TextMode::Cs => Cow::Borrowed(s),
        TextMode::Ci => fold_ci(s),
    }
}

fn text_op(
    left: &Value,
    right: &Value,
    mode: TextMode,
    f: impl Fn(&str, &str) -> bool,
) -> Option<bool> {
    let (a, b) = (left.as_text()?, right.as_text()?);
    let a = text_with_mode(a, mode);
    let b = text_with_mode(b, mode);
    Some(f(&a, &b))
}

pub(crate) fn ci_key(value: &Value) -> Option<String> {
    match value {
        Value::Text(s) => Some(fold_ci(s).into_owned()),
        Value::Ulid(u) => Some(u.to_string().to_ascii_lowercase()),
        Value::Principal(p) => Some(p.to_string().to_ascii_lowercase()),
        Value::Account(a) => Some(a.to_string().to_ascii_lowercase()),
        _ => None,
    }
}

pub(crate) fn eq_ci(left: &Value, right: &Value) -> bool {
    if let (Some(left_key), Some(right_key)) = (ci_key(left), ci_key(right)) {
        return left_key == right_key;
    }

    left == right
}

/// Case-sensitive/insensitive equality check for text-like values.
#[must_use]
pub fn text_eq(left: &Value, right: &Value, mode: TextMode) -> Option<bool> {
    text_op(left, right, mode, |a, b| a == b)
}

/// Check whether `needle` is a substring of `value` under the given text mode.
#[must_use]
pub fn text_contains(value: &Value, needle: &Value, mode: TextMode) -> Option<bool> {
    text_op(value, needle, mode, |a, b| a.contains(b))
}

/// Check whether `value` starts with `needle` under the given text mode.
#[must_use]
pub fn text_starts_with(value: &Value, needle: &Value, mode: TextMode) -> Option<bool> {
    text_op(value, needle, mode, |a, b| a.starts_with(b))
}

/// Check whether `value` ends with `needle` under the given text mode.
#[must_use]
pub fn text_ends_with(value: &Value, needle: &Value, mode: TextMode) -> Option<bool> {
    text_op(value, needle, mode, |a, b| a.ends_with(b))
}

impl Value {
    /// Case-sensitive/insensitive equality check for text-like values.
    #[must_use]
    pub fn text_eq(&self, other: &Self, mode: TextMode) -> Option<bool> {
        text_eq(self, other, mode)
    }

    /// Check whether `other` is a substring of `self` under the given text mode.
    #[must_use]
    pub fn text_contains(&self, needle: &Self, mode: TextMode) -> Option<bool> {
        text_contains(self, needle, mode)
    }

    /// Check whether `self` starts with `other` under the given text mode.
    #[must_use]
    pub fn text_starts_with(&self, needle: &Self, mode: TextMode) -> Option<bool> {
        text_starts_with(self, needle, mode)
    }

    /// Check whether `self` ends with `other` under the given text mode.
    #[must_use]
    pub fn text_ends_with(&self, needle: &Self, mode: TextMode) -> Option<bool> {
        text_ends_with(self, needle, mode)
    }
}

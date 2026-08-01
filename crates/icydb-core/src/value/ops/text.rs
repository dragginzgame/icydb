//! Module: value::ops::text
//!
//! Responsibility: text and casefolded identifier operations for `Value`.
//! Does not own: collection membership or predicate-level coercion policy.
//! Boundary: representation-local text helpers used by query operators.

use crate::value::{TextMode, Value};
use std::borrow::Cow;

/// Apply the canonical case-insensitive text fold.
///
/// Casefolding remains a distinct semantic contract from SQL `LOWER`, even
/// while both use Unicode lowercase conversion today. A future full Unicode
/// casefold must not silently change `LOWER` or persisted index expressions.
#[must_use]
pub(crate) fn casefold_text(input: &str) -> String {
    lowercase_text(input)
}

/// Apply the canonical `LOWER` transform used by query and index expressions.
#[must_use]
pub(crate) fn lower_text(input: &str) -> String {
    lowercase_text(input)
}

/// Apply the canonical `UPPER` transform used by query and index expressions.
#[must_use]
pub(crate) fn upper_text(input: &str) -> String {
    if input.is_ascii() {
        return input.to_ascii_uppercase();
    }

    input.to_uppercase()
}

fn lowercase_text(input: &str) -> String {
    if input.is_ascii() {
        return input.to_ascii_lowercase();
    }

    input.to_lowercase()
}

fn text_with_mode(s: &'_ str, mode: TextMode) -> Cow<'_, str> {
    match mode {
        TextMode::Cs => Cow::Borrowed(s),
        TextMode::Ci => Cow::Owned(casefold_text(s)),
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

fn ci_key(value: &Value) -> Option<String> {
    match value {
        Value::Text(s) => Some(casefold_text(s)),
        Value::Ulid(u) => Some(u.to_string().to_ascii_lowercase()),
        Value::Principal(p) => Some(p.to_string().to_ascii_lowercase()),
        Value::Account(a) => Some(a.to_string().to_ascii_lowercase()),
        _ => None,
    }
}

pub(super) fn eq_ci(left: &Value, right: &Value) -> bool {
    if let (Some(left_key), Some(right_key)) = (ci_key(left), ci_key(right)) {
        return left_key == right_key;
    }

    left == right
}

/// Case-sensitive/insensitive equality check for text-like values.
#[must_use]
fn text_eq(left: &Value, right: &Value, mode: TextMode) -> Option<bool> {
    text_op(left, right, mode, |a, b| a == b)
}

/// Check whether `needle` is a substring of `value` under the given text mode.
#[must_use]
fn text_contains(value: &Value, needle: &Value, mode: TextMode) -> Option<bool> {
    text_op(value, needle, mode, |a, b| a.contains(b))
}

/// Check whether `value` starts with `needle` under the given text mode.
#[must_use]
fn text_starts_with(value: &Value, needle: &Value, mode: TextMode) -> Option<bool> {
    text_op(value, needle, mode, |a, b| a.starts_with(b))
}

/// Check whether `value` ends with `needle` under the given text mode.
#[must_use]
fn text_ends_with(value: &Value, needle: &Value, mode: TextMode) -> Option<bool> {
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

#[cfg(test)]
mod tests {
    use super::{casefold_text, lower_text, upper_text};

    #[test]
    fn canonical_text_transforms_preserve_current_ascii_and_unicode_semantics() {
        assert_eq!(casefold_text("IcYDB"), "icydb");
        assert_eq!(lower_text("IcYDB"), "icydb");
        assert_eq!(upper_text("IcYDB"), "ICYDB");

        assert_eq!(casefold_text("Straße"), "straße");
        assert_eq!(lower_text("Straße"), "straße");
        assert_eq!(upper_text("Straße"), "STRASSE");
    }
}

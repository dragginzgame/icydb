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

/// Conservative construction allowance for `lower_text` on the pinned Rust
/// 1.98.1 implementation: cumulative requested backing and byte-work units.
/// This is not allocator telemetry or an IC instruction estimate.
#[must_use]
pub(crate) fn lower_text_construction_allowance(input_len: usize) -> (u64, u64) {
    let len = input_len as u64;
    if len <= 1 {
        // Empty/single-byte UTF-8 is necessarily ASCII and never grows:
        // inspect, copy, then lowercase the copied bytes in place.
        return (len, len.saturating_mul(3));
    }
    // Unicode lowercase output is at most twice the input byte length (the
    // exhaustive mapping test pins this). Rust starts at `len`, then at most
    // once grows to max(2 * len, 8), including its small byte-vector minimum.
    let backing = len.saturating_add(len.saturating_mul(2).max(8));
    // Input work: ASCII check (n), ASCII-prefix conversion (<=2n), scalar walk
    // (n), and final-sigma context scans (<=2n: Sigma is not case-ignorable).
    // Backing also covers output fills and the retained-prefix copy on growth.
    (backing, len.saturating_mul(6).saturating_add(backing))
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
    use super::{casefold_text, lower_text, lower_text_construction_allowance, upper_text};

    #[test]
    fn lowercase_allowance_covers_unicode_expansion_and_output_growth() {
        // Allocation-free qualification of every scalar mapping in the pinned
        // toolchain. Contextual sigma changes spelling, not UTF-8 width.
        for scalar in (0..=u32::from(char::MAX)).filter_map(char::from_u32) {
            let output_bytes: usize = scalar.to_lowercase().map(char::len_utf8).sum();
            assert!(output_bytes <= 2 * scalar.len_utf8(), "{scalar:?}");
        }
        for text in ["", "A", "İ", "Aİ", "İΣ", "ΟΣ\u{301}", "ΣΑ", "ASCII"] {
            for repeats in [1, 2, 16, 1024] {
                let input = text.repeat(repeats);
                let output = lower_text(&input);
                let (backing, _) = lower_text_construction_allowance(input.len());
                let requested = if output.capacity() > input.len() {
                    input.len() + output.capacity()
                } else {
                    input.len()
                };
                assert!(requested as u64 <= backing);
                assert_eq!(output, input.to_lowercase());
            }
        }
    }

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

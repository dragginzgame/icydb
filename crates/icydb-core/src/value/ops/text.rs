//! Module: value::ops::text
//!
//! Responsibility: text and casefolded identifier operations for `Value`.
//! Does not own: collection membership or predicate-level coercion policy.
//! Boundary: representation-local text helpers used by query operators.

use crate::value::{TextMode, Value};
use std::borrow::Cow;

// These standard-library tables determine persisted expression-index keys,
// predicate bounds and TRIM whitespace. Pin consumers as well as this checkout:
// a new Unicode release needs an explicit retained-index compatibility decision,
// not a compiler upgrade silently changing lookups or uniqueness enforcement.
const _: () = assert!(
    matches!(char::UNICODE_VERSION, (17, 0, 0)),
    "IcyDB persists Unicode 17.0.0 text transforms; changing Unicode data requires a retained-index compatibility review",
);

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
/// 1.99.1 implementation: cumulative requested backing and byte-work units.
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

/// Conservative cumulative backing and byte-work allowance for `upper_text`.
///
/// Rust's scalar mappings expand to at most three input byte lengths. Starting
/// at the input capacity therefore needs at most two geometric growths; include
/// both old and new requested backing rather than treating output length as
/// allocator telemetry. The mapping and capacity controls below pin this bound.
#[must_use]
pub(crate) fn upper_text_construction_allowance(input_len: usize) -> (u64, u64) {
    let len = input_len as u64;
    if len <= 1 {
        return (len, len.saturating_mul(3));
    }
    let first_growth = len.saturating_mul(2).max(8);
    let backing = len
        .saturating_add(first_growth)
        .saturating_add(first_growth.saturating_mul(2));
    (backing, len.saturating_mul(4).saturating_add(backing))
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
    use super::{
        casefold_text, lower_text, lower_text_construction_allowance, upper_text,
        upper_text_construction_allowance,
    };
    use sha2::{Digest, Sha256};

    #[test]
    fn unicode_scalar_mappings_and_properties_match_the_persisted_contract() {
        let mut hash = Sha256::new();
        hash.update(b"icydb.unicode-text-contract.v1");
        // Pin all scalar mappings and text properties, independently of the
        // compiler's version label. Contextual str casing has separate vectors.
        for scalar in (0..=u32::from(char::MAX)).filter_map(char::from_u32) {
            hash.update(u32::from(scalar).to_le_bytes());
            let lower = scalar.to_lowercase();
            hash.update([u8::try_from(lower.clone().count()).unwrap()]);
            for mapped in lower {
                hash.update(u32::from(mapped).to_le_bytes());
            }
            let upper = scalar.to_uppercase();
            hash.update([u8::try_from(upper.clone().count()).unwrap()]);
            for mapped in upper {
                hash.update(u32::from(mapped).to_le_bytes());
            }
            hash.update([
                u8::from(scalar.is_whitespace()),
                u8::from(scalar.is_lowercase()),
                u8::from(scalar.is_uppercase()),
                u8::from(scalar.is_alphabetic()),
            ]);
        }
        assert_eq!(
            format!("{:x}", hash.finalize()),
            "e0a71973d533bb6f1004c0e3a0a5e8402879e7dbf0491fd3182be9856e9082f8",
        );
    }

    #[test]
    fn unicode_text_transform_goldens_cover_new_pairs_and_contextual_sigma() {
        for (input, lower, upper) in [
            ("ɤƛ", "ɤƛ", "\u{a7cb}\u{a7dc}"),
            ("\u{a7cb}\u{a7dc}", "ɤƛ", "\u{a7cb}\u{a7dc}"),
            ("\u{1c89}", "\u{1c8a}", "\u{1c89}"),
            ("İßﬃ", "i\u{307}ßﬃ", "İSSFFI"),
            ("ΟΣ", "ος", "ΟΣ"),
            ("ΟΣ\u{301}", "ος\u{301}", "ΟΣ\u{301}"),
            ("ΟΣ\u{301}Α", "οσ\u{301}α", "ΟΣ\u{301}Α"),
            ("ΣΑ", "σα", "ΣΑ"),
            ("Σ", "σ", "Σ"),
        ] {
            assert_eq!(lower_text(input), lower);
            assert_eq!(casefold_text(input), lower);
            assert_eq!(upper_text(input), upper);
        }
    }

    #[test]
    fn uppercase_allowance_covers_unicode_expansion_and_cumulative_growth() {
        for scalar in (0..=u32::from(char::MAX)).filter_map(char::from_u32) {
            let output_bytes: usize = scalar.to_uppercase().map(char::len_utf8).sum();
            assert!(output_bytes <= 3 * scalar.len_utf8(), "{scalar:?}");
        }
        for text in ["", "a", "ß", "ΐ", "aΐ", "ﬃ", "é🍋", "ASCII"] {
            for repeats in [1, 2, 16, 1024] {
                let input = text.repeat(repeats);
                let output = upper_text(&input);
                let initial = input.len();
                let first_growth = (2 * initial).max(8);
                let requested = if output.capacity() > first_growth {
                    initial + first_growth + output.capacity()
                } else if output.capacity() > initial {
                    initial + output.capacity()
                } else {
                    initial
                };
                let (backing, work) = upper_text_construction_allowance(input.len());
                assert!(requested as u64 <= backing, "{input:?}");
                assert!(output.len() as u64 <= work);
                assert_eq!(output, input.to_uppercase());
            }
        }
        assert_eq!(upper_text_construction_allowance(0), (0, 0));
        assert!(upper_text_construction_allowance(usize::MAX).0 >= usize::MAX as u64);
    }

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

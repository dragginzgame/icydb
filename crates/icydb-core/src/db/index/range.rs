//! Module: index::range
//! Responsibility: semantic-to-raw range lowering for index key traversal.
//! Does not own: continuation token verification or index-store scanning.
//! Boundary: planner/cursor paths call this module to build raw bounds.

use crate::{
    db::index::{EncodedValue, IndexId, IndexKey, IndexKeyKind, RawIndexKey},
    model::index::IndexModel,
    value::Value,
};
use std::ops::Bound;

///
/// IndexBoundsSpec
///
/// Semantic index-bound request accepted by the canonical lowering path.
/// Callers choose the logical shape; this module owns the conversion into raw
/// index-key bounds without changing ordered-component encoding semantics.
///

pub(in crate::db) enum IndexBoundsSpec<'a> {
    /// Exact index-prefix lookup over zero or more leading components.
    Prefix { values: &'a [Value] },
    /// Component range lookup after zero or more exact prefix components.
    ComponentRange {
        prefix: &'a [Value],
        lower: &'a Bound<Value>,
        upper: &'a Bound<Value>,
    },
    /// Text starts-with lookup after zero or more exact prefix components.
    TextPrefixRange {
        prefix: &'a [Value],
        text_prefix: &'a str,
        mode: TextPrefixBoundMode,
    },
}

impl<'a> IndexBoundsSpec<'a> {
    /// Build a component-range spec, preserving canonical text-prefix shape
    /// when the semantic bounds match one of the starts-with envelopes.
    #[must_use]
    pub(in crate::db) fn component_range(
        prefix: &'a [Value],
        lower: &'a Bound<Value>,
        upper: &'a Bound<Value>,
    ) -> Self {
        if let Some((text_prefix, mode)) = text_prefix_mode_for_component_bounds(lower, upper) {
            return Self::TextPrefixRange {
                prefix,
                text_prefix,
                mode,
            };
        }

        Self::ComponentRange {
            prefix,
            lower,
            upper,
        }
    }
}

///
/// TextPrefixBoundMode
///
/// Planner-visible text-prefix envelope policy. Strict field-key lookups use
/// the canonical next-prefix upper bound; expression-key access can request a
/// lower-only envelope when its planner contract intentionally keeps the
/// residual predicate responsible for exact prefix filtering.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum TextPrefixBoundMode {
    /// Emit `[prefix, next_prefix)` when a strict lexical successor exists.
    Strict,
    /// Emit `[prefix, +inf)` while preserving the canonical lower bound.
    LowerOnly,
}

///
/// IndexRangeBoundEncodeError
///
/// Reason a logical `IndexRange` bound shape could not be translated into
/// canonical raw index-key bounds.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum IndexRangeBoundEncodeError {
    Prefix,
    Lower,
    Upper,
}

impl IndexRangeBoundEncodeError {
    #[must_use]
    pub(in crate::db) const fn validated_spec_not_indexable_reason(self) -> &'static str {
        match self {
            Self::Prefix => "validated index-range prefix is not indexable",
            Self::Lower => "validated index-range lower bound is not indexable",
            Self::Upper => "validated index-range upper bound is not indexable",
        }
    }

    #[must_use]
    pub(in crate::db) const fn cursor_anchor_not_indexable_reason(self) -> &'static str {
        match self {
            Self::Prefix => "index-range continuation anchor prefix is not indexable",
            Self::Lower => "index-range cursor lower continuation bound is not indexable",
            Self::Upper => "index-range cursor upper continuation bound is not indexable",
        }
    }
}

///
/// build_index_bounds
///
/// Canonical semantic-to-raw index-bound conversion path.
/// This is the only function that should lower semantic prefix/range/prefix-text
/// requests into executable raw index-key scan bounds.
///

pub(in crate::db) fn build_index_bounds(
    index_id: &IndexId,
    index: &IndexModel,
    spec: IndexBoundsSpec<'_>,
) -> Result<(Bound<RawIndexKey>, Bound<RawIndexKey>), IndexRangeBoundEncodeError> {
    build_index_bounds_for_arity(index_id, index.fields().len(), spec)
}

/// Build raw index-key bounds from reduced index key arity facts.
pub(in crate::db) fn build_index_bounds_for_arity(
    index_id: &IndexId,
    index_len: usize,
    spec: IndexBoundsSpec<'_>,
) -> Result<(Bound<RawIndexKey>, Bound<RawIndexKey>), IndexRangeBoundEncodeError> {
    match spec {
        IndexBoundsSpec::Prefix { values } => {
            let encoded_prefix = EncodedValue::try_encode_all(values)
                .map_err(|_| IndexRangeBoundEncodeError::Prefix)?;
            let (lower, upper) =
                raw_keys_for_encoded_prefix(index_id, index_len, encoded_prefix.as_slice());

            Ok((Bound::Included(lower), Bound::Included(upper)))
        }
        IndexBoundsSpec::ComponentRange {
            prefix,
            lower,
            upper,
        } => {
            raw_bounds_for_semantic_index_component_range(index_id, index_len, prefix, lower, upper)
        }
        IndexBoundsSpec::TextPrefixRange {
            prefix,
            text_prefix,
            mode,
        } => {
            let Some((lower, upper)) = starts_with_component_bounds(text_prefix, mode) else {
                return Err(IndexRangeBoundEncodeError::Lower);
            };

            raw_bounds_for_semantic_index_component_range(
                index_id, index_len, prefix, &lower, &upper,
            )
        }
    }
}

/// Build the semantic component interval for one starts-with predicate.
#[must_use]
pub(in crate::db) fn starts_with_component_bounds(
    prefix: &str,
    mode: TextPrefixBoundMode,
) -> Option<(Bound<Value>, Bound<Value>)> {
    text_prefix_component_bounds(prefix, mode)
}

// Build the text-specific starts-with interval. Keeping this helper private
// leaves callers on the semantic starts-with API while this module retains the
// exact Unicode successor ownership.
fn text_prefix_component_bounds(
    prefix: &str,
    mode: TextPrefixBoundMode,
) -> Option<(Bound<Value>, Bound<Value>)> {
    if prefix.is_empty() {
        return None;
    }

    let lower = Bound::Included(Value::Text(prefix.to_string()));
    let upper = match mode {
        TextPrefixBoundMode::Strict => next_text_prefix(prefix)
            .map_or(Bound::Unbounded, |next| Bound::Excluded(Value::Text(next))),
        TextPrefixBoundMode::LowerOnly => Bound::Unbounded,
    };

    Some((lower, upper))
}

fn text_prefix_mode_for_component_bounds<'a>(
    lower: &'a Bound<Value>,
    upper: &Bound<Value>,
) -> Option<(&'a str, TextPrefixBoundMode)> {
    let Bound::Included(Value::Text(prefix)) = lower else {
        return None;
    };

    if text_prefix_component_bounds(prefix, TextPrefixBoundMode::Strict)
        .is_some_and(|(_, strict_upper)| &strict_upper == upper)
    {
        return Some((prefix, TextPrefixBoundMode::Strict));
    }

    if matches!(upper, Bound::Unbounded) {
        return Some((prefix, TextPrefixBoundMode::LowerOnly));
    }

    None
}

///
/// raw_keys_for_encoded_prefix
///
/// Build canonical raw start/end keys for an encoded prefix in the user namespace.
///

#[must_use]
fn raw_keys_for_encoded_prefix(
    index_id: &IndexId,
    index_len: usize,
    prefix: &[EncodedValue],
) -> (RawIndexKey, RawIndexKey) {
    raw_keys_for_encoded_prefix_with_kind(index_id, IndexKeyKind::User, index_len, prefix)
}

///
/// raw_keys_for_encoded_prefix_with_kind
///
/// Build canonical raw start/end keys for an encoded prefix in the requested key namespace.
///

#[must_use]
pub(in crate::db) fn raw_keys_for_encoded_prefix_with_kind(
    index_id: &IndexId,
    key_kind: IndexKeyKind,
    index_len: usize,
    prefix: &[EncodedValue],
) -> (RawIndexKey, RawIndexKey) {
    raw_keys_for_component_prefix_with_kind(index_id, key_kind, index_len, prefix)
}

/// Build canonical raw start/end keys for any pre-encoded prefix bytes in the
/// requested key namespace.
#[must_use]
pub(in crate::db) fn raw_keys_for_component_prefix_with_kind<C: AsRef<[u8]>>(
    index_id: &IndexId,
    key_kind: IndexKeyKind,
    index_len: usize,
    prefix: &[C],
) -> (RawIndexKey, RawIndexKey) {
    let (start, end) = IndexKey::bounds_for_prefix_with_kind(index_id, key_kind, index_len, prefix);

    (start.to_raw(), end.to_raw())
}

///
/// raw_bounds_for_encoded_index_component_range
///
/// Build raw key-space bounds from pre-encoded index components.
///

fn raw_bounds_for_encoded_index_component_range(
    index_id: &IndexId,
    index_len: usize,
    prefix: &[EncodedValue],
    lower: &Bound<EncodedValue>,
    upper: &Bound<EncodedValue>,
) -> (Bound<RawIndexKey>, Bound<RawIndexKey>) {
    let lower_component = encoded_component_bound(lower);
    let upper_component = encoded_component_bound(upper);
    let (start, end) = IndexKey::bounds_for_prefix_component_range(
        index_id,
        index_len,
        prefix,
        &lower_component,
        &upper_component,
    );

    (raw_index_key_bound(start), raw_index_key_bound(end))
}

///
/// raw_bounds_for_semantic_index_component_range
///
/// Build raw key-space bounds from semantic index components.
/// This is the semantic-to-physical lowering boundary for index-range access.
///

fn raw_bounds_for_semantic_index_component_range(
    index_id: &IndexId,
    index_len: usize,
    prefix: &[Value],
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> Result<(Bound<RawIndexKey>, Bound<RawIndexKey>), IndexRangeBoundEncodeError> {
    // Phase 1: encode semantic values into canonical index-component bytes.
    let encoded_prefix =
        EncodedValue::try_encode_all(prefix).map_err(|_| IndexRangeBoundEncodeError::Prefix)?;
    let encoded_lower = encode_semantic_component_bound(lower, IndexRangeBoundEncodeError::Lower)?;
    let encoded_upper = encode_semantic_component_bound(upper, IndexRangeBoundEncodeError::Upper)?;

    // Phase 2: lower encoded bounds to canonical raw index-key bounds.
    Ok(raw_bounds_for_encoded_index_component_range(
        index_id,
        index_len,
        encoded_prefix.as_slice(),
        &encoded_lower,
        &encoded_upper,
    ))
}

/// Return the smallest strict lexical successor prefix, or `None` when the
/// input is already at the terminal Unicode scalar boundary.
fn next_text_prefix(prefix: &str) -> Option<String> {
    let mut chars = prefix.chars().collect::<Vec<_>>();
    for index in (0..chars.len()).rev() {
        let Some(next_char) = next_unicode_scalar(chars[index]) else {
            continue;
        };
        chars.truncate(index);
        chars.push(next_char);
        return Some(chars.into_iter().collect());
    }

    None
}

const fn encoded_component_bound(bound: &Bound<EncodedValue>) -> Bound<&[u8]> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(value) => Bound::Included(value.encoded()),
        Bound::Excluded(value) => Bound::Excluded(value.encoded()),
    }
}

fn encode_semantic_component_bound(
    bound: &Bound<Value>,
    kind: IndexRangeBoundEncodeError,
) -> Result<Bound<EncodedValue>, IndexRangeBoundEncodeError> {
    match bound {
        Bound::Unbounded => Ok(Bound::Unbounded),
        Bound::Included(value) => EncodedValue::try_from_ref(value)
            .map(Bound::Included)
            .map_err(|_| kind),
        Bound::Excluded(value) => EncodedValue::try_from_ref(value)
            .map(Bound::Excluded)
            .map_err(|_| kind),
    }
}

fn raw_index_key_bound(bound: Bound<IndexKey>) -> Bound<RawIndexKey> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(key) => Bound::Included(key.to_raw()),
        Bound::Excluded(key) => Bound::Excluded(key.to_raw()),
    }
}

fn next_unicode_scalar(value: char) -> Option<char> {
    if value == char::MAX {
        return None;
    }

    let mut next = u32::from(value).saturating_add(1);
    if (0xD800..=0xDFFF).contains(&next) {
        next = 0xE000;
    }

    char::from_u32(next)
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::IndexRangeBoundEncodeError;

    #[test]
    fn index_range_bound_encode_error_owns_validated_spec_reason_text() {
        assert_eq!(
            IndexRangeBoundEncodeError::Prefix.validated_spec_not_indexable_reason(),
            "validated index-range prefix is not indexable",
        );
        assert_eq!(
            IndexRangeBoundEncodeError::Lower.validated_spec_not_indexable_reason(),
            "validated index-range lower bound is not indexable",
        );
        assert_eq!(
            IndexRangeBoundEncodeError::Upper.validated_spec_not_indexable_reason(),
            "validated index-range upper bound is not indexable",
        );
    }

    #[test]
    fn index_range_bound_encode_error_owns_cursor_anchor_reason_text() {
        assert_eq!(
            IndexRangeBoundEncodeError::Prefix.cursor_anchor_not_indexable_reason(),
            "index-range continuation anchor prefix is not indexable",
        );
        assert_eq!(
            IndexRangeBoundEncodeError::Lower.cursor_anchor_not_indexable_reason(),
            "index-range cursor lower continuation bound is not indexable",
        );
        assert_eq!(
            IndexRangeBoundEncodeError::Upper.cursor_anchor_not_indexable_reason(),
            "index-range cursor upper continuation bound is not indexable",
        );
    }
}

//! Module: index::range
//! Responsibility: semantic-to-raw range lowering for index key traversal.
//! Does not own: continuation token verification or index-store scanning.
//! Boundary: planner/cursor paths call this module to build raw bounds.

use crate::db::index::{IndexId, IndexKey, IndexKeyKind, RawIndexStoreKey};
use crate::{db::index::EncodedValue, value::Value};
use std::ops::Bound;

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
    Lower,
    Upper,
    RawKey,
}

///
/// IndexBoundsLowering
///
/// Raw index range bounds plus the exact encoded equality-prefix components
/// produced while lowering those bounds.
///

pub(in crate::db) struct IndexBoundsLowering {
    lower: Bound<RawIndexStoreKey>,
    upper: Bound<RawIndexStoreKey>,
    encoded_prefix: Vec<EncodedValue>,
}

impl IndexBoundsLowering {
    const fn new(
        lower: Bound<RawIndexStoreKey>,
        upper: Bound<RawIndexStoreKey>,
        encoded_prefix: Vec<EncodedValue>,
    ) -> Self {
        Self {
            lower,
            upper,
            encoded_prefix,
        }
    }

    pub(in crate::db) fn into_bounds_and_prefix_components(
        self,
    ) -> (
        Bound<RawIndexStoreKey>,
        Bound<RawIndexStoreKey>,
        Vec<Vec<u8>>,
    ) {
        let prefix_components = self
            .encoded_prefix
            .into_iter()
            .map(EncodedValue::into_bytes)
            .collect();

        (self.lower, self.upper, prefix_components)
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

///
/// build_index_prefix_bounds_for_encoded_components
///
/// Build canonical exact-prefix raw key-space bounds from already-encoded
/// index components.
///
pub(in crate::db) fn build_index_prefix_bounds_for_encoded_components(
    index_id: &IndexId,
    key_kind: IndexKeyKind,
    index_len: usize,
    prefix: &[EncodedValue],
) -> Result<(Bound<RawIndexStoreKey>, Bound<RawIndexStoreKey>), IndexRangeBoundEncodeError> {
    let (lower, upper) =
        raw_keys_for_component_prefix_with_kind(index_id, key_kind, index_len, prefix)?;

    Ok((Bound::Included(lower), Bound::Included(upper)))
}

/// Build canonical raw start/end keys for any pre-encoded prefix bytes in the
/// requested key namespace.
pub(in crate::db) fn raw_keys_for_component_prefix_with_kind<C: AsRef<[u8]>>(
    index_id: &IndexId,
    key_kind: IndexKeyKind,
    index_len: usize,
    prefix: &[C],
) -> Result<(RawIndexStoreKey, RawIndexStoreKey), IndexRangeBoundEncodeError> {
    IndexKey::raw_bounds_for_prefix_with_kind(index_id, key_kind, index_len, prefix)
        .map_err(|_| IndexRangeBoundEncodeError::RawKey)
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
) -> Result<(Bound<RawIndexStoreKey>, Bound<RawIndexStoreKey>), IndexRangeBoundEncodeError> {
    let lower_component = encoded_component_bound(lower);
    let upper_component = encoded_component_bound(upper);
    IndexKey::raw_bounds_for_prefix_component_range_with_kind(
        index_id,
        IndexKeyKind::User,
        index_len,
        prefix,
        &lower_component,
        &upper_component,
    )
    .map_err(|_| IndexRangeBoundEncodeError::RawKey)
}

/// Lower one ordered range after its equality prefix has already been encoded
/// against accepted index contracts.
pub(in crate::db) fn build_index_component_range_with_encoded_prefix(
    index_id: &IndexId,
    index_len: usize,
    encoded_prefix: Vec<EncodedValue>,
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> Result<IndexBoundsLowering, IndexRangeBoundEncodeError> {
    let encoded_lower = encode_semantic_component_bound(lower, IndexRangeBoundEncodeError::Lower)?;
    let encoded_upper = encode_semantic_component_bound(upper, IndexRangeBoundEncodeError::Upper)?;
    let (lower, upper) = raw_bounds_for_encoded_index_component_range(
        index_id,
        index_len,
        encoded_prefix.as_slice(),
        &encoded_lower,
        &encoded_upper,
    )?;

    Ok(IndexBoundsLowering::new(lower, upper, encoded_prefix))
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

//! Module: index::range
//! Responsibility: semantic-to-raw range lowering for index key traversal.
//! Does not own: continuation token verification or index-store scanning.
//! Boundary: planner/cursor paths call this module to build raw bounds.

#[cfg(test)]
mod tests;

use crate::{
    MAX_INDEX_FIELDS,
    db::{
        index::{
            EncodedValue, IndexId, IndexKey, IndexKeyKind, RawIndexStoreKey,
            admit_query_index_component,
        },
        query::construction::ConstructionBudget,
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
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

#[derive(Debug)]
pub(in crate::db) enum IndexRangeBoundEncodeError {
    Lower,
    Upper,
    RawKey,
    Construction(InternalError),
}

impl IndexRangeBoundEncodeError {
    /// Preserve admission exhaustion at callers that already validated the shape.
    pub(in crate::db) fn into_internal_error(self) -> InternalError {
        match self {
            Self::Construction(error) => error,
            Self::Lower | Self::Upper | Self::RawKey => InternalError::query_executor_invariant(),
        }
    }
}

/// Admit both raw bounds before allocating or copying. The codec remains usable
/// by non-query maintenance; query callers must supply their current authority.
pub(in crate::db) fn admit_index_prefix_bounds<C: AsRef<[u8]>>(
    index_len: usize,
    prefix: &[C],
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    if index_len > MAX_INDEX_FIELDS || prefix.len() > index_len {
        return Err(InternalError::query_executor_invariant());
    }
    let capacity = IndexKey::raw_prefix_bounds_retained_capacity(index_len, prefix);
    admit_raw_bound_capacity(capacity, budget)
}

fn admit_raw_bound_capacity(
    capacity: usize,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    // One byte-step allowance covers framing, operand copies and wildcard fills.
    // Capacity discovery only reads lengths from at most MAX_INDEX_FIELDS slots;
    // allocator metadata and scalar-to-component conversion are not modeled here.
    budget.charge(Resource::TemporaryBytes, capacity as u64)?;
    budget.charge(Resource::PredicateExpressionSteps, capacity as u64)
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
/// Query callers with a construction budget admit with `admit_text_prefix_bounds`
/// first; scalar encoding separately admits the encoded destinations.
#[must_use]
pub(in crate::db) fn starts_with_component_bounds(
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

/// Admit semantic prefix output and scan work before constructing either bound.
pub(in crate::db) fn admit_text_prefix_bounds(
    prefix: &str,
    mode: TextPrefixBoundMode,
    budget: &dyn ConstructionBudget,
) -> Result<(), InternalError> {
    if prefix.is_empty() {
        return Ok(());
    }
    let len = prefix.len() as u64;
    // Incrementing one Unicode scalar grows its UTF-8 width by at most one
    // byte (skipping the surrogate gap stays three bytes). Truncating trailing
    // terminal scalars only shrinks the result. Charge this conservative bound
    // from metadata, before the reverse scan or either string allocation.
    let (backing, scan) = match mode {
        TextPrefixBoundMode::Strict => (len.saturating_mul(2).saturating_add(1), len),
        TextPrefixBoundMode::LowerOnly => (len, 0),
    };
    budget.charge(Resource::TemporaryBytes, backing)?;
    // Byte-work units cover the reverse scan and both output fills, not an
    // estimate of IC instructions or allocator overhead.
    budget.charge(
        Resource::PredicateExpressionSteps,
        backing.saturating_add(scan),
    )
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
    budget: &dyn ConstructionBudget,
) -> Result<(Bound<RawIndexStoreKey>, Bound<RawIndexStoreKey>), IndexRangeBoundEncodeError> {
    admit_index_prefix_bounds(index_len, prefix, budget)
        .map_err(IndexRangeBoundEncodeError::Construction)?;
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
    budget: &dyn ConstructionBudget,
) -> Result<(Bound<RawIndexStoreKey>, Bound<RawIndexStoreKey>), IndexRangeBoundEncodeError> {
    if index_len == 0 || index_len > MAX_INDEX_FIELDS || prefix.len() >= index_len {
        return Err(IndexRangeBoundEncodeError::RawKey);
    }
    let lower_component = encoded_component_bound(lower);
    let upper_component = encoded_component_bound(upper);
    let capacity = IndexKey::raw_component_range_bounds_capacity(
        index_len,
        prefix,
        &lower_component,
        &upper_component,
    )
    .map_err(|_| IndexRangeBoundEncodeError::RawKey)?;
    admit_raw_bound_capacity(capacity, budget).map_err(IndexRangeBoundEncodeError::Construction)?;
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
    budget: &dyn ConstructionBudget,
) -> Result<IndexBoundsLowering, IndexRangeBoundEncodeError> {
    let encoded_lower =
        encode_semantic_component_bound(lower, IndexRangeBoundEncodeError::Lower, budget)?;
    let encoded_upper =
        encode_semantic_component_bound(upper, IndexRangeBoundEncodeError::Upper, budget)?;
    let (lower, upper) = raw_bounds_for_encoded_index_component_range(
        index_id,
        index_len,
        encoded_prefix.as_slice(),
        &encoded_lower,
        &encoded_upper,
        budget,
    )?;

    Ok(IndexBoundsLowering::new(lower, upper, encoded_prefix))
}

/// Return the smallest strict lexical successor prefix, or `None` when the
/// input is already at the terminal Unicode scalar boundary.
pub(in crate::db) fn next_text_prefix(prefix: &str) -> Option<String> {
    // Skip terminal scalars in place. The byte offset is a UTF-8 boundary;
    // only the final successor needs backing, not a full character-vector copy.
    for (offset, character) in prefix.char_indices().rev() {
        let Some(next_char) = next_unicode_scalar(character) else {
            continue;
        };
        let mut successor = String::with_capacity(offset + next_char.len_utf8());
        successor.push_str(&prefix[..offset]);
        successor.push(next_char);

        return Some(successor);
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
    budget: &dyn ConstructionBudget,
) -> Result<Bound<EncodedValue>, IndexRangeBoundEncodeError> {
    if let Bound::Included(value) | Bound::Excluded(value) = bound {
        admit_query_index_component(value, budget)
            .map_err(IndexRangeBoundEncodeError::Construction)?;
    }
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

use crate::{
    db::index::{
        EncodedValue, IndexRangeBoundEncodeError, RawIndexKey,
        raw_bounds_for_encoded_index_component_range,
    },
    model::index::IndexModel,
    traits::EntityKind,
    value::Value,
};
use std::ops::Bound;

///
/// raw_bounds_for_semantic_index_component_range
///
/// Lower semantic index-range literals into canonical raw-key bounds at plan time.
/// This keeps execution/index traversal byte-only.
///

pub(in crate::db) fn raw_bounds_for_semantic_index_component_range<E: EntityKind>(
    index: &IndexModel,
    prefix: &[Value],
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> Result<(Bound<RawIndexKey>, Bound<RawIndexKey>), IndexRangeBoundEncodeError> {
    let encoded_prefix =
        EncodedValue::try_encode_all(prefix).map_err(|_| IndexRangeBoundEncodeError::Prefix)?;
    let encoded_lower = encode_index_component_bound(lower, IndexRangeBoundEncodeError::Lower)?;
    let encoded_upper = encode_index_component_bound(upper, IndexRangeBoundEncodeError::Upper)?;

    Ok(raw_bounds_for_encoded_index_component_range::<E>(
        index,
        encoded_prefix.as_slice(),
        &encoded_lower,
        &encoded_upper,
    ))
}

// Convert one semantic bound into its canonical encoded representation.
fn encode_index_component_bound(
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

use crate::{
    db::index::{IndexId, IndexKey, RawIndexKey, encode_canonical_index_component},
    model::index::IndexModel,
    traits::EntityKind,
    value::Value,
};
use std::ops::Bound;

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

///
/// raw_bounds_for_index_component_range
///
/// Build raw key-space bounds for one ranged component after an equality prefix.
/// This is the canonical path shared by execution and cursor-anchor validation.
///

pub(in crate::db) fn raw_bounds_for_index_component_range<E: EntityKind>(
    index: &IndexModel,
    prefix: &[Value],
    lower: &Bound<Value>,
    upper: &Bound<Value>,
) -> Result<(Bound<RawIndexKey>, Bound<RawIndexKey>), IndexRangeBoundEncodeError> {
    let index_id = IndexId::new::<E>(index);

    let mut prefix_components = Vec::with_capacity(prefix.len());
    for value in prefix {
        let component = encode_canonical_index_component(value)
            .map_err(|_| IndexRangeBoundEncodeError::Prefix)?;
        prefix_components.push(component);
    }

    let lower_component = encode_index_component_bound(lower, IndexRangeBoundEncodeError::Lower)?;
    let upper_component = encode_index_component_bound(upper, IndexRangeBoundEncodeError::Upper)?;
    let (start, end) = IndexKey::bounds_for_prefix_component_range(
        &index_id,
        index.fields.len(),
        &prefix_components,
        lower_component,
        upper_component,
    );

    Ok((raw_index_key_bound(start), raw_index_key_bound(end)))
}

fn encode_index_component_bound(
    bound: &Bound<Value>,
    kind: IndexRangeBoundEncodeError,
) -> Result<Bound<Vec<u8>>, IndexRangeBoundEncodeError> {
    match bound {
        Bound::Unbounded => Ok(Bound::Unbounded),
        Bound::Included(value) => encode_canonical_index_component(value)
            .map(Bound::Included)
            .map_err(|_| kind),
        Bound::Excluded(value) => encode_canonical_index_component(value)
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

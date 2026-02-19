use super::InlineIndexValue;
use crate::{
    db::{
        data::DataKey,
        index::{
            Direction, IndexId, IndexKey, continuation_advanced, encode_canonical_index_component,
            map_bound_encode_error, raw_bounds_for_index_component_range, resume_bounds,
            store::{IndexStore, RawIndexKey},
        },
    },
    error::{ErrorOrigin, InternalError},
    model::index::IndexModel,
    traits::EntityKind,
    value::Value,
};
use std::ops::Bound;

impl IndexStore {
    pub(crate) fn resolve_data_values<E: EntityKind>(
        &self,
        index: &IndexModel,
        prefix: &[Value],
    ) -> Result<Vec<DataKey>, InternalError> {
        let index_id = IndexId::new::<E>(index);

        let mut components = Vec::with_capacity(prefix.len());
        for value in prefix {
            let component = encode_canonical_index_component(value).map_err(|_| {
                InternalError::index_invariant(
                    "executor invariant violated: index prefix value is not indexable",
                )
            })?;
            components.push(component);
        }

        let (start, end) = IndexKey::bounds_for_prefix(&index_id, index.fields.len(), &components);
        let (start_raw, end_raw) = (start.to_raw(), end.to_raw());

        let mut out = Vec::new();

        for entry in self.entry_map().range(start_raw..=end_raw) {
            let raw_key = entry.key();
            let value = entry.value();
            let reached_limit = Self::decode_index_entry_and_push::<E>(
                index, raw_key, &value, &mut out, None, "resolve",
            )?;
            debug_assert!(
                !reached_limit,
                "unbounded prefix resolution must not hit a decode helper limit"
            );
        }

        Ok(out)
    }

    pub(crate) fn resolve_data_values_in_range_from_start_exclusive<E: EntityKind>(
        &self,
        index: &IndexModel,
        prefix: &[Value],
        lower: &Bound<Value>,
        upper: &Bound<Value>,
        continuation_start_exclusive: Option<&RawIndexKey>,
        direction: Direction,
    ) -> Result<Vec<DataKey>, InternalError> {
        self.resolve_data_values_in_range_limited::<E>(
            index,
            prefix,
            (lower, upper),
            continuation_start_exclusive,
            direction,
            usize::MAX,
        )
    }

    pub(crate) fn resolve_data_values_in_range_limited<E: EntityKind>(
        &self,
        index: &IndexModel,
        prefix: &[Value],
        bounds: (&Bound<Value>, &Bound<Value>),
        continuation_start_exclusive: Option<&RawIndexKey>,
        direction: Direction,
        limit: usize,
    ) -> Result<Vec<DataKey>, InternalError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let (lower, upper) = bounds;
        let (start_raw, end_raw) = raw_bounds_for_index_component_range::<E>(
            index, prefix, lower, upper,
        )
        .map_err(|err| {
            InternalError::index_invariant(map_bound_encode_error(
                err,
                "executor invariant violated: index range prefix value is not indexable",
                "executor invariant violated: index range lower bound value is not indexable",
                "executor invariant violated: index range upper bound value is not indexable",
            ))
        })?;
        let (start_raw, end_raw) = match continuation_start_exclusive {
            Some(anchor) => resume_bounds(direction, start_raw, end_raw, anchor),
            None => (start_raw, end_raw),
        };
        if range_is_empty(&start_raw, &end_raw) {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        match direction {
            Direction::Asc => {
                for entry in self.entry_map().range((start_raw, end_raw)) {
                    let raw_key = entry.key();
                    let value = entry.value();

                    if let Some(anchor) = continuation_start_exclusive
                        && !continuation_advanced(direction, raw_key, anchor)
                    {
                        return Err(InternalError::index_invariant(
                            "index-range continuation scan did not advance beyond the anchor",
                        ));
                    }
                    if Self::decode_index_entry_and_push::<E>(
                        index,
                        raw_key,
                        &value,
                        &mut out,
                        Some(limit),
                        "range resolve",
                    )? {
                        return Ok(out);
                    }
                }
            }
            Direction::Desc => {
                for entry in self.entry_map().range((start_raw, end_raw)).rev() {
                    let raw_key = entry.key();
                    let value = entry.value();

                    if let Some(anchor) = continuation_start_exclusive
                        && !continuation_advanced(direction, raw_key, anchor)
                    {
                        return Err(InternalError::index_invariant(
                            "index-range continuation scan did not advance beyond the anchor",
                        ));
                    }
                    if Self::decode_index_entry_and_push::<E>(
                        index,
                        raw_key,
                        &value,
                        &mut out,
                        Some(limit),
                        "range resolve",
                    )? {
                        return Ok(out);
                    }
                }
            }
        }

        Ok(out)
    }

    fn decode_index_entry_and_push<E: EntityKind>(
        index: &IndexModel,
        raw_key: &RawIndexKey,
        value: &InlineIndexValue,
        out: &mut Vec<DataKey>,
        limit: Option<usize>,
        context: &'static str,
    ) -> Result<bool, InternalError> {
        #[cfg(debug_assertions)]
        if let Err(err) = Self::verify_entry_fingerprint(Some(index), raw_key, value) {
            panic!(
                "invariant violation (debug-only): index fingerprint verification failed: {err}"
            );
        }

        IndexKey::try_from_raw(raw_key).map_err(|err| {
            InternalError::corruption(
                ErrorOrigin::Index,
                format!("index key corrupted during {context}: {err}"),
            )
        })?;

        let storage_keys = value
            .entry
            .decode_keys()
            .map_err(|err| InternalError::corruption(ErrorOrigin::Index, err.to_string()))?;

        if index.unique && storage_keys.len() != 1 {
            return Err(InternalError::corruption(
                ErrorOrigin::Index,
                "unique index entry contains an unexpected number of keys",
            ));
        }

        for storage_key in storage_keys {
            out.push(DataKey::from_key::<E>(storage_key));
            if let Some(limit) = limit
                && out.len() == limit
            {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

fn range_is_empty(lower: &Bound<RawIndexKey>, upper: &Bound<RawIndexKey>) -> bool {
    let (Some(lower_key), Some(upper_key)) = (bound_key(lower), bound_key(upper)) else {
        return false;
    };

    if lower_key < upper_key {
        return false;
    }
    if lower_key > upper_key {
        return true;
    }

    !matches!(lower, Bound::Included(_)) || !matches!(upper, Bound::Included(_))
}

const fn bound_key(bound: &Bound<RawIndexKey>) -> Option<&RawIndexKey> {
    match bound {
        Bound::Included(value) | Bound::Excluded(value) => Some(value),
        Bound::Unbounded => None,
    }
}

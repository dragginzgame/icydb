use super::InlineIndexValue;
use crate::{
    db::{
        data::DataKey,
        index::{
            Direction, IndexId, IndexKey, continuation_advanced, encode_canonical_index_component,
            envelope_is_empty, map_bound_encode_error, raw_bounds_for_index_component_range,
            resume_bounds,
            store::{IndexStore, RawIndexKey},
        },
    },
    error::InternalError,
    model::index::IndexModel,
    traits::EntityKind,
    value::Value,
};
use std::ops::Bound;

const PREFIX_NOT_INDEXABLE: &str = "index prefix value is not indexable";
const RANGE_PREFIX_NOT_INDEXABLE: &str = "index range prefix value is not indexable";
const RANGE_LOWER_NOT_INDEXABLE: &str = "index range lower bound value is not indexable";
const RANGE_UPPER_NOT_INDEXABLE: &str = "index range upper bound value is not indexable";

// Keep index-store invariant messages on the canonical executor-invariant prefix.
fn index_executor_invariant(reason: &'static str) -> InternalError {
    InternalError::index_invariant(InternalError::executor_invariant_message(reason))
}

impl IndexStore {
    pub(crate) fn resolve_data_values<E: EntityKind>(
        &self,
        index: &IndexModel,
        prefix: &[Value],
    ) -> Result<Vec<DataKey>, InternalError> {
        self.resolve_data_values_limited::<E>(index, prefix, Direction::Asc, usize::MAX)
    }

    pub(crate) fn resolve_data_values_limited<E: EntityKind>(
        &self,
        index: &IndexModel,
        prefix: &[Value],
        direction: Direction,
        limit: usize,
    ) -> Result<Vec<DataKey>, InternalError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let index_id = IndexId::new::<E>(index);

        // Phase 1: encode prefix bounds once and derive the canonical index envelope.
        let mut components = Vec::with_capacity(prefix.len());
        for value in prefix {
            let component = encode_canonical_index_component(value)
                .map_err(|_| index_executor_invariant(PREFIX_NOT_INDEXABLE))?;
            components.push(component);
        }

        let (start, end) = IndexKey::bounds_for_prefix(&index_id, index.fields.len(), &components);
        let (start_raw, end_raw) = (start.to_raw(), end.to_raw());

        // Phase 2: decode in traversal direction and stop once we collected `limit` keys.
        let mut out = Vec::new();
        match direction {
            Direction::Asc => {
                for entry in self.entry_map().range(start_raw..=end_raw) {
                    let raw_key = entry.key();
                    let value = entry.value();
                    if Self::decode_index_entry_and_push::<E>(
                        index,
                        raw_key,
                        &value,
                        &mut out,
                        Some(limit),
                        "resolve",
                    )? {
                        return Ok(out);
                    }
                }
            }
            Direction::Desc => {
                for entry in self.entry_map().range(start_raw..=end_raw).rev() {
                    let raw_key = entry.key();
                    let value = entry.value();
                    if Self::decode_index_entry_and_push::<E>(
                        index,
                        raw_key,
                        &value,
                        &mut out,
                        Some(limit),
                        "resolve",
                    )? {
                        return Ok(out);
                    }
                }
            }
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
            index_executor_invariant(map_bound_encode_error(
                err,
                RANGE_PREFIX_NOT_INDEXABLE,
                RANGE_LOWER_NOT_INDEXABLE,
                RANGE_UPPER_NOT_INDEXABLE,
            ))
        })?;
        let (start_raw, end_raw) = match continuation_start_exclusive {
            Some(anchor) => resume_bounds(direction, start_raw, end_raw, anchor),
            None => (start_raw, end_raw),
        };
        if envelope_is_empty(&start_raw, &end_raw) {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        match direction {
            Direction::Asc => {
                for entry in self.entry_map().range((start_raw, end_raw)) {
                    let raw_key = entry.key();
                    let value = entry.value();

                    Self::ensure_continuation_advanced(
                        direction,
                        raw_key,
                        continuation_start_exclusive,
                    )?;
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

                    Self::ensure_continuation_advanced(
                        direction,
                        raw_key,
                        continuation_start_exclusive,
                    )?;
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

    // Validate strict continuation advancement when an anchor is present.
    fn ensure_continuation_advanced(
        direction: Direction,
        candidate: &RawIndexKey,
        anchor: Option<&RawIndexKey>,
    ) -> Result<(), InternalError> {
        if let Some(anchor) = anchor
            && !continuation_advanced(direction, candidate, anchor)
        {
            return Err(InternalError::index_invariant(
                "index-range continuation scan did not advance beyond the anchor",
            ));
        }

        Ok(())
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
            InternalError::index_corruption(format!("index key corrupted during {context}: {err}"))
        })?;

        let storage_keys = value
            .entry
            .decode_keys()
            .map_err(|err| InternalError::index_corruption(err.to_string()))?;

        if index.unique && storage_keys.len() != 1 {
            return Err(InternalError::index_corruption(
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{PREFIX_NOT_INDEXABLE, index_executor_invariant};
    use crate::error::{ErrorClass, ErrorOrigin};

    #[test]
    fn index_executor_invariant_uses_canonical_prefix_and_origin() {
        let err = index_executor_invariant(PREFIX_NOT_INDEXABLE);
        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Index);
        assert_eq!(
            err.message,
            "executor invariant violated: index prefix value is not indexable"
        );
    }
}

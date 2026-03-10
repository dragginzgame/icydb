//! Module: index::scan
//! Responsibility: raw-range index scan resolution and continuation guards.
//! Does not own: index persistence layout or predicate compilation.
//! Boundary: executor/query range reads go through this layer above `index::store`.

use crate::{
    db::{
        cursor::{
            IndexScanContinuationInput, resume_bounds_from_refs,
            validate_index_scan_continuation_advancement,
            validate_index_scan_continuation_envelope,
        },
        data::DataKey,
        direction::Direction,
        index::{
            IndexKey, envelope_is_empty,
            key::RawIndexKey,
            predicate::{IndexPredicateExecution, eval_index_execution_on_decoded_key},
            store::{IndexStore, StoredIndexValue},
        },
    },
    error::InternalError,
    model::index::IndexModel,
    traits::EntityKind,
};
use std::ops::Bound;

impl IndexStore {
    pub(in crate::db) fn resolve_data_values_in_raw_range_limited<E: EntityKind>(
        &self,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        continuation: IndexScanContinuationInput<'_>,
        limit: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<DataKey>, InternalError> {
        self.resolve_raw_range_limited(bounds, continuation, limit, |raw_key, value, out| {
            Self::decode_index_entry_and_push::<E>(
                index,
                raw_key,
                value,
                out,
                Some(limit),
                "range resolve",
                index_predicate_execution,
            )
        })
    }

    pub(in crate::db) fn resolve_data_values_with_component_in_raw_range_limited<E: EntityKind>(
        &self,
        index: &IndexModel,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        continuation: IndexScanContinuationInput<'_>,
        limit: usize,
        component_index: usize,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<(DataKey, Vec<u8>)>, InternalError> {
        self.resolve_raw_range_limited(bounds, continuation, limit, |raw_key, value, out| {
            Self::decode_index_entry_and_push_with_component::<E>(
                index,
                raw_key,
                value,
                out,
                Some(limit),
                component_index,
                "range resolve",
                index_predicate_execution,
            )
        })
    }

    // Resolve one bounded directional raw-range scan with shared continuation guards.
    fn resolve_raw_range_limited<T, F>(
        &self,
        bounds: (&Bound<RawIndexKey>, &Bound<RawIndexKey>),
        continuation: IndexScanContinuationInput<'_>,
        limit: usize,
        mut decode_and_push: F,
    ) -> Result<Vec<T>, InternalError>
    where
        F: FnMut(&RawIndexKey, &StoredIndexValue, &mut Vec<T>) -> Result<bool, InternalError>,
    {
        // Phase 1: validate envelope/anchor preconditions and derive scan bounds.
        if limit == 0 {
            return Ok(Vec::new());
        }

        validate_index_scan_continuation_envelope(continuation.anchor(), bounds.0, bounds.1)?;

        let (start_raw, end_raw) = match continuation.anchor() {
            Some(anchor) => {
                resume_bounds_from_refs(continuation.direction(), bounds.0, bounds.1, anchor)
            }
            None => (bounds.0.clone(), bounds.1.clone()),
        };

        if envelope_is_empty(&start_raw, &end_raw) {
            return Ok(Vec::new());
        }

        // Phase 2: scan in directional order and decode entries until limit.
        let mut out = Vec::new();
        let direction = continuation.direction();
        let anchor = continuation.anchor();

        match direction {
            Direction::Asc => {
                for entry in self.map.range((start_raw, end_raw)) {
                    let raw_key = entry.key();
                    let value = entry.value();

                    if Self::scan_range_entry(
                        direction,
                        anchor,
                        raw_key,
                        &value,
                        &mut out,
                        &mut decode_and_push,
                    )? {
                        return Ok(out);
                    }
                }
            }
            Direction::Desc => {
                for entry in self.map.range((start_raw, end_raw)).rev() {
                    let raw_key = entry.key();
                    let value = entry.value();

                    if Self::scan_range_entry(
                        direction,
                        anchor,
                        raw_key,
                        &value,
                        &mut out,
                        &mut decode_and_push,
                    )? {
                        return Ok(out);
                    }
                }
            }
        }

        Ok(out)
    }

    // Apply continuation advancement guard and one decode/push attempt for an entry.
    fn scan_range_entry<T, F>(
        direction: Direction,
        anchor: Option<&RawIndexKey>,
        raw_key: &RawIndexKey,
        value: &StoredIndexValue,
        out: &mut Vec<T>,
        decode_and_push: &mut F,
    ) -> Result<bool, InternalError>
    where
        F: FnMut(&RawIndexKey, &StoredIndexValue, &mut Vec<T>) -> Result<bool, InternalError>,
    {
        validate_index_scan_continuation_advancement(direction, anchor, raw_key)?;
        decode_and_push(raw_key, value, out)
    }

    fn decode_index_entry_and_push<E: EntityKind>(
        index: &IndexModel,
        raw_key: &RawIndexKey,
        value: &StoredIndexValue,
        out: &mut Vec<DataKey>,
        limit: Option<usize>,
        context: &'static str,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<bool, InternalError> {
        #[cfg(debug_assertions)]
        Self::verify_if_debug(raw_key, value);

        // Phase 1: decode key + evaluate optional index-only predicate.
        let decoded_key = IndexKey::try_from_raw(raw_key).map_err(|err| {
            InternalError::index_corruption(format!("index key corrupted during {context}: {err}"))
        })?;

        if let Some(execution) = index_predicate_execution
            && !eval_index_execution_on_decoded_key(&decoded_key, execution)?
        {
            return Ok(false);
        }

        // Phase 2: decode entry payload and push bounded data keys.
        let storage_keys = value
            .entry
            .decode_keys()
            .map_err(|err| InternalError::index_corruption(err.to_string()))?;

        if index.is_unique() && storage_keys.len() != 1 {
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

    #[expect(clippy::too_many_arguments)]
    fn decode_index_entry_and_push_with_component<E: EntityKind>(
        index: &IndexModel,
        raw_key: &RawIndexKey,
        value: &StoredIndexValue,
        out: &mut Vec<(DataKey, Vec<u8>)>,
        limit: Option<usize>,
        component_index: usize,
        context: &'static str,
        index_predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<bool, InternalError> {
        #[cfg(debug_assertions)]
        Self::verify_if_debug(raw_key, value);

        // Phase 1: decode key, extract requested component, and evaluate optional
        // index-only predicate.
        let decoded_key = IndexKey::try_from_raw(raw_key).map_err(|err| {
            InternalError::index_corruption(format!("index key corrupted during {context}: {err}"))
        })?;
        let Some(component) = decoded_key.component(component_index) else {
            return Err(InternalError::index_invariant(format!(
                "index projection referenced missing component: index='{}' component_index={component_index}",
                index.name()
            )));
        };
        let component = component.to_vec();

        if let Some(execution) = index_predicate_execution
            && !eval_index_execution_on_decoded_key(&decoded_key, execution)?
        {
            return Ok(false);
        }

        // Phase 2: decode entry payload and push bounded `(data_key, component)`.
        let storage_keys = value
            .entry
            .decode_keys()
            .map_err(|err| InternalError::index_corruption(err.to_string()))?;

        if index.is_unique() && storage_keys.len() != 1 {
            return Err(InternalError::index_corruption(
                "unique index entry contains an unexpected number of keys",
            ));
        }

        for storage_key in storage_keys {
            out.push((DataKey::from_key::<E>(storage_key), component.clone()));

            if let Some(limit) = limit
                && out.len() == limit
            {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

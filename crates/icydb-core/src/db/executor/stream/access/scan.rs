//! Module: executor::stream::access::scan
//! Responsibility: low-level primary/index scan adapters over store/index handles.
//! Does not own: access routing decisions or planner spec construction.
//! Boundary: direct scan primitives used by access-stream resolver.

use crate::{
    db::{
        cursor::IndexScanContinuationInput,
        data::{DataKey, RawDataKey},
        direction::Direction,
        executor::LoweredKey,
        executor::{LoweredIndexPrefixSpec, LoweredIndexRangeSpec},
        index::predicate::IndexPredicateExecution,
        registry::StoreHandle,
    },
    error::InternalError,
    model::index::IndexModel,
    types::EntityTag,
};
use std::ops::Bound;

///
/// PrimaryScan
///
/// Executor-owned adapter for primary data-store iteration.
/// The physical stream resolver must request scans through this boundary instead of
/// traversing store handles directly.
///

pub(in crate::db::executor) struct PrimaryScan;

impl PrimaryScan {
    // Keep bounded scan preallocation modest so small page-limited reads avoid
    // the first growth step without reserving huge vectors for large limits.
    const LIMITED_SCAN_PREALLOC_CAP: usize = 32;

    /// Resolve one inclusive primary-key range through structural store authority.
    pub(in crate::db::executor) fn range_structural(
        store: StoreHandle,
        start: &DataKey,
        end: &DataKey,
        direction: Direction,
        limit: Option<usize>,
    ) -> Result<Vec<DataKey>, InternalError> {
        match limit {
            Some(limit) => Self::range_limited_with_store(store, start, end, direction, limit),
            None => Self::range_unbounded_with_store(store, start, end),
        }
    }

    // Decode one raw data key through the canonical corruption mapping.
    fn decode_data_key(raw: &RawDataKey) -> Result<DataKey, InternalError> {
        DataKey::try_from_raw(raw).map_err(|err| {
            InternalError::identity_corruption(format!("failed to decode data key: {err}"))
        })
    }

    // Resolve one bounded range probe with direction-aware early stop.
    fn range_limited_with_store(
        store: StoreHandle,
        start: &DataKey,
        end: &DataKey,
        direction: Direction,
        limit: usize,
    ) -> Result<Vec<DataKey>, InternalError> {
        let keys = store.with_data(|store| -> Result<Vec<DataKey>, InternalError> {
            if limit == 0 {
                return Ok(Vec::new());
            }

            let mut out = Vec::with_capacity(limit.min(Self::LIMITED_SCAN_PREALLOC_CAP));

            let start_raw = start.to_raw()?;
            let end_raw = end.to_raw()?;

            match direction {
                Direction::Asc => {
                    for entry in store.range((Bound::Included(start_raw), Bound::Included(end_raw)))
                    {
                        out.push(Self::decode_data_key(entry.key())?);
                        if out.len() == limit {
                            break;
                        }
                    }
                }
                Direction::Desc => {
                    for entry in store
                        .range((Bound::Included(start_raw), Bound::Included(end_raw)))
                        .rev()
                    {
                        out.push(Self::decode_data_key(entry.key())?);
                        if out.len() == limit {
                            break;
                        }
                    }
                }
            }

            Ok(out)
        })?;

        Ok(keys)
    }

    // Resolve one unbounded range scan in canonical ascending storage order.
    fn range_unbounded_with_store(
        store: StoreHandle,
        start: &DataKey,
        end: &DataKey,
    ) -> Result<Vec<DataKey>, InternalError> {
        let keys = store.with_data(|store| -> Result<Vec<DataKey>, InternalError> {
            let start_raw = start.to_raw()?;
            let end_raw = end.to_raw()?;
            let mut keys = Vec::new();

            for entry in store.range((Bound::Included(start_raw), Bound::Included(end_raw))) {
                keys.push(Self::decode_data_key(entry.key())?);
            }

            Ok(keys)
        })?;

        Ok(keys)
    }
}

///
/// IndexScan
///
/// Executor-owned adapter for secondary-index iteration.
/// The physical stream resolver must request index traversal via this adapter so routing
/// stays decoupled from store-registry/index-handle internals.
///

pub(in crate::db::executor) struct IndexScan;

impl IndexScan {
    /// Resolve one lowered index-prefix envelope through structural store authority.
    pub(in crate::db::executor) fn prefix_structural(
        store: StoreHandle,
        entity_tag: EntityTag,
        spec: &LoweredIndexPrefixSpec,
        direction: Direction,
        limit: usize,
        predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<DataKey>, InternalError> {
        Self::resolve_limited(
            store,
            entity_tag,
            spec.index(),
            spec.lower(),
            spec.upper(),
            IndexScanContinuationInput::new(None, direction),
            limit,
            predicate_execution,
        )
    }

    /// Resolve one lowered index-range envelope through structural store authority.
    pub(in crate::db::executor) fn range_structural(
        store: StoreHandle,
        entity_tag: EntityTag,
        spec: &LoweredIndexRangeSpec,
        continuation: IndexScanContinuationInput<'_>,
        limit: usize,
        predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<DataKey>, InternalError> {
        Self::resolve_limited(
            store,
            entity_tag,
            spec.index(),
            spec.lower(),
            spec.upper(),
            continuation,
            limit,
            predicate_execution,
        )
    }

    // Resolve one index range via store registry and index-store iterator boundary.
    #[expect(clippy::too_many_arguments)]
    fn resolve_limited(
        store: StoreHandle,
        entity_tag: EntityTag,
        index: &IndexModel,
        lower: &Bound<LoweredKey>,
        upper: &Bound<LoweredKey>,
        continuation: IndexScanContinuationInput<'_>,
        limit: usize,
        predicate_execution: Option<IndexPredicateExecution<'_>>,
    ) -> Result<Vec<DataKey>, InternalError> {
        let keys = store.with_index(|index_store| {
            index_store.resolve_data_values_in_raw_range_limited(
                entity_tag,
                index,
                (lower, upper),
                continuation,
                limit,
                predicate_execution,
            )
        })?;

        Ok(keys)
    }
}
